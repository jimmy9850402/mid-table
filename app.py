import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import os
from datetime import datetime
import time

# --- 1. 初始化設定 ---
st.set_page_config(page_title="富邦 D&O 全台股採集中心", layout="wide", page_icon="📊")
st.title("📊 D&O 智能核保 - 全台股自動化採集中心")

# 讀取 Supabase 設定
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("⚠️ 請設定 Supabase URL 與 Key")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. 核心功能：抓取 TWSE 上市總表 ---
@st.cache_data(ttl=3600) # 快取 1 小時，避免重複爬網站
def get_twse_listed_companies():
    """從證交所網站抓取所有上市公司清單"""
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    try:
        # 台灣網站通常是 Big5 或 cp950 編碼
        dfs = pd.read_html(url, encoding='cp950')
        df = dfs[0]
        
        # 資料清理：證交所的表頭很亂，通常第 0 列是標題，我們要整理一下
        # 設定欄位名稱
        df.columns = df.iloc[0]
        df = df.iloc[1:] # 移除第一列標題
        
        # 篩選出有「有價證券代號及名稱」的列
        # 格式通常是 "2330　台積電" (中間全形空白)
        df = df[df['有價證券代號及名稱'].notna()]
        
        # 拆分 代號 與 名稱
        # 有些列是分類標題 (如 "股票"), 只有代號名稱列會有全形空白
        df_stock = df[df['有價證券代號及名稱'].str.contains('　')]
        
        # 分割字串
        df_stock[['代號', '名稱']] = df_stock['有價證券代號及名稱'].str.split('　', expand=True).iloc[:, :2]
        
        # 只保留需要的欄位
        clean_df = df_stock[['代號', '名稱', '產業別', '上市日', '市場別']]
        
        # 只要 "股票" 類別 (排除權證等)
        # 上市公司的代號通常是 4 碼數字
        clean_df = clean_df[clean_df['代號'].str.match(r'^\d{4}$')]
        
        return clean_df
    except Exception as e:
        st.error(f"無法讀取證交所清單: {e}")
        return pd.DataFrame()

# --- 3. 輔助函數：日期轉民國季別 ---
def date_to_roc_quarter(date_obj):
    year_roc = date_obj.year - 1911
    quarter = (date_obj.month - 1) // 3 + 1
    return f"{year_roc}年 Q{quarter}"

# --- 4. 核心爬蟲邏輯 (Fetch Logic) ---
def fetch_and_upload_data(stock_code, stock_name_tw=None):
    """
    抓取單一股票數據並上傳
    stock_name_tw: 如果有提供中文名就用，沒有就讓 yfinance 抓
    """
    ticker_symbol = f"{stock_code}.TW"
    stock = yf.Ticker(ticker_symbol)
    
    try:
        # 抓取報表
        bs = stock.quarterly_balance_sheet
        is_ = stock.quarterly_financials
        cf = stock.quarterly_cashflow
        
        # 若是完全空值 (可能下市或代號錯)
        if bs.empty or is_.empty:
            return False, "無財務數據 (可能無權限或代號錯誤)"

        # 合併報表
        df_merged = pd.concat([is_.T, bs.T, cf.T], axis=1)
        df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]
        df_merged.index = pd.to_datetime(df_merged.index)
        
        # 抓近 12 季 (3年) 確保有完整年度資料
        df_sorted = df_merged.sort_index(ascending=False).head(12)
        
        # 欄位對照
        mapping = {
            "Total Revenue": "營業收入",
            "Operating Revenue": "營業收入",
            "Total Assets": "總資產",
            "Total Liabilities Net Minority Interest": "總負債",
            "Total Liabilities": "總負債",
            "Current Assets": "流動資產",
            "Current Liabilities": "流動負債",
            "Basic EPS": "每股盈餘(EPS)",
            "Operating Cash Flow": "營業活動淨現金流"
        }
        
        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", "每股盈餘(EPS)", 
            "營業活動淨現金流"
        ]
        
        formatted_data = []

        for target_name in target_items:
            row_dict = {"項目": target_name}
            for date_idx in df_sorted.index:
                key_name = date_to_roc_quarter(date_idx)
                
                # 負債比計算
                if target_name == "負債比":
                    try:
                        liab = df_sorted.loc[date_idx].get("Total Liabilities Net Minority Interest") or df_sorted.loc[date_idx].get("Total Liabilities")
                        assets = df_sorted.loc[date_idx].get("Total Assets")
                        if liab and assets:
                            val = (liab / assets) * 100
                            row_dict[key_name] = f"{val:.2f}%"
                        else:
                            row_dict[key_name] = "-"
                    except: row_dict[key_name] = "-"
                
                # 一般項目
                else:
                    found_val = None
                    for eng_col, ch_col in mapping.items():
                        if ch_col == target_name:
                            if eng_col in df_sorted.columns:
                                val = df_sorted.loc[date_idx, eng_col]
                                if pd.notna(val):
                                    found_val = val
                                    break
                    
                    if found_val is not None:
                        if target_name != "每股盈餘(EPS)":
                            # 單位換算：元 -> 千元
                            row_dict[key_name] = f"{int(found_val / 1000):,}"
                        else:
                            row_dict[key_name] = f"{found_val:.2f}"
                    else:
                        row_dict[key_name] = "-"
            
            formatted_data.append(row_dict)

        # 上傳 Supabase
        # 如果使用者沒提供中文名，嘗試從 yfinance 抓 (通常是英文)
        final_name = stock_name_tw if stock_name_tw else stock.info.get('longName', stock_code)

        payload = {
            "code": stock_code,
            "name": final_name,
            "financial_data": formatted_data,
            "updated_at": datetime.now().isoformat()
        }
        
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True, f"成功同步: {final_name}"

    except Exception as e:
        return False, str(e)

# --- 5. Streamlit UI 介面 ---
# 側邊欄：資料庫狀態
with st.sidebar:
    st.header("💾 資料庫狀態")
    if st.button("🔄 刷新資料庫列表"):
        try:
            res = supabase.table("underwriting_cache").select("code, name, updated_at", count="exact").execute()
            st.metric("已建檔公司數", res.count)
            if res.data:
                df_db = pd.DataFrame(res.data)
                df_db['updated_at'] = pd.to_datetime(df_db['updated_at']).dt.strftime('%Y-%m-%d %H:%M')
                st.dataframe(df_db, hide_index=True)
        except Exception as e:
            st.error(f"連線失敗: {e}")

# 主畫面
tab1, tab2 = st.tabs(["🚀 上市公司總表 (批量)", "🔍 手動單筆查詢"])

# --- Tab 1: TWSE 總表模式 (新功能) ---
with tab1:
    st.markdown("### 🏢 台灣證券交易所 (TWSE) 上市公司總表")
    st.info("資料來源：https://isin.twse.com.tw/isin/C_public.jsp?strMode=2")
    
    # 載入按鈕
    if 'twse_df' not in st.session_state:
        st.session_state.twse_df = None

    if st.button("📥 載入/刷新 上市公司清單"):
        with st.spinner("正在連線證交所抓取最新清單..."):
            df = get_twse_listed_companies()
            if not df.empty:
                st.session_state.twse_df = df
                st.success(f"成功載入 {len(df)} 家上市公司！")
    
    # 如果已經載入清單，顯示操作介面
    if st.session_state.twse_df is not None:
        df = st.session_state.twse_df
        
        # 1. 篩選器
        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            all_industries = ["全部"] + list(df['產業別'].unique())
            selected_industry = st.selectbox("📂 篩選產業別", all_industries)
        
        with col_filter2:
            search_keyword = st.text_input("🔍 搜尋公司名稱/代號", "")

        # 套用篩選
        filtered_df = df.copy()
        if selected_industry != "全部":
            filtered_df = filtered_df[filtered_df['產業別'] == selected_industry]
        if search_keyword:
            filtered_df = filtered_df[filtered_df['代號'].str.contains(search_keyword) | filtered_df['名稱'].str.contains(search_keyword)]

        # 2. 顯示表格 (可勾選)
        st.write(f"顯示 {len(filtered_df)} 筆資料 (請勾選要更新的公司):")
        
        # 使用 data_editor 讓使用者可以勾選
        filtered_df['選取'] = False # 新增一欄勾選框
        # 將 '選取' 欄移到最前面
        cols = ['選取'] + [c for c in filtered_df.columns if c != '選取']
        edited_df = st.data_editor(
            filtered_df[cols], 
            hide_index=True, 
            column_config={"選取": st.column_config.CheckboxColumn(required=True)},
            disabled=["代號", "名稱", "產業別", "上市日", "市場別"]
        )

        # 3. 批量執行按鈕
        selected_rows = edited_df[edited_df['選取'] == True]
        
        if not selected_rows.empty:
            st.warning(f"⚠️ 即將更新 {len(selected_rows)} 家公司的財務數據。大量更新可能需耗時數分鐘。")
            
            if st.button("🚀 開始批量更新 (Batch Update)", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                log_area = st.expander("詳細執行紀錄", expanded=True)
                
                total = len(selected_rows)
                success_count = 0
                
                for i, row in enumerate(selected_rows.itertuples()):
                    code = row.代號
                    name = row.名稱
                    
                    status_text.text(f"⏳ ({i+1}/{total}) 正在處理: {code} {name} ...")
                    
                    # 執行爬蟲
                    success, msg = fetch_and_upload_data(code, name)
                    
                    if success:
                        success_count += 1
                        log_area.write(f"✅ {code} {name}: 成功")
                    else:
                        log_area.write(f"❌ {code} {name}: {msg}")
                    
                    progress_bar.progress((i + 1) / total)
                    time.sleep(1) # 稍微暫停避免被 Yahoo 封鎖
                
                status_text.success(f"🎉 任務完成！成功更新 {success_count}/{total} 家公司。")
                st.balloons()

# --- Tab 2: 單筆模式 (舊功能保留) ---
with tab2:
    st.markdown("### 📝 手動輸入代號")
    stock_input = st.text_input("輸入股票代號", value="2330", help="例如 2330")
    if st.button("執行單筆採集", type="primary"):
        if stock_input:
            success, msg = fetch_and_upload_data(stock_input)
            if success:
                st.success(msg)
            else:
                st.error(msg)
