import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import os
from datetime import datetime
import time
import requests
import ssl
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# 忽略 SSL 警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# --- 1. 初始化設定 ---
st.set_page_config(page_title="富邦 D&O 全台股採集中心", layout="wide", page_icon="📊")
st.title("📊 D&O 智能核保 - 全台股自動化採集中心 (上市/上櫃/興櫃)")

# 讀取 Supabase 設定
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("⚠️ 請設定 Supabase URL 與 Key")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. 核心功能：抓取 上市/上櫃/興櫃 總表 ---
@st.cache_data(ttl=3600)
def get_all_tw_companies():
    """從證交所抓取 上市、上櫃、興櫃 清單並合併"""
    
    # 定義來源 (名稱, URL, 市場代碼)
    sources = [
        ("上市", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"), # 上市
        ("上櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"), # 上櫃
        ("興櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=5")  # 興櫃
    ]
    
    all_dfs = []
    
    progress_text = st.empty()
    
    try:
        for market_name, url in sources:
            progress_text.text(f"正在下載 {market_name} 清單...")
            
            # 使用 requests 略過 SSL
            response = requests.get(url, verify=False)
            response.encoding = 'cp950' 
            
            dfs = pd.read_html(response.text)
            df = dfs[0]
            
            # 資料清理
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df = df[df['有價證券代號及名稱'].notna()]
            
            # 只要股票
            df_stock = df[df['有價證券代號及名稱'].str.contains('　')]
            
            # 拆分 代號 與 名稱
            df_stock[['代號', '名稱']] = df_stock['有價證券代號及名稱'].str.split('　', expand=True).iloc[:, :2]
            
            # 標記市場別 (重要！用來判斷 .TW 或 .TWO)
            df_stock['市場別'] = market_name
            
            # 保留欄位
            # 注意：興櫃的表格欄位可能略有不同，這裡取交集或核心欄位
            target_cols = ['代號', '名稱', '產業別', '市場別', '上市日'] # 上市日可能在興櫃叫別的名字，先嘗試通用
            
            # 確保欄位存在，若無則補空值 (避免興櫃報錯)
            for col in target_cols:
                if col not in df_stock.columns:
                    df_stock[col] = "-"
            
            clean_df = df_stock[target_cols]
            
            # 過濾代號：只留 4 碼數字
            clean_df = clean_df[clean_df['代號'].str.match(r'^\d{4}$')]
            
            all_dfs.append(clean_df)
            
        progress_text.empty()
        
        # 合併所有 DataFrame
        if all_dfs:
            final_df = pd.concat(all_dfs, ignore_index=True)
            return final_df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"讀取清單失敗: {e}")
        return pd.DataFrame()

# --- 3. 輔助函數 ---
def date_to_roc_quarter(date_obj):
    year_roc = date_obj.year - 1911
    quarter = (date_obj.month - 1) // 3 + 1
    return f"{year_roc}年 Q{quarter}"

# --- 4. 核心爬蟲邏輯 (支援 .TWO) ---
def fetch_and_upload_data(stock_code, stock_name_tw=None, market_type="上市"):
    """
    market_type: "上市", "上櫃", "興櫃" (用來決定後綴)
    """
    
    # 🔥 關鍵判斷：上市用 .TW，上櫃/興櫃用 .TWO
    suffix = ".TW"
    if market_type in ["上櫃", "興櫃"]:
        suffix = ".TWO"
    
    ticker_symbol = f"{stock_code}{suffix}"
    
    # 興櫃備用方案：有時候 Yahoo Finance 對興櫃支援不佳，若 .TWO 失敗可嘗試 .TW，但通常是 .TWO
    
    stock = yf.Ticker(ticker_symbol)
    
    try:
        bs = stock.quarterly_balance_sheet
        is_ = stock.quarterly_financials
        cf = stock.quarterly_cashflow 
        
        if bs.empty or is_.empty:
            return False, f"無財務數據 ({ticker_symbol})"

        # 合併報表
        df_merged = pd.concat([is_.T, bs.T, cf.T], axis=1)
        df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]
        df_merged.index = pd.to_datetime(df_merged.index)
        
        # 抓近 12 季
        df_sorted = df_merged.sort_index(ascending=False).head(12)
        
        # Mapping
        mapping = {
            "Total Revenue": "營業收入",
            "Operating Revenue": "營業收入",
            "Total Assets": "總資產",
            "Total Liabilities Net Minority Interest": "總負債",
            "Total Liabilities": "總負債",
            "Current Assets": "流動資產",
            "Current Liabilities": "流動負債",
            "Basic EPS": "每股盈餘(EPS)",
            "Operating Cash Flow": "營業活動淨現金流",
            "Total Cash From Operating Activities": "營業活動淨現金流",
            "Cash Flow From Continuing Operating Activities": "營業活動淨現金流"
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
                
                if target_name == "負債比":
                    try:
                        liab = df_sorted.loc[date_idx].get("Total Liabilities Net Minority Interest") or df_sorted.loc[date_idx].get("Total Liabilities")
                        assets = df_sorted.loc[date_idx].get("Total Assets")
                        if liab and assets:
                            val = (liab / assets) * 100
                            row_dict[key_name] = f"{val:.2f}%"
                        else:
                            row_dict[key_name] = "-"
                    except:
                        row_dict[key_name] = "-"
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
                            try:
                                row_dict[key_name] = f"{int(found_val / 1000):,}"
                            except:
                                row_dict[key_name] = "-"
                        else:
                            row_dict[key_name] = f"{found_val:.2f}"
                    else:
                        row_dict[key_name] = "-"
            
            formatted_data.append(row_dict)

        final_name = stock_name_tw if stock_name_tw else stock.info.get('longName', stock_code)

        payload = {
            "code": stock_code,
            "name": final_name,
            "financial_data": formatted_data,
            "updated_at": datetime.now().isoformat()
        }
        
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True, f"成功同步: {final_name} ({suffix})"

    except Exception as e:
        return False, str(e)

# --- 5. UI 介面 ---
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

tab1, tab2 = st.tabs(["🚀 全市場批量採集 (上市/櫃/興)", "🔍 手動查詢"])

# --- Tab 1: 全市場列表 ---
with tab1:
    st.markdown("### 🏢 上市 / 上櫃 / 興櫃 總表")
    
    col_src1, col_src2 = st.columns(2)
    
    # 來源 A: 證交所/櫃買中心 (整合版)
    with col_src1:
        if st.button("🌐 下載全市場最新清單 (上市+上櫃+興櫃)"):
            with st.spinner("正在連線證交所與櫃買中心..."):
                df = get_all_tw_companies()
                if not df.empty:
                    st.session_state.twse_df = df
                    st.success(f"成功載入 {len(df)} 家公司！(含興櫃)")
                else:
                    st.error("清單下載失敗")

    # 來源 B: Supabase
    with col_src2:
        if st.button("💾 載入 Supabase 現有清單"):
            with st.spinner("讀取中..."):
                try:
                    res = supabase.table("underwriting_cache").select("code, name, updated_at").execute()
                    if res.data:
                        df_db = pd.DataFrame(res.data)
                        df_db = df_db.rename(columns={"code": "代號", "name": "名稱"})
                        df_db['產業別'] = "已建檔"
                        df_db['市場別'] = "Supabase" # 標記來源
                        df_db['上市日'] = df_db['updated_at'].apply(lambda x: str(x)[:10])
                        st.session_state.twse_df = df_db
                        st.success(f"成功載入 {len(df_db)} 筆！")
                except Exception as e:
                    st.error(f"讀取失敗: {e}")

    # 顯示區
    if 'twse_df' in st.session_state and st.session_state.twse_df is not None:
        df = st.session_state.twse_df
        
        st.markdown("---")
        # 篩選器 (增加市場別)
        col_f1, col_f2, col_f3 = st.columns(3)
        
        with col_f1:
            all_markets = ["全部"] + list(df['市場別'].unique())
            sel_market = st.selectbox("📊 篩選市場", all_markets)
            
        with col_f2:
            all_inds = ["全部"] + list(df['產業別'].unique())
            sel_ind = st.selectbox("📂 篩選產業", all_inds)
            
        with col_f3:
            search_txt = st.text_input("🔍 搜尋代號/名稱", "")

        # 套用篩選
        f_df = df.copy()
        if sel_market != "全部":
            f_df = f_df[f_df['市場別'] == sel_market]
        if sel_ind != "全部":
            f_df = f_df[f_df['產業別'] == sel_ind]
        if search_txt:
            f_df = f_df[f_df['代號'].str.contains(search_txt) | f_df['名稱'].str.contains(search_txt)]

        st.write(f"顯示 {len(f_df)} 筆資料:")
        
        # 全選功能
        c_btn1, c_btn2, _ = st.columns([1, 1, 6])
        if 'editor_key' not in st.session_state: st.session_state.editor_key = 0
        if 'def_sel' not in st.session_state: st.session_state.def_sel = False
        
        if c_btn1.button("✅ 全選"):
            st.session_state.def_sel = True
            st.session_state.editor_key += 1
            st.rerun()
        if c_btn2.button("❌ 取消全選"):
            st.session_state.def_sel = False
            st.session_state.editor_key += 1
            st.rerun()

        f_df['選取'] = st.session_state.def_sel
        cols = ['選取', '代號', '名稱', '市場別', '產業別', '上市日']
        
        # 確保只顯示存在的欄位
        valid_cols = [c for c in cols if c in f_df.columns]
        
        edited_df = st.data_editor(
            f_df[valid_cols],
            hide_index=True,
            column_config={"選取": st.column_config.CheckboxColumn(required=True)},
            disabled=[c for c in valid_cols if c != '選取'],
            height=400,
            key=f"editor_{st.session_state.editor_key}"
        )

        # 執行批量
        sel_rows = edited_df[edited_df['選取'] == True]
        if not sel_rows.empty:
            st.warning(f"⚠️ 即將更新 {len(sel_rows)} 家公司 ({sel_rows['市場別'].unique()})")
            if st.button("🚀 開始批量更新"):
                prog_bar = st.progress(0)
                status = st.empty()
                logs = st.expander("執行紀錄", expanded=True)
                
                total = len(sel_rows)
                ok_cnt = 0
                
                for i, row in enumerate(sel_rows.itertuples()):
                    # 安全獲取欄位 (因為 index 會變)
                    # getattr(row, '代號') 是比較安全的做法
                    code = getattr(row, '代號')
                    name = getattr(row, '名稱')
                    mkt = getattr(row, '市場別', '上市') # 預設上市
                    
                    status.text(f"({i+1}/{total}) 處理中: {code} {name} [{mkt}]...")
                    
                    # 傳入 market_type 以決定後綴
                    suc, msg = fetch_and_upload_data(code, name, market_type=mkt)
                    
                    if suc:
                        ok_cnt += 1
                        logs.write(f"✅ {code}: {msg}")
                    else:
                        logs.write(f"❌ {code}: {msg}")
                    
                    prog_bar.progress((i+1)/total)
                    time.sleep(1.0)
                
                status.success(f"完成！成功 {ok_cnt}/{total}")

# --- Tab 2: 單筆模式 ---
with tab2:
    st.markdown("### 📝 手動輸入")
    s_in = st.text_input("輸入代號", value="8069", help="例如 8069 (元太 - 上櫃)")
    # 手動選擇市場，以免使用者輸入興櫃代號卻查不到
    m_type = st.radio("選擇市場別 (影響查詢代碼)", ["上市 (.TW)", "上櫃/興櫃 (.TWO)"], horizontal=True)
    
    if st.button("執行單筆採集", type="primary"):
        if s_in:
            real_mkt = "上市" if "上市" in m_type else "上櫃"
            with st.spinner(f"正在抓取 {s_in} ({real_mkt})..."):
                suc, msg = fetch_and_upload_data(s_in, market_type=real_mkt)
                if suc: st.success(msg)
                else: st.error(msg)
