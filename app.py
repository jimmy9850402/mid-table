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
st.title("📊 D&O 智能核保 - 全台股自動化採集中心 (含年度報表)")

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
    """從證交所抓取並合併清單"""
    sources = [
        ("上市", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"),
        ("上櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"),
        ("興櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=5")
    ]
    all_dfs = []
    
    progress_text = st.empty()
    try:
        for market_name, url in sources:
            progress_text.text(f"正在下載 {market_name} 清單...")
            response = requests.get(url, verify=False)
            response.encoding = 'cp950'
            dfs = pd.read_html(response.text)
            df = dfs[0]
            
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df = df[df['有價證券代號及名稱'].notna()]
            df_stock = df[df['有價證券代號及名稱'].str.contains('　')]
            df_stock[['代號', '名稱']] = df_stock['有價證券代號及名稱'].str.split('　', expand=True).iloc[:, :2]
            df_stock['市場別'] = market_name
            
            target_cols = ['代號', '名稱', '市場別', '上市日']
            for col in target_cols:
                if col not in df_stock.columns: df_stock[col] = "-"
            
            clean_df = df_stock[target_cols]
            clean_df = clean_df[clean_df['代號'].str.match(r'^\d{4}$')]
            all_dfs.append(clean_df)
            
        progress_text.empty()
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"讀取清單失敗: {e}")
        return pd.DataFrame()

# --- 3. 輔助函數 ---
def date_to_roc_quarter(date_obj):
    """將日期轉為民國季別 (例如: 114年 Q1)"""
    year_roc = date_obj.year - 1911
    quarter = (date_obj.month - 1) // 3 + 1
    return f"{year_roc}年 Q{quarter}"

def date_to_roc_year(date_obj):
    """將日期轉為民國年度 (例如: 112年)"""
    year_roc = date_obj.year - 1911
    return f"{year_roc}年"

# --- 4. 核心爬蟲邏輯 (修復版：抓取年度+季度) ---
def fetch_and_upload_data(stock_code, stock_name_tw=None, market_type="上市"):
    """
    抓取季度與年度報表並合併
    """
    suffix = ".TWO" if market_type in ["上櫃", "興櫃"] else ".TW"
    ticker_symbol = f"{stock_code}{suffix}"
    
    stock = yf.Ticker(ticker_symbol)
    
    try:
        # ==========================================
        # 步驟 A: 抓取「季度」報表 (Quarterly)
        # ==========================================
        q_bs = stock.quarterly_balance_sheet
        q_is = stock.quarterly_financials
        q_cf = stock.quarterly_cashflow 
        
        if q_bs.empty or q_is.empty:
            return False, f"無財務數據 ({ticker_symbol})"

        # 合併季度報表
        df_q = pd.concat([q_is.T, q_bs.T, q_cf.T], axis=1)
        df_q = df_q.loc[:, ~df_q.columns.duplicated()]
        df_q.index = pd.to_datetime(df_q.index)
        # 只取近 12 季
        df_q_sorted = df_q.sort_index(ascending=False).head(12)

        # ==========================================
        # 步驟 B: 抓取「年度」報表 (Annual) - 新增功能 🔥
        # ==========================================
        a_bs = stock.balance_sheet
        a_is = stock.financials
        a_cf = stock.cashflow

        df_a_sorted = pd.DataFrame()
        if not a_is.empty:
            df_a = pd.concat([a_is.T, a_bs.T, a_cf.T], axis=1)
            df_a = df_a.loc[:, ~df_a.columns.duplicated()]
            df_a.index = pd.to_datetime(df_a.index)
            # 取近 5 年
            df_a_sorted = df_a.sort_index(ascending=False).head(5)

        # ==========================================
        # 步驟 C: 整合欄位與數據
        # ==========================================
        mapping = {
            "Total Revenue": "營業收入", "Operating Revenue": "營業收入",
            "Total Assets": "總資產",
            "Total Liabilities Net Minority Interest": "總負債", "Total Liabilities": "總負債",
            "Current Assets": "流動資產", "Current Liabilities": "流動負債",
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
            
            # --- 1. 處理季度數據 (Quarterly) ---
            for date_idx in df_q_sorted.index:
                key_name = date_to_roc_quarter(date_idx) # 格式：114年 Q1
                val = extract_value(df_q_sorted, date_idx, target_name, mapping)
                row_dict[key_name] = val

            # --- 2. 處理年度數據 (Annual) ---
            if not df_a_sorted.empty:
                for date_idx in df_a_sorted.index:
                    key_name = date_to_roc_year(date_idx) # 格式：112年
                    val = extract_value(df_a_sorted, date_idx, target_name, mapping)
                    row_dict[key_name] = val
            
            formatted_data.append(row_dict)

        # 上傳 Supabase
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

# 數值提取輔助函式
def extract_value(df, date_idx, target_name, mapping):
    if target_name == "負債比":
        try:
            liab = df.loc[date_idx].get("Total Liabilities Net Minority Interest") or df.loc[date_idx].get("Total Liabilities")
            assets = df.loc[date_idx].get("Total Assets")
            if liab and assets:
                return f"{(liab / assets) * 100:.2f}%"
        except: pass
        return "-"
    else:
        found_val = None
        for eng_col, ch_col in mapping.items():
            if ch_col == target_name and eng_col in df.columns:
                val = df.loc[date_idx, eng_col]
                if pd.notna(val):
                    found_val = val
                    break
        
        if found_val is not None:
            if target_name != "每股盈餘(EPS)":
                try: return f"{int(found_val / 1000):,}"
                except: return "-"
            else:
                return f"{found_val:.2f}"
    return "-"

# --- 5. UI 介面 (保持不變) ---
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

tab1, tab2 = st.tabs(["🚀 全市場批量採集", "🔍 手動查詢"])

with tab1:
    st.markdown("### 🏢 上市 / 上櫃 / 興櫃 總表")
    col_src1, col_src2 = st.columns(2)
    with col_src1:
        if st.button("🌐 下載全市場最新清單"):
            with st.spinner("下載中..."):
                df = get_all_tw_companies()
                if not df.empty:
                    st.session_state.twse_df = df
                    st.success(f"成功載入 {len(df)} 家公司")
    with col_src2:
        if st.button("💾 載入 Supabase 清單"):
            with st.spinner("讀取中..."):
                try:
                    res = supabase.table("underwriting_cache").select("code, name, updated_at").execute()
                    if res.data:
                        df_db = pd.DataFrame(res.data)
                        df_db = df_db.rename(columns={"code": "代號", "name": "名稱"})
                        df_db['產業別'] = "已建檔"
                        df_db['市場別'] = "Supabase"
                        df_db['上市日'] = df_db['updated_at'].apply(lambda x: str(x)[:10])
                        st.session_state.twse_df = df_db
                        st.success(f"成功載入 {len(df_db)} 筆")
                except Exception as e: st.error(f"失敗: {e}")

    if 'twse_df' in st.session_state and st.session_state.twse_df is not None:
        df = st.session_state.twse_df
        st.markdown("---")
        c1, c2, c3 = st.columns(3)
        with c1: mkt = st.selectbox("市場", ["全部"] + list(df['市場別'].unique()))
        with c2: ind = st.selectbox("產業", ["全部"] + list(df['產業別'].unique()))
        with c3: txt = st.text_input("搜尋", "")
        
        f_df = df.copy()
        if mkt != "全部": f_df = f_df[f_df['市場別'] == mkt]
        if ind != "全部": f_df = f_df[f_df['產業別'] == ind]
        if txt: f_df = f_df[f_df['代號'].str.contains(txt) | f_df['名稱'].str.contains(txt)]
        
        st.write(f"共 {len(f_df)} 筆")
        
        # 全選邏輯
        if 'editor_key' not in st.session_state: st.session_state.editor_key = 0
        if 'def_sel' not in st.session_state: st.session_state.def_sel = False
        cb1, cb2, _ = st.columns([1,1,6])
        if cb1.button("✅ 全選"): 
            st.session_state.def_sel = True
            st.session_state.editor_key += 1
            st.rerun()
        if cb2.button("❌ 取消"): 
            st.session_state.def_sel = False
            st.session_state.editor_key += 1
            st.rerun()
            
        f_df['選取'] = st.session_state.def_sel
        valid_cols = [c for c in ['選取', '代號', '名稱', '市場別', '產業別', '上市日'] if c in f_df.columns]
        
        edited_df = st.data_editor(
            f_df[valid_cols], hide_index=True,
            column_config={"選取": st.column_config.CheckboxColumn(required=True)},
            disabled=[c for c in valid_cols if c != '選取'],
            height=400, key=f"editor_{st.session_state.editor_key}"
        )
        
        sel_rows = edited_df[edited_df['選取'] == True]
        if not sel_rows.empty and st.button("🚀 執行更新"):
            p_bar = st.progress(0)
            status = st.empty()
            total = len(sel_rows)
            for i, row in enumerate(sel_rows.itertuples()):
                code = getattr(row, '代號')
                name = getattr(row, '名稱')
                mkt_type = getattr(row, '市場別', '上市')
                status.text(f"處理中 ({i+1}/{total}): {code}")
                fetch_and_upload_data(code, name, mkt_type)
                p_bar.progress((i+1)/total)
                time.sleep(1)
            status.success("完成")

with tab2:
    st.markdown("### 手動查詢")
    s_in = st.text_input("代號", value="2330")
    m_type = st.radio("市場", ["上市", "上櫃/興櫃"], horizontal=True)
    if st.button("執行單筆"):
        real_mkt = "上市" if "上市" in m_type else "上櫃"
        suc, msg = fetch_and_upload_data(s_in, market_type=real_mkt)
        if suc: st.success(msg)
        else: st.error(msg)
