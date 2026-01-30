import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import time

# --- 1. 系統環境設定 ---
st.set_page_config(page_title="富邦產險 | D&O 數據中台採集器", layout="wide")

# 從 Streamlit Secrets 讀取連線資訊
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.error("❌ 無法讀取 Secrets，請確認 Streamlit Cloud 設定。")

# --- 2. 核心邏輯模組 ---

def find_stock_code(query):
    """將公司名稱或簡稱轉換為 Yahoo Finance 代碼"""
    if str(query).isdigit(): return f"{query}.TW"
    try:
        res = supabase.table("stock_isin_list").select("code, name").ilike("name", f"%{query}%").execute()
        if res.data:
            for item in res.data:
                if item['name'] == query: return f"{item['code']}.TW"
            return f"{res.data[0]['code']}.TW"
    except: return None

def fetch_analysis_report(symbol):
    """抓取財報、對齊日期並執行「千元校準」"""
    try:
        ticker = yf.Ticker(symbol)
        q_inc = ticker.quarterly_financials
        q_bal = ticker.quarterly_balance_sheet
        
        if q_inc is None or q_inc.empty or q_bal is None or q_bal.empty:
            return None

        # 解決 Timestamp 錯誤：找出兩張表共有的結算日期
        common_dates = q_inc.columns.intersection(q_bal.columns)
        if len(common_dates) == 0: return None
        valid_dates = common_dates[:4]

        metrics = ["營業收入", "總資產", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)"]
        result_df = pd.DataFrame({"項目": metrics})

        for col in valid_dates:
            p_label = f"{col.year - 1911}年 Q{((col.month-1)//3)+1}" # 民國紀年標籤
            
            def get_f(df, keys):
                """適配製造業與金融業的多標籤抓取"""
                for k in keys:
                    if k in df.index:
                        val = df.loc[k, col]
                        actual_val = float(val.iloc[0] if hasattr(val, 'iloc') else val)
                        if not pd.isna(actual_val): return actual_val
                return 0

            # 執行數據採集與千元化校準
            rev = get_f(q_inc, ["Total Revenue", "Operating Revenue", "Net Interest Income"]) / 1000
            assets = get_f(q_bal, ["Total Assets", "Total Combined Assets"]) / 1000
            liab = get_f(q_bal, ["Total Liabilities Net Minority Interest", "Total Liab", "Total Liabilities"]) / 1000
            ca = get_f(q_bal, ["Current Assets", "Total Current Assets"]) / 1000
            cl = get_f(q_bal, ["Current Liabilities", "Total Current Liabilities"]) / 1000
            eps = get_f(q_inc, ["Basic EPS", "Diluted EPS"])

            result_df[p_label] = [
                f"{rev:,.0f}", f"{assets:,.0f}", 
                f"{(liab/assets):.2%}" if assets > 0 else "0.00%", 
                f"{ca:,.0f}" if ca > 0 else "N/A", 
                f"{cl:,.0f}" if cl > 0 else "N/A", 
                f"{eps:.2f}"
            ]
        return result_df
    except: return None

def sync_to_supabase(name, code, report_df):
    """將校準後的 JSON 數據 upsert 到中台"""
    try:
        data_list = report_df.to_dict(orient="records")
        payload = {
            "code": str(code).replace(".TW", "").replace(".TWO", ""),
            "name": name,
            "financial_data": data_list,
            "updated_at": "now()"
        }
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True
    except: return False

# --- 3. UI 介面設計 ---
st.title("🛡️ 富邦產險 - D&O 數據採集中台")
st.markdown("---")

# 區塊一：單一公司即時同步
st.header("🔍 個案數據同步")
col1, col2 = st.columns([3, 1])
with col1:
    user_query = st.text_input("輸入公司名稱或代碼 (如: 2330 或 富邦金)", value="台積電")
with col2:
    st.write("##")
    single_btn = st.button("🚀 即時同步")

if single_btn:
    with st.spinner(f"正在採集 {user_query}..."):
        symbol = find_stock_code(user_query)
        if symbol:
            report = fetch_analysis_report(symbol)
            if report is not None:
                if sync_to_supabase(user_query, symbol, report):
                    st.success(f"✅ {user_query} 同步成功！")
                    st.table(report)
            else: st.error("抓取失敗，請確認該公司財報已公開。")
        else: st.error("查無此公司。")

st.markdown("---")

# 區塊二：MA 專用批量同步面板
st.header("📦 批量中台化任務")
tab1, tab2 = st.tabs(["📋 手動清單", "🤖 全庫自動同步"])

with tab1:
    batch_list = st.text_area("請輸入代碼 (每行一個)", value="2330\n2881\n2308\n2454\n2882")
    if st.button("⚡ 開始批量同步"):
        codes = batch_list.splitlines()
        progress = st.progress(0)
        for i, c in enumerate(codes):
            c = c.strip()
            sym = find_stock_code(c)
            if sym:
                rep = fetch_analysis_report(sym)
                if rep is not None: sync_to_supabase(c, sym, rep)
            progress.progress((i + 1) / len(codes))
        st.balloons()
        st.success(f"任務完成！已同步 {len(codes)} 家標的。")

with tab2:
    st.warning("此功能將遍歷 `stock_isin_list` 內所有公司。")
    if st.button("🌊 執行全庫同步 (慎用)"):
        res = supabase.table("stock_isin_list").select("code, name").execute()
        if res.data:
            p_bar = st.progress(0)
            status = st.empty()
            for i, item in enumerate(res.data):
                status.text(f"處理中 ({i+1}/{len(res.data)}): {item['name']}")
                sym = f"{item['code']}.TW"
                rep = fetch_analysis_report(sym)
                if rep is not None: sync_to_supabase(item['name'], sym, rep)
                p_bar.progress((i + 1) / len(res.data))
                time.sleep(0.1) # 避免頻率過快
            st.success("🏁 全台公司數據中台化已完成！")
