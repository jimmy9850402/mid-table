import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import os

# --- 1. 基礎連線設定 ---
st.set_page_config(page_title="富邦產險 | D&O 數據採集中台", layout="wide")

# 從 Streamlit Secrets 讀取 Supabase 金鑰
SUPABASE_URL = "https://cemnzictjgunjyktrruc.supabase.co"
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

# 初始化 Supabase 連線
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. 核心數據功能 ---

def find_stock_code(query):
    """透過 Supabase 進行名稱/代碼轉換"""
    if query.isdigit(): return f"{query}.TW"
    try:
        res = supabase.table("stock_isin_list").select("code, name").ilike("name", f"%{query}%").execute()
        if res.data:
            for item in res.data:
                if item['name'] == query: return f"{item['code']}.TW"
            return f"{res.data[0]['code']}.TW"
    except: return None

def fetch_analysis_report(symbol):
    """抓取財報並校準為「千元單位」 (對齊 989,918,318 營收截圖)"""
    try:
        ticker = yf.Ticker(symbol)
        q_inc = ticker.quarterly_financials
        q_bal = ticker.quarterly_balance_sheet
        if q_inc.empty or q_bal.empty: return None

        metrics = ["營業收入", "總資產", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)"]
        result_df = pd.DataFrame({"項目": metrics})

        for col in q_inc.columns[:4]:
            p_label = f"{col.year - 1911}年 Q{((col.month-1)//3)+1}"
            
            # 輔助抓取函數
            def get_f(df, keys):
                for k in keys:
                    if k in df.index: return float(df.loc[k, col])
                return 0

            # 校準邏輯：將元轉換為千元
            rev = get_f(q_inc, ["Total Revenue", "Operating Revenue"]) / 1000
            assets = get_f(q_bal, ["Total Assets"]) / 1000
            liab = get_f(q_bal, ["Total Liabilities Net Minority Interest", "Total Liab"]) / 1000
            ca = get_f(q_bal, ["Current Assets"]) / 1000
            cl = get_f(q_bal, ["Current Liabilities"]) / 1000
            eps = get_f(q_inc, ["Basic EPS", "Diluted EPS"]) # EPS 通常不除以千

            result_df[p_label] = [
                f"{rev:,.0f}", f"{assets:,.0f}", f"{(liab/assets):.2%}", 
                f"{ca:,.0f}", f"{cl:,.0f}", f"{eps:.2f}"
            ]
        return result_df
    except: return None

def sync_to_supabase(query, symbol, report_df):
    """將格式化後的 JSON 數據同步至中台"""
    try:
        # 將 DataFrame 轉為 List[Dict] 格式存入 JSONB 欄位
        data_list = report_df.to_dict(orient="records")
        payload = {
            "code": symbol.split('.')[0],
            "name": query,
            "financial_data": data_list,
            "updated_at": "now()"
        }
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True
    except Exception as e:
        st.error(f"同步失敗: {e}")
        return False

# --- 3. UI 介面 ---
st.title("🛡️ 富邦產險 - D&O 數據採集中台")
st.info("本工具專門負責將 Yahoo Finance 資料校準後同步至中台，AI 分析由 Copilot Studio 執行。")

user_query = st.text_input("輸入公司名稱或代碼 (如: 2308 或 台達電)", value="台積電")

if st.button("🚀 採集並同步至中台"):
    with st.spinner("正在校準數據..."):
        target_symbol = find_stock_code(user_query)
        if target_symbol:
            report = fetch_analysis_report(target_symbol)
            if report is not None:
                if sync_to_supabase(user_query, target_symbol, report):
                    st.success(f"✅ {user_query} ({target_symbol}) 數據已同步至 Supabase！")
                    st.table(report)
            else:
                st.error("無法抓取數據，請確認標的代號。")
        else:
            st.error("查無此公司名稱。")
