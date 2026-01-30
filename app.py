import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import google.generativeai as genai
import os

# --- 1. 基礎配置與安全性設定 ---
st.set_page_config(page_title="富邦產險 | D&O 數據採集中台", layout="wide")

# 建議將以下金鑰放入 Streamlit Cloud 的 Secrets 中
SUPABASE_URL = "https://cemnzictjgunjyktrruc.supabase.co"
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY", "您的暫時金鑰") 
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "您的暫時金鑰")

# 初始化連線
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)

# --- 2. 核心邏輯模組 ---

def find_stock_code(query):
    """透過 Supabase 進行名稱/代碼轉換 (支援 2881 或 富邦金)"""
    if query.isdigit(): return f"{query}.TW"
    try:
        res = supabase.table("stock_isin_list").select("code, name").ilike("name", f"%{query}%").execute()
        if res.data:
            # 優先找完全符合的名字
            for item in res.data:
                if item['name'] == query: return f"{item['code']}.TW"
            return f"{res.data[0]['code']}.TW"
    except: return None

def fetch_analysis_report(symbol):
    """抓取 4 季財報並執行千元校準 (對齊 989,918,318 營收)"""
    try:
        ticker = yf.Ticker(symbol)
        q_inc = ticker.quarterly_financials
        q_bal = ticker.quarterly_balance_sheet
        if q_inc.empty: return None

        metrics = ["營業收入", "總資產", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)"]
        result_df = pd.DataFrame({"項目": metrics})

        for col in q_inc.columns[:4]:
            p_label = f"{col.year - 1911}年 Q{((col.month-1)//3)+1}"
            
            # 精確標籤檢索與千元換算
            def get_f(df, keys):
                for k in keys:
                    if k in df.index: return float(df.loc[k, col])
                return 0

            rev = get_f(q_inc, ["Total Revenue", "Operating Revenue"]) / 1000
            assets = get_f(q_bal, ["Total Assets"]) / 1000
            liab = get_f(q_bal, ["Total Liabilities Net Minority Interest", "Total Liab"]) / 1000
            ca = get_f(q_bal, ["Current Assets"]) / 1000
            cl = get_f(q_bal, ["Current Liabilities"]) / 1000
            eps = get_f(q_inc, ["Basic EPS", "Diluted EPS"])

            result_df[p_label] = [
                f"{rev:,.0f}", f"{assets:,.0f}", f"{(liab/assets):.2%}", 
                f"{ca:,.0f}", f"{cl:,.0f}", f"{eps:.2f}"
            ]
        return result_df
    except: return None

def sync_to_supabase(query, symbol, report_df):
    """將校準後的數據同步至 underwriting_cache 表"""
    try:
        data_list = report_df.to_dict(orient="records")
        payload = {
            "code": symbol.split('.')[0],
            "name": query,
            "financial_data": data_list,
            "updated_at": "now()"
        }
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True
    except: return False

# --- 3. UI 介面展示 ---

st.title("🛡️ 富邦產險 - D&O 數據採集中台")
st.markdown("本系統負責將 Yahoo Finance 數據精確校準後同步至 Supabase 中台，供 Copilot Agent 調用。")

col1, col2 = st.columns([3, 1])
with col1:
    user_query = st.text_input("輸入公司名稱或代碼 (例如: 2308 或 台達電)", value="台積電")
with col2:
    st.write("##")
    run_btn = st.button("🚀 採集並同步數據")

if run_btn:
    with st.spinner(f"正在抓取 {user_query} 的最新財報..."):
        target_symbol = find_stock_code(user_query)
        if target_symbol:
            report = fetch_analysis_report(target_symbol)
            if report is not None:
                # 執行同步
                if sync_to_supabase(user_query, target_symbol, report):
                    st.toast(f"✅ {user_query} 數據已同步至 Supabase 中台", icon="🚀")
                    st.success(f"數據採集成功：{user_query} ({target_symbol})")
                    st.table(report)
            else:
                st.error("無法抓取數據，請確認 Yahoo Finance 標籤是否存在。")
        else:
            st.error("查無此公司，請檢查名稱是否輸入正確。")
