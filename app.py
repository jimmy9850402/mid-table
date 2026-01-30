import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import os

# --- 1. 基礎連線與頁面設定 ---
st.set_page_config(page_title="富邦產險 | D&O 數據採集中台", layout="wide")

# 從 Streamlit Secrets 讀取連線資訊
# 請確保已在 Streamlit Cloud 設定好 SUPABASE_URL 與 SUPABASE_KEY
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    st.error("❌ 無法讀取 Secrets 或 Supabase 連線失敗，請檢查設定。")

# --- 2. 核心邏輯模組 ---

def find_stock_code(query):
    """透過 Supabase 進行名稱與代碼轉換"""
    if query.isdigit(): return f"{query}.TW"
    try:
        res = supabase.table("stock_isin_list").select("code, name").ilike("name", f"%{query}%").execute()
        if res.data:
            for item in res.data:
                if item['name'] == query: return f"{item['code']}.TW"
            return f"{res.data[0]['code']}.TW"
    except: return None

def fetch_analysis_report(symbol):
    """
    抓取四期財報，執行『日期對齊』與『千元校準』
    解決 Timestamp('2025-12-31') 導致的日期不匹配錯誤
    """
    try:
        ticker = yf.Ticker(symbol)
        q_inc = ticker.quarterly_financials
        q_bal = ticker.quarterly_balance_sheet
        
        if q_inc is None or q_inc.empty or q_bal is None or q_bal.empty:
            return None

        # --- 關鍵修正：找出兩張表共同擁有的結算日期 ---
        common_dates = q_inc.columns.intersection(q_bal.columns)
        if len(common_dates) == 0:
            st.error(f"⚠️ 無法對齊 {symbol} 的損益表與資產負債表日期。")
            return None
            
        # 取最近的 4 個季度
        valid_dates = common_dates[:4]

        metrics = ["營業收入", "總資產", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)"]
        result_df = pd.DataFrame({"項目": metrics})

        for col in valid_dates:
            # 轉換為民國紀年標籤 (例如：114年 Q3)
            p_label = f"{col.year - 1911}年 Q{((col.month-1)//3)+1}"
            
            def get_f(df, keys):
                """多標籤容錯抓取，確保抓到數值"""
                for k in keys:
                    if k in df.index:
                        val = df.loc[k, col]
                        # 處理 Series 或單一數值的情況
                        actual_val = float(val.iloc[0] if hasattr(val, 'iloc') else val)
                        if not pd.isna(actual_val):
                            return actual_val
                return 0

            # 執行數據採集與千元化校準 (單位：千元)
            rev = get_f(q_inc, ["Total Revenue", "Operating Revenue", "Net Interest Income"]) / 1000
            assets = get_f(q_bal, ["Total Assets", "Total Combined Assets"]) / 1000
            liab = get_f(q_bal, ["Total Liabilities Net Minority Interest", "Total Liab", "Total Liabilities"]) / 1000
            ca = get_f(q_bal, ["Current Assets", "Total Current Assets"]) / 1000
            cl = get_f(q_bal, ["Current Liabilities", "Total Current Liabilities"]) / 1000
            eps = get_f(q_inc, ["Basic EPS", "Diluted EPS"])

            # 格式化輸出文字
            result_df[p_label] = [
                f"{rev:,.0f}", 
                f"{assets:,.0f}", 
                f"{(liab/assets):.2%}" if assets > 0 else "0.00%", 
                f"{ca:,.0f}" if ca > 0 else "N/A", 
                f"{cl:,.0f}" if cl > 0 else "N/A", 
                f"{eps:.2f}"
            ]
        return result_df
    except Exception as e:
        st.error(f"❌ 數據處理異常: {str(e)}")
        return None

def sync_to_supabase(query, symbol, report_df):
    """將校準後的 JSON 數據同步至中台"""
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
    except Exception as e:
        st.error(f"❌ 中台同步失敗: {e}")
        return False

# --- 3. UI 介面 ---
st.title("🛡️ 富邦產險 - D&O 數據採集中台")
st.markdown("本工具負責採集 Yahoo Finance 資料並校準為**千元單位**，同步至中台供 Copilot 調用。")

with st.sidebar:
    st.header("⚙️ 系統狀態")
    st.success("中台連線：已就緒")
    st.info("單位校準：新台幣千元")

user_query = st.text_input("🔍 輸入公司名稱或代碼 (例如: 2330 或 富邦金)", value="台積電")

if st.button("🚀 執行數據採集與同步"):
    with st.spinner(f"正在分析 {user_query} 的財務指標..."):
        target_symbol = find_stock_code(user_query)
        
        if target_symbol:
            report = fetch_analysis_report(target_symbol)
            if report is not None:
                if sync_to_supabase(user_query, target_symbol, report):
                    st.success(f"✅ {user_query} ({target_symbol}) 數據已成功同步至核保中台！")
                    st.subheader("📊 校準數據預覽 (單位：千元)")
                    st.table(report)
            else:
                st.error("數據抓取失敗，請確認該公司是否有公開季度財報。")
        else:
            st.error("查無此公司名稱，請確認輸入是否正確。")
