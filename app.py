import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import os

# --- 1. 基礎連線與頁面設定 ---
st.set_page_config(page_title="富邦產險 | D&O 數據採集中台", layout="wide")

# 從 Streamlit Secrets 讀取連線資訊
# 請確保已在 Streamlit Cloud 的 Advanced Settings > Secrets 設定好以下變數
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]

# 初始化 Supabase 用戶端
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. 核心邏輯模組 ---

def find_stock_code(query):
    """透過 Supabase 資料庫進行名稱與代碼的自動轉換"""
    if query.isdigit(): return f"{query}.TW"
    try:
        # 執行模糊查詢
        res = supabase.table("stock_isin_list").select("code, name").ilike("name", f"%{query}%").execute()
        if res.data:
            # 優先搜尋完全匹配的名稱
            for item in res.data:
                if item['name'] == query: return f"{item['code']}.TW"
            # 若無完全匹配，則回傳搜尋結果的第一項
            return f"{res.data[0]['code']}.TW"
    except Exception as e:
        st.error(f"資料庫檢索異常: {e}")
        return None

def fetch_analysis_report(symbol):
    """
    抓取四期財報，執行跨產業標籤校準，並轉換為「千元單位」
    """
    try:
        ticker = yf.Ticker(symbol)
        q_inc = ticker.quarterly_financials
        q_bal = ticker.quarterly_balance_sheet
        
        if q_inc.empty or q_bal.empty:
            return None

        # 定義 D&O 核保核心指標
        metrics = ["營業收入", "總資產", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)"]
        result_df = pd.DataFrame({"項目": metrics})

        # 抓取最新的四個季度
        for col in q_inc.columns[:4]:
            # 轉換為民國紀年標籤 (例如：114年 Q3)
            p_label = f"{col.year - 1911}年 Q{((col.month-1)//3)+1}"
            
            def get_f(df, keys):
                """多標籤容錯抓取函數"""
                for k in keys:
                    if k in df.index:
                        val = df.loc[k, col]
                        return float(val.iloc[0] if hasattr(val, 'iloc') else val)
                return 0

            # 執行數據採集與千元化校準
            # 營收：適配製造業 (Total Revenue) 與金融業 (Net Interest Income)
            rev = get_f(q_inc, ["Total Revenue", "Operating Revenue", "Net Interest Income"]) / 1000
            # 資產：適配一般資產與金融業合併資產
            assets = get_f(q_bal, ["Total Assets", "Total Combined Assets"]) / 1000
            # 負債：備選多個負債標籤
            liab = get_f(q_bal, ["Total Liabilities Net Minority Interest", "Total Liab", "Total Liabilities"]) / 1000
            # 流動項目：金融業通常為 0 (N/A)
            ca = get_f(q_bal, ["Current Assets", "Total Current Assets"]) / 1000
            cl = get_f(q_bal, ["Current Liabilities", "Total Current Liabilities"]) / 1000
            # EPS：保持原始單位
            eps = get_f(q_inc, ["Basic EPS", "Diluted EPS"])

            # 格式化輸出
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
        st.error(f"數據處理異常: {e}")
        return None

def sync_to_supabase(query, symbol, report_df):
    """將格式化後的財務 JSON 同步至中台 underwriting_cache 表"""
    try:
        data_list = report_df.to_dict(orient="records")
        payload = {
            "code": symbol.split('.')[0],
            "name": query,
            "financial_data": data_list, # 存儲為 JSONB 格式
            "updated_at": "now()"
        }
        # 執行 Upsert (有則更新，無則新增)
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True
    except Exception as e:
        st.error(f"中台同步失敗: {e}")
        return False

# --- 3. 使用者介面 (UI) ---

st.title("🛡️ 富邦產險 - D&O 數據採集中台")
st.markdown("本工具負責從 Yahoo Finance 採集並校準**「千元單位」**財報數據，並同步至 Supabase 中台供 Copilot 調用。")

# 使用 SideBar 顯示系統狀態
with st.sidebar:
    st.header("⚙️ 系統狀態")
    st.success("中台連線：已就緒")
    st.info("單位校準：新台幣千元")

# 主輸入區
user_query = st.text_input("🔍 輸入公司名稱或代碼 (例如: 2330 或 富邦金)", value="台積電")

if st.button("🚀 執行數據採集與同步"):
    with st.spinner(f"正在抓取 {user_query} 的最新財務指標..."):
        target_symbol = find_stock_code(user_query)
        
        if target_symbol:
            report = fetch_analysis_report(target_symbol)
            if report is not None:
                # 執行中台同步
                if sync_to_supabase(user_query, target_symbol, report):
                    st.success(f"✅ {user_query} ({target_symbol}) 數據已成功同步至核保中台！")
                    # 顯示預覽表格
                    st.subheader("📊 校準數據預覽 (單位：千元)")
                    st.table(report)
            else:
                st.error("數據抓取失敗，請確認 Yahoo Finance 是否有該公司的季度報表。")
        else:
            st.error("查無此公司名稱，請確認輸入是否正確。")
