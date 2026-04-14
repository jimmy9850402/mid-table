import streamlit as st
import pandas as pd
from supabase import create_client
import os
from datetime import datetime, timedelta
import time
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from duckduckgo_search import DDGS
from google import genai

# 忽略 SSL 警告 (部分金融資料介接需求)
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# --- 1. 系統環境初始化 ---
st.set_page_config(page_title="富邦 D&O 採集引擎 V26.0", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保數據中台 (V26.0 深度探勘版)")

# 讀取環境變數
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or st.secrets.get("GEMINI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("⚠️ 請在環境變數中設定 Supabase 連線資訊 (URL/KEY)")
    st.stop()

# 初始化客戶端
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
# 使用最新 google-genai SDK 
ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# ==========================================
# 🤖 核心功能 1：AI 輿情探勘 (非財務風險分析)
# ==========================================
def ai_web_research(company_name):
    """
    透過搜尋引擎抓取長尾新聞，並由 Gemini 萃取 D&O 專屬風險特徵。
    """
    if not ai_client: return "⚠️ 未設定 GEMINI_API_KEY，跳過 AI 探勘。"
    
    try:
        # 定義 D&O 關鍵搜尋字詞
        query = f"{company_name} (美國員工 OR ADR OR 重大裁罰 OR 掏空訴訟 OR 職場爭議) 新聞 報導"
        search_context = []
        
        with DDGS() as ddgs:
            results = ddgs.text(query, max_results=5)
            for r in results:
                search_context.append(f"標題: {r['title']}\n摘要: {r['body']}")
        
        if not search_context:
            return "✅ 經初步網路搜尋，未發現該公司有明顯之重大負面新聞或美國曝險紀錄。"

        prompt = f"""
        你是一位嚴謹的董監事責任險 (D&O) 核保人員。請根據以下搜尋到的外部新聞資料，
        精準回答關於「{company_name}」的核保要件，若資料未提及請回答「查無資訊」，不得造假。

        1. 美國曝險：是否有美國員工、辦公室或計畫發行 ADR (美國存託憑證)？
        2. 治理風險：近期有無重大裁罰、董監事涉入訴訟或公司經營權爭議？
        3. 勞工風險：有無大規模罷工或職場負面爭議新聞？

        搜尋參考資料：
        {"-"*30}
        {"\n\n".join(search_context)}
        """

        # 使用穩定且配額充足的模型
        response = ai_client.models.generate_content(
            model='gemini-2.0-flash', 
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"⚠️ AI 探勘模組暫時無法使用: {str(e)}"

# ==========================================
# 📊 核心功能 2：最新財報採集 (FinMind 整合)
# ==========================================
def get_latest_financial_data(stock_code, stock_name):
    """
    自動追蹤最新季度財報並轉換為 D&O 標準核保格式。
    """
    try:
        # 回溯 3 年以確保獲得完整的年度趨勢
        lookback_date = (datetime.now() - timedelta(days=1100)).strftime('%Y-%m-%d')
        api_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        def fetch_api(dataset):
            params = {"dataset": dataset, "data_id": stock_code, "start_date": lookback_date}
            time.sleep(1.2) # 基礎節流保護
            return requests.get(api_url, params=params, headers=headers).json().get('data', [])

        # 抓取三張表：綜合損益、資產負債、現金流量
        income_raw = fetch_api("TaiwanStockFinancialStatements")
        balance_raw = fetch_api("TaiwanStockBalanceSheet")
        cash_raw = fetch_api("TaiwanStockCashFlowsStatement")

        if not income_raw: return None

        # 整理季度數據 (ROC 轉換)
        quarterly_bucket = {}
        for row in income_raw:
            dt = datetime.strptime(row['date'], '%Y-%m-%d')
            roc_q = f"{dt.year - 1911}年 Q{(dt.month - 1) // 3 + 1}"
            if roc_q not in quarterly_bucket: quarterly_bucket[roc_q] = {}
            if row['type'] in ['EPS', 'BasicEarningsPerShare']: quarterly_bucket[roc_q]['EPS'] = row['value']
            if row['type'] in ['OperatingRevenue', 'Revenue']: quarterly_bucket[roc_q]['營收'] = row['value']

        for row in balance_raw:
            dt = datetime.strptime(row['date'], '%Y-%m-%d')
            roc_q = f"{dt.year - 1911}年 Q{(dt.month - 1) // 3 + 1}"
            if roc_q in quarterly_bucket:
                if row['type'] == 'TotalAssets': quarterly_bucket[roc_q]['總資產'] = row['value']
                if row['type'] == 'TotalLiabilities': quarterly_bucket[roc_q]['總負債'] = row['value']

        # 構建 D&O 專利模型要求之 JSON 結構 (Formatted List)
        formatted_list = []
        target_items = ["營業收入", "總資產", "總負債", "負債比", "每股盈餘(EPS)"]
        
        for item in target_items:
            data_row = {"項目": item}
            for q, vals in quarterly_bucket.items():
                if item == "營業收入": data_row[q] = f"{int(vals.get('營收', 0)/1000):,}"
                if item == "總資產": data_row[q] = f"{int(vals.get('總資產', 0)/1000):,}"
                if item == "總負債": data_row[q] = f"{int(vals.get('總負債', 0)/1000):,}"
                if item == "負債比" and vals.get('總資產'):
                    data_row[q] = f"{(vals['總負債'] / vals['總資產']) * 100:.2f}%"
                if item == "每股盈餘(EPS)": data_row[q] = f"{vals.get('EPS', 0):.2f}"
            formatted_list.append(data_row)

        # 🌟 核心：附加 AI 深度探勘 (不影響舊有財報數據讀取)
        research_content = ai_web_research(stock_name)
        formatted_list.append({
            "項目": "AI深度網路探勘(非財務特徵)", 
            "探勘結果": research_content
        })

        formatted_list.append({
            "項目": "資料來源", 
            "說明": f"FinMind + DDGS-Gemini (更新時間: {datetime.now().strftime('%Y-%m-%d %H:%M')})"
        })

        return formatted_list

    except Exception as e:
        st.error(f"財報處理錯誤: {e}")
        return None

# ==========================================
# 🚀 執行端介面 (Streamlit UI)
# ==========================================
tab1, tab2 = st.tabs(["⚡ 單筆即時更新", "📊 全庫監控"])

with tab1:
    st.markdown("### 📝 手動更新與深度探勘")
    c1, c2 = st.columns(2)
    with c1: stock_code = st.text_input("股票代號", value="2330")
    with c2: stock_name = st.text_input("公司名稱", value="台積電")

    if st.button("🚀 啟動採集引擎"):
        with st.spinner(f"正在連線中台... 正在同步最新財報與 AI 調查"):
            final_data = get_latest_financial_data(stock_code, stock_name)
            
            if final_data:
                # 寫入 Supabase (Upsert 邏輯)
                payload = {
                    "code": stock_code,
                    "name": stock_name,
                    "financial_data": final_data,
                    "updated_at": datetime.now().isoformat()
                }
                supabase.table("underwriting_cache").upsert(payload).execute()
                
                st.success(f"✅ {stock_name} 資料更新成功，已寫入中台冰庫。")
                
                # 預覽結果
                with st.expander("查看 AI 探勘結果"):
                    st.write(final_data[-2].get("探勘結果", "無資料"))
                st.write("### 財務數據預覽")
                st.dataframe(pd.DataFrame(final_data[:-2]))
            else:
                st.error("❌ 採集失敗，請確認代號是否正確或 API 是否過載。")

with tab2:
    st.info("💡 批次更新模組維護中，如需補齊全庫資料，請聯繫數據行銷部。")
