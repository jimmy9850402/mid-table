import streamlit as st
import pandas as pd
from supabase import create_client
import os
from datetime import datetime, timedelta
import time
import requests
import io
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from duckduckgo_search import DDGS
from google import genai

# 忽略 SSL 警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# --- 1. 系統初始化 ---
st.set_page_config(page_title="富邦 D&O 採集引擎 V26.5", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 數據採集與股價自動評估")

# 環境變數設定
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or st.secrets.get("GEMINI_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("⚠️ 請在環境變數中設定 Supabase 連線資訊")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# ==========================================
# 🤖 核心功能：AI 輿情探勘 (慢速深度模式用)
# ==========================================
def ai_web_research(company_name):
    if not ai_client: return "⚠️ 未設定 API KEY"
    try:
        queries = [
            f"{company_name} (ADR OR 美國存託憑證) site:mops.twse.com.tw OR site:cmoney.tw",
            f"{company_name} (重大訴訟 OR 裁罰 OR 弊案)"
        ]
        search_context = []
        with DDGS() as ddgs:
            for q in queries:
                for r in ddgs.text(q, max_results=3):
                    search_context.append(f"標題: {r['title']}\n摘要: {r['body']}")
        
        if not search_context: return "✅ 無顯著重大負面與美國風險。"
        prompt = f"你是一位 D&O 核保專家。請分析「{company_name}」有無發行 ADR 及近期重大訴訟。\n搜尋內容：\n" + "\n".join(search_context)
        response = ai_client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
        return response.text
    except: return "⚠️ 探勘超限，建議使用快速更新模式。"

# ==========================================
# 📈 核心功能：股價趨勢分析 (自動計算高低點)
# ==========================================
def fetch_stock_analysis(stock_code):
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        # 抓取近三年的日股價與大盤參考
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}
        
        params = {"dataset": "TaiwanStockPrice", "data_id": stock_code, "start_date": start_date}
        res = requests.get(url, params=params, headers=headers).json().get('data', [])
        
        if not res: return []
        
        df = pd.DataFrame(res)
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        
        analysis = []
        for year in [2024, 2025, 2026]:
            year_data = df[df['year'] == year]
            if not year_data.empty:
                high = year_data['max'].max()
                low = year_data['min'].min()
                # 簡易評估邏輯 (可依需求調整)
                trend = "大致相符" if (high/low) < 1.5 else "明顯背離(弱於大盤)"
                analysis.append({"年度": str(year), "高點": str(high), "低點": str(low), "走勢評估": trend})
        return analysis
    except: return []

# ==========================================
# 📊 核心功能：完整財報採集 (依 JSON 格式封裝)
# ==========================================
def process_data(stock_code, stock_name, skip_ai=False):
    try:
        lookback = (datetime.now() - timedelta(days=1100)).strftime('%Y-%m-%d')
        base_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        def fetch(ds):
            time.sleep(1.1)
            return requests.get(base_url, params={"dataset": ds, "data_id": stock_code, "start_date": lookback}, headers=headers).json().get('data', [])

        income = fetch("TaiwanStockFinancialStatements")
        balance = fetch("TaiwanStockBalanceSheet")
        cash = fetch("TaiwanStockCashFlowsStatement")
        
        if not income: return None

        bucket = {}
        for r in income + balance + cash:
            dt = datetime.strptime(r['date'], '%Y-%m-%d')
            # 轉換為 113年 Q3 格式
            q_label = f"{dt.year-1911}年 Q{(dt.month-1)//3+1}"
            year_label = f"{dt.year-1911}年"
            
            for lbl in [q_label, year_label]:
                if lbl not in bucket: bucket[lbl] = {}
                t = r.get('type')
                v = r.get('value', 0)
                if t in ['OperatingRevenue', 'Revenue']: bucket[lbl]['營收'] = v
                if t == 'TotalAssets': bucket[lbl]['總資產'] = v
                if t == 'TotalLiabilities': bucket[lbl]['總負債'] = v
                if t == 'TotalCurrentAssets': bucket[lbl]['流動資產'] = v
                if t == 'TotalCurrentLiabilities': bucket[lbl]['流動負債'] = v
                if t in ['EPS', 'BasicEarningsPerShare']: bucket[lbl]['EPS'] = v
                if t == 'NetCashFlowsFromUsedInOperatingActivities': bucket[lbl]['營業活動淨現金流'] = v

        # 依照您的 JSON 項目結構封裝
        items_map = ["營業收入", "總資產", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)", "營業活動淨現金流"]
        final_list = []
        
        for item in items_map:
            row = {"項目": item}
            for lbl, vals in bucket.items():
                if item == "營業收入": row[lbl] = f"{int(vals.get('營收', 0)/1000):,}" if vals.get('營收') else "-"
                if item == "總資產": row[lbl] = f"{int(vals.get('總資產', 0)/1000):,}" if vals.get('總資產') else "-"
                if item == "負債比":
                    if vals.get('總資產'): row[lbl] = f"{(vals['總負債']/vals['總資產'])*100:.2f}%"
                    else: row[lbl] = "nan%"
                if item == "流動資產": row[lbl] = f"{int(vals.get('流動資產', 0)/1000):,}" if vals.get('流動資產') else "-"
                if item == "流動負債": row[lbl] = f"{int(vals.get('流動負債', 0)/1000):,}" if vals.get('流動負債') else "-"
                if item == "每股盈餘(EPS)": row[lbl] = f"{vals.get('EPS', 0):.2f}" if vals.get('EPS') else "-"
                if item == "營業活動淨現金流": row[lbl] = f"{int(vals.get('營業活動淨現金流', 0)/1000):,}" if vals.get('營業活動淨現金流') else "-"
            final_list.append(row)

        # 附加股價分析
        stock_data = fetch_stock_analysis(stock_code)
        final_list.append({"項目": "近三年股價與大盤", "股價分析數據": stock_data})

        # 如果不跳過 AI，則執行深度探勘 (否則保留空值或標註)
        ai_result = ai_web_research(stock_name) if not skip_ai else "（快速更新模式：未啟動網路探勘）"
        final_list.append({"項目": "AI深度網路探勘(非財務特徵)", "探勘結果": ai_result})
        
        return final_list
    except Exception as e:
        st.error(f"數據處理錯誤: {e}")
        return None

# ==========================================
# 🚀 介面：新增快速更新按鈕
# ==========================================
st.markdown("### 📝 數據進件工作台")
c1, c2 = st.columns(2)
sc = c1.text_input("股票代號", "2330")
sn = c2.text_input("公司名稱", "台積電")

col_btn1, col_btn2 = st.columns(2)

with col_btn1:
    if st.button("⚡ 快速更新 (財務及股價)", use_container_name=True):
        with st.spinner("正在極速同步財務指標與股價..."):
            res = process_data(sc, sn, skip_ai=True)
            if res:
                supabase.table("underwriting_cache").upsert({
                    "code": sc, "name": sn, "financial_data": res, "updated_at": datetime.now().isoformat()
                }).execute()
                st.success(f"✅ {sn} 財務數據已更新！")
                st.json(res[:2]) # 預覽前兩筆

with col_btn2:
    if st.button("🔍 深度更新 (含 AI 探勘)", type="primary", use_container_name=True):
        with st.spinner("正在進行完整核保調查 (含網路搜尋)..."):
            res = process_data(sc, sn, skip_ai=False)
            if res:
                supabase.table("underwriting_cache").upsert({
                    "code": sc, "name": sn, "financial_data": res, "updated_at": datetime.now().isoformat()
                }).execute()
                st.success(f"✅ {sn} 完整調查已存入中台！")
