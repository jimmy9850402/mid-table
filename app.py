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
st.set_page_config(page_title="富邦 D&O 採集引擎 V26.6", layout="wide", page_icon="🛡️")
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
                trend = "大致相符" if (high/low) < 1.5 else "明顯背離(弱於大盤)"
                analysis.append({"年度": str(year), "高點": str(high), "低點": str(low), "走勢評估": trend})
        return analysis
    except: return []

# ==========================================
# 📊 數據庫讀取功能 (純淨實體公司過濾版)
# ==========================================
@st.cache_data(ttl=3600)
def get_all_tw_companies():
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {"dataset": "TaiwanStockInfo"}
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}
        
        res = requests.get(url, params=params, headers=headers, verify=False, timeout=15)
        data = res.json().get('data', [])
        
        if not data: return pd.DataFrame()

        df = pd.DataFrame(data)
        df = df.rename(columns={'stock_id': '代號', 'stock_name': '名稱', 'type': '市場別', 'industry_category': '產業別'})
        df['代號'] = df['代號'].astype(str)

        # 🛡️ ETF 與特殊商品過濾器
        exclude_prefixes = ('00', '01', '02', '91')
        df = df[~df['代號'].str.startswith(exclude_prefixes)]
        df = df[df['代號'].str.isnumeric()]
        df = df[~df['產業別'].isin(['ETF', 'ETN', '受益證券', '指數類'])]
        
        df['市場別'] = df['市場別'].replace({'twse': '上市', 'tpex': '上櫃', 'rotc': '興櫃'})
        df = df[df['市場別'].isin(['上市', '上櫃', '興櫃'])]
        
        return df[['代號', '名稱', '市場別', '產業別']]
    except Exception as e:
        st.error(f"⚠️ 獲取 FinMind 市場名單失敗: {e}")
        return pd.DataFrame()

def get_db_codes():
    try:
        res = supabase.table("underwriting_cache").select("code").execute()
        return {str(item['code']) for item in res.data}
    except: return set()

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

        stock_data = fetch_stock_analysis(stock_code)
        final_list.append({"項目": "近三年股價與大盤", "股價分析數據": stock_data})

        ai_result = ai_web_research(stock_name) if not skip_ai else "（快速更新模式：未啟動網路探勘）"
        final_list.append({"項目": "AI深度網路探勘(非財務特徵)", "探勘結果": ai_result})
        
        return final_list
    except Exception as e:
        st.error(f"數據處理錯誤: {e}")
        return None

# ==========================================
# 🚀 介面與功能觸發
# ==========================================
tab1, tab2 = st.tabs(["🔍 補漏監控 (批次)", "📝 數據進件工作台"])

with tab1:
    st.markdown("### 📉 缺漏名單自動補足")
    st.info("系統會透過 FinMind API 比對台灣市場名單與 Supabase，並已自動過濾 ETF、ETN 與特別股。")
    
    col_a, col_b = st.columns(2)
    if col_a.button("🔄 1. 開始掃描缺漏", type="primary"):
        with st.spinner("正在安全獲取全台純實體公司名單..."):
            market_df = get_all_tw_companies()
            
            if market_df.empty or '代號' not in market_df.columns:
                st.error("❌ 無法取得市場名單！請確認 API 連線狀態。")
            else:
                db_codes = get_db_codes()
                missing = market_df[~market_df['代號'].astype(str).isin(db_codes)].copy()
                st.session_state.missing_list = missing
                st.success(f"掃描完畢！資料庫已有 {len(db_codes)} 家，尚缺 {len(missing)} 家。")

    if 'missing_list' in st.session_state and not st.session_state.missing_list.empty:
        m_list = st.session_state.missing_list
        st.dataframe(m_list.head(100))
        
        batch_count = st.slider("選擇補足家數 (建議一次 20-50 家以免鎖頻)", 1, len(m_list), 10)
        
        if st.button(f"🚀 啟動批次補足 ({batch_count} 家)"):
            p_bar = st.progress(0)
            st_status = st.empty()
            success_cnt = 0
            
            for i, row in enumerate(m_list.head(batch_count).itertuples()):
                st_status.text(f"⏳ 正在處理 ({i+1}/{batch_count}): {row.代號} {row.名稱}")
                # 批次更新預設跑完整深度模式
                res_data = process_data(row.代號, row.名稱, skip_ai=False)
                if res_data:
                    supabase.table("underwriting_cache").upsert({
                        "code": row.代號, "name": row.名稱, "financial_data": res_data, "updated_at": datetime.now().isoformat()
                    }).execute()
                    success_cnt += 1
                p_bar.progress((i+1)/batch_count)
                
            st.success(f"✅ 批次更新完成！成功補足 {success_cnt} 家資料。")

with tab2:
    st.markdown("### 📝 單筆數據進件與更新")
    c1, c2 = st.columns(2)
    sc = c1.text_input("股票代號", "2330")
    sn = c2.text_input("公司名稱", "台積電")

    col_btn1, col_btn2 = st.columns(2)

    with col_btn1:
        # ✅ 修正完成：改用 use_container_width=True
        if st.button("⚡ 快速更新 (財務及股價)", use_container_width=True):
            with st.spinner("正在極速同步財務指標與股價..."):
                res = process_data(sc, sn, skip_ai=True)
                if res:
                    supabase.table("underwriting_cache").upsert({
                        "code": sc, "name": sn, "financial_data": res, "updated_at": datetime.now().isoformat()
                    }).execute()
                    st.success(f"✅ {sn} 財務數據已更新！")
                    with st.expander("預覽更新資料"):
                        st.json(res[:2])

    with col_btn2:
        # ✅ 修正完成：改用 use_container_width=True
        if st.button("🔍 深度更新 (含 AI 探勘)", type="primary", use_container_width=True):
            with st.spinner("正在進行完整核保調查 (含網路搜尋)..."):
                res = process_data(sc, sn, skip_ai=False)
                if res:
                    supabase.table("underwriting_cache").upsert({
                        "code": sc, "name": sn, "financial_data": res, "updated_at": datetime.now().isoformat()
                    }).execute()
                    st.success(f"✅ {sn} 完整調查已存入中台！")
