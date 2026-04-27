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

# 忽略 SSL 警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# --- 1. 系統初始化 ---
st.set_page_config(page_title="富邦 D&O 採集引擎 V26.4", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 全庫數據自動補漏系統 (V26.4 終極防彈版)")

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
# 🤖 核心功能：AI 輿情探勘模組 (多路徑權威搜尋)
# ==========================================
def ai_web_research(company_name):
    if not ai_client: return "⚠️ 未設定 API KEY"
    try:
        search_context = []
        
        # 🌟 多路徑與權威網域搜尋策略
        queries = [
            f"{company_name} (ADR OR GDR OR 美國存託憑證) site:mops.twse.com.tw OR site:cmoney.tw",
            f"{company_name} (重大訴訟 OR 掏空 OR 內線交易 OR 裁罰)",
            f"{company_name} (美國員工 OR 美國分公司 OR 美國據點)"
        ]
        
        with DDGS() as ddgs:
            for q in queries:
                results = ddgs.text(q, max_results=3)
                for r in results: 
                    search_context.append(f"來源: {q}\n標題: {r['title']}\n摘要: {r['body']}")
        
        if not search_context: return "✅ 無顯著重大負面與美國風險。"

        context_str = "\n\n".join(search_context)[:10000]

        prompt = f"""
        你是一位 D&O 核保專家。請根據以下多重來源的搜尋結果，分析「{company_name}」的：
        1. 有無發行 ADR/GDR 或美國存託憑證？
        2. 美國員工與據點狀況？
        3. 近期重大訴訟、裁罰或治理弊案？
        若查無明確資訊請回答「查無公開資料」，不得編造。
        \n搜尋內容：\n{context_str}
        """
        response = ai_client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
        return response.text
    except Exception as e: 
        return f"⚠️ 探勘異常: {e}"

# ==========================================
# 📊 數據庫讀取功能 (全面改用 FinMind API)
# ==========================================
@st.cache_data(ttl=3600)
def get_all_tw_companies():
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {"dataset": "TaiwanStockInfo"}
        # 使用 FinMind Token
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}
        
        res = requests.get(url, params=params, headers=headers, verify=False, timeout=15)
        data = res.json().get('data', [])
        
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        
        # 轉換欄位名稱
        df = df.rename(columns={
            'stock_id': '代號',
            'stock_name': '名稱',
            'type': '市場別',
            'industry_category': '產業別'
        })
        
        # 中文化市場別
        df['市場別'] = df['市場別'].replace({
            'twse': '上市', 
            'tpex': '上櫃', 
            'rotc': '興櫃'
        })
        
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
# 📈 財報處理核心邏輯 (支持最新季度)
# ==========================================
def process_data(stock_code, stock_name):
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
        
        if not income: return None

        bucket = {}
        for r in income:
            dt = datetime.strptime(r['date'], '%Y-%m-%d')
            q = f"{dt.year-1911}年 Q{(dt.month-1)//3+1}"
            if q not in bucket: bucket[q] = {}
            if r['type'] in ['EPS', 'BasicEarningsPerShare']: bucket[q]['EPS'] = r['value']
            if r['type'] in ['OperatingRevenue', 'Revenue']: bucket[q]['營收'] = r['value']

        for r in balance:
            dt = datetime.strptime(r['date'], '%Y-%m-%d')
            q = f"{dt.year-1911}年 Q{(dt.month-1)//3+1}"
            if q in bucket:
                if r['type'] == 'TotalAssets': bucket[q]['總資產'] = r['value']
                if r['type'] == 'TotalLiabilities': bucket[q]['總負債'] = r['value']

        final = []
        for item in ["營業收入", "總資產", "總負債", "負債比", "每股盈餘(EPS)"]:
            row = {"項目": item}
            for q, v in bucket.items():
                if item == "營業收入": row[q] = f"{int(v.get('營收', 0)/1000):,}"
                if item == "總資產": row[q] = f"{int(v.get('總資產', 0)/1000):,}"
                if item == "總負債": row[q] = f"{int(v.get('總負債', 0)/1000):,}"
                if item == "負債比" and v.get('總資產'): row[q] = f"{(v['總負債']/v['總資產'])*100:.2f}%"
                if item == "每股盈餘(EPS)": row[q] = f"{v.get('EPS', 0):.2f}"
            final.append(row)

        final.append({"項目": "AI深度網路探勘(非財務特徵)", "探勘結果": ai_web_research(stock_name)})
        final.append({"項目": "資料來源", "說明": f"FinMind + 智能探勘 (更新: {datetime.now().strftime('%m/%d %H:%M')})"})
        return final
    except: return None

# ==========================================
# 🚀 介面與功能觸發
# ==========================================
tab1, tab2 = st.tabs(["🔍 補漏監控 (批次)", "📝 手動進件"])

with tab1:
    st.markdown("### 📉 缺漏名單自動補足")
    st.info("系統會透過 FinMind API 比對台灣市場名單與 Supabase，找出尚未進件的公司。")
    
    col_a, col_b = st.columns(2)
    if col_a.button("🔄 1. 開始掃描缺漏", type="primary"):
        with st.spinner("正在安全獲取全台市場名單..."):
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
                res_data = process_data(row.代號, row.名稱)
                if res_data:
                    supabase.table("underwriting_cache").upsert({
                        "code": row.代號, "name": row.名稱, "financial_data": res_data, "updated_at": datetime.now().isoformat()
                    }).execute()
                    success_cnt += 1
                p_bar.progress((i+1)/batch_count)
                
            st.success(f"✅ 批次更新完成！成功補足 {success_cnt} 家資料。")

with tab2:
    st.markdown("### 📝 單筆深度進件")
    sc = st.text_input("代號", "2330")
    sn = st.text_input("名稱", "台積電")
    if st.button("📥 手動更新此筆"):
        with st.spinner("抓取最新財報與啟動多路徑 AI 探勘中..."):
            res = process_data(sc, sn)
            if res:
                supabase.table("underwriting_cache").upsert({
                    "code": sc, "name": sn, "financial_data": res, "updated_at": datetime.now().isoformat()
                }).execute()
                st.success(f"{sn} 更新成功！")
            else: st.error("抓取失敗。")
