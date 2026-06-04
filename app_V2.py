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
from bs4 import BeautifulSoup

# 忽略 SSL 警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# --- 1. 系統初始化 ---
st.set_page_config(page_title="富邦 D&O 採集引擎 V2.0 (雙軌寫入版)", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 數據採集與情報總管")

# ==========================================
# 🔑 密碼直接寫死區 (暴力破解法，免設環境變數)
# ==========================================
SUPABASE_URL = "https://cemnzictjgunjyktrruc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNlbW56aWN0amd1bmp5a3RycnVjIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2OTA1MTU2MSwiZXhwIjoyMDg0NjI3NTYxfQ.LScr9qrJV7EcjTxp_f47r6-PLMsxz-mJTTblL4ZTmbs" 
GEMINI_API_KEY = "AIzaSyCWng91o6S_wYygIhg-BryYQQaUdUOvFnQ" 

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

# ==========================================
# 🔑 FinMind 專屬通行證 (解決雲端 IP 阻擋)
# ==========================================
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
FINMIND_HEADERS = {"Authorization": f"Bearer {FINMIND_TOKEN}"}

# ==========================================
# 🤖 核心功能：AI 輿情探勘
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
# 📈 核心功能：股價趨勢分析
# ==========================================
def fetch_stock_analysis(stock_code):
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        params = {"dataset": "TaiwanStockPrice", "data_id": stock_code, "start_date": start_date}
        res = requests.get(url, params=params, headers=FINMIND_HEADERS, verify=False, timeout=15).json().get('data', [])
        
        if not res: return []
        
        df = pd.DataFrame(res)
        df['date'] = pd.to_datetime(df['date'])
        df['year'] = df['date'].dt.year
        
        analysis = []
        current_year = datetime.now().year
        for year in [current_year-2, current_year-1, current_year]:
            year_data = df[df['year'] == year]
            if not year_data.empty:
                high = year_data['max'].max()
                low = year_data['min'].min()
                trend = "大致相符" if (high/low) < 1.5 else "明顯背離(弱於大盤)"
                analysis.append({"年度": str(year), "高點": str(high), "低點": str(low), "走勢評估": trend})
        return analysis
    except: return []

# ==========================================
# 🕵️ 核心功能：MOPS 舊版後門重訊爬蟲 (搭載高風險智能濾網)
# ==========================================
def fetch_mops_detailed_news(stock_code):
    current_roc_year = datetime.now().year - 1911
    target_years = [current_roc_year, current_roc_year - 1, current_roc_year - 2]
    detailed_news = []
    
    # 🚨 D&O 核保專屬地雷關鍵字
    danger_keywords = [
        "訴訟", "掏空", "辭任", "變動達三分之一", "退票", "終止買賣", 
        "保留意見", "繼續經營", "虧損", "解任", "調查", "違規", 
        "異常", "罰鍰", "裁罰", "檢調", "搜索", "扣押"
    ]
    
    post_url = 'https://mopsov.twse.com.tw/mops/web/ajax_t05st01'
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Referer': 'https://mopsov.twse.com.tw/mops/web/t05st01',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        session.get('https://mopsov.twse.com.tw/mops/web/t05st01', headers=headers, verify=False, timeout=10)
    except: pass

    for year in target_years:
        data = {
            'encodeURIComponent': '1', 'step': '1', 'firstin': '1', 'off': '1',
            'keyword4': '', 'code1': '', 'TYPEK2': '', 'checkbtn': '',
            'queryName': 'co_id', 'inpuType': 'co_id', 'TYPEK': 'all', 'isnew': 'false',
            'co_id': stock_code, 'year': str(year), 'month': '', 'b_date': '', 'e_date': ''
        }
        try:
            time.sleep(1.0)
            r = session.post(post_url, data=data, headers=headers, verify=False, timeout=15)
            r.encoding = 'utf8'
            soup = BeautifulSoup(r.text, 'html.parser')
            
            infos_table = soup.find('table', 'hasBorder')
            if not infos_table: continue
                
            all_rows = infos_table.find_all('tr', 'even') + infos_table.find_all('tr', 'odd')
            
            for row in all_rows:
                inputs = row.find_all('input')
                if not inputs: continue
                
                codes = [p[p.find('=')+1:].replace("'", "") for p in inputs[0].get('onclick', '').split(';') if '=' in p]
                if len(codes) < 6: continue
                
                detail_payload = {
                    'firstin': 'true', 'step': '2', 'off': '1', 'month': 'all', 'e_month': 'all',
                    'TYPEK': codes[5], 'year': str(year), 'co_id': stock_code,
                    'spoke_date': codes[3], 'spoke_time': codes[2], 'seq_no': codes[1]
                }
                
                retry_count = 0
                while retry_count < 3:
                    time.sleep(0.5) 
                    detail_res = session.post(post_url, data=detail_payload, headers=headers, verify=False, timeout=15)
                    detail_res.encoding = 'utf8'
                    detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
                    if detail_res.status_code == 200 and detail_soup.find('table', 'hasBorder'):
                        break
                    retry_count += 1
                    time.sleep(1.5)
                
                subject, content = "無主旨", "無詳細內容"
                detail_table = detail_soup.find('table', 'hasBorder')
                if detail_table:
                    for tr in detail_table.find_all('tr'):
                        tds = tr.find_all('td')
                        for i, td in enumerate(tds):
                            td_text = td.get_text()
                            if '主旨' in td_text and i + 1 < len(tds): subject = tds[i+1].get_text().strip()
                            if '說明' in td_text and i + 1 < len(tds): content = tds[i+1].get_text().strip().replace('\n', '').replace('\r', '')
                
                # 第一道防線過濾：只有命中風險字眼才存入
                if any(keyword in subject or keyword in content for keyword in danger_keywords):
                    short_content = content[:800] + ("...(內文過長已截斷)" if len(content) > 800 else "")
                    detailed_news.append({"日期": codes[3], "主旨": subject, "內文": short_content})
        except: continue
        
    return detailed_news

# ==========================================
# 📊 數據庫讀取功能 (升級嚴格過濾版)
# ==========================================
@st.cache_data(ttl=3600)
def get_all_tw_companies():
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {"dataset": "TaiwanStockInfo"}
        res = requests.get(url, params=params, headers=FINMIND_HEADERS, verify=False, timeout=15)
        data = res.json().get('data', [])
        if not data: return pd.DataFrame()

        df = pd.DataFrame(data)
        df = df.rename(columns={'stock_id': '代號', 'stock_name': '名稱', 'type': '市場別', 'industry_category': '產業別'})
        df['代號'] = df['代號'].astype(str)

        # 🛡️ 升級過濾器：徹底封殺權證、特別股與非標準實體
        df = df[df['代號'].str.isnumeric()]           # 第一關：必須全是數字（排除英文字母特別股）
        df = df[df['代號'].str.len() == 4]            # 第二關：必須剛好 4 碼（徹底排除 6 碼權證、牛熊證）
        exclude_prefixes = ('00', '01', '02', '91') 
        df = df[~df['代號'].str.startswith(exclude_prefixes)] # 第三關：排除 ETF 與存託憑證開頭
        
        # 第四關：排除特定產業別
        df = df[~df['產業別'].isin(['ETF', 'ETN', '受益證券', '指數類', '存託憑證', '特別股'])]
        df['市場別'] = df['市場別'].replace({'twse': '上市', 'tpex': '上櫃', 'rotc': '興櫃'})
        df = df[df['市場別'].isin(['上市', '上櫃', '興櫃'])]
        
        return df[['代號', '名稱', '市場別', '產業別']]
    except Exception as e:
        st.error(f"⚠️ 獲取市場名單失敗: {e}")
        return pd.DataFrame()

def get_db_codes():
    try:
        res = supabase.table("underwriting_cache").select("code").execute()
        return {str(item['code']) for item in res.data}
    except: return set()

def get_news_db_codes():
    try:
        # 新增：向 Supabase 索取已經有重訊資料的代號名單
        res = supabase.table("mops_news_cache").select("stock_code").execute()
        return {str(item['stock_code']) for item in res.data}
    except: return set()

# ==========================================
# 📊 核心功能：雙軌數據組合 (財報 + 新聞)
# ==========================================
def process_data(stock_code, stock_name, skip_ai=False):
    try:
        lookback = (datetime.now() - timedelta(days=1100)).strftime('%Y-%m-%d')
        base_url = "https://api.finmindtrade.com/api/v4/data"

        def fetch(ds):
            time.sleep(1.1)
            return requests.get(base_url, params={"dataset": ds, "data_id": stock_code, "start_date": lookback}, headers=FINMIND_HEADERS, verify=False).json().get('data', [])

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
                t, v = r.get('type'), r.get('value')
                if v is not None:
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
                if item == "營業收入":
                    v = vals.get('營收')
                    row[lbl] = f"{int(v/1000):,}" if v is not None else "-"
                elif item == "總資產":
                    v = vals.get('總資產')
                    row[lbl] = f"{int(v/1000):,}" if v is not None else "-"
                elif item == "負債比":
                    ta, tl = vals.get('總資產'), vals.get('總負債')
                    row[lbl] = f"{(tl/ta)*100:.2f}%" if ta and tl and ta != 0 else "nan%"
                elif item == "流動資產":
                    v = vals.get('流動資產')
                    row[lbl] = f"{int(v/1000):,}" if v is not None else "-"
                elif item == "流動負債":
                    v = vals.get('流動負債')
                    row[lbl] = f"{int(v/1000):,}" if v is not None else "-"
                elif item == "每股盈餘(EPS)":
                    v = vals.get('EPS')
                    row[lbl] = f"{v:.2f}" if v is not None else "-"
                elif item == "營業活動淨現金流":
                    v = vals.get('營業活動淨現金流')
                    row[lbl] = f"{int(v/1000):,}" if v is not None else "-"
            final_list.append(row)

        stock_data = fetch_stock_analysis(stock_code)
        final_list.append({"項目": "近三年股價與大盤", "股價分析數據": stock_data})

        ai_result = ai_web_research(stock_name) if not skip_ai else "（快速更新模式：未啟動網路探勘）"
        final_list.append({"項目": "AI深度網路探勘(非財務特徵)", "探勘結果": ai_result})
        
        mops_news = fetch_mops_detailed_news(stock_code)
        
        return {"financials": final_list, "news": mops_news}
    except Exception as e:
        st.error(f"數據處理錯誤: {e}")
        return None

# ==========================================
# 🚀 介面與功能觸發 (搭載智慧跳過機制)
# ==========================================
tab1, tab2 = st.tabs(["🔍 補漏監控 (批次)", "📝 數據進件工作台"])

with tab1:
    st.markdown("### 📉 缺漏名單自動補足")
    st.info("系統將嚴格把關，過濾非實體標的，並自動跳過已有資料的公司以節省資源。")
    
    col_a, col_b = st.columns(2)
    if col_a.button("🔄 1. 開始掃描缺漏", type="primary"):
        with st.spinner("正在安全獲取全台純實體公司名單..."):
            market_df = get_all_tw_companies()
            if market_df.empty:
                st.error("❌ 無法取得市場名單！")
            else:
                db_codes = get_db_codes()
                missing = market_df[~market_df['代號'].astype(str).isin(db_codes)].copy()
                st.session_state.missing_list = missing
                st.success(f"掃描完畢！已過濾權證與 ETN。目前資料庫已有 {len(db_codes)} 家，尚缺 {len(missing)} 家。")

    if 'missing_list' in st.session_state and not st.session_state.missing_list.empty:
        m_list = st.session_state.missing_list
        st.dataframe(m_list.head(100))
        batch_count = st.slider("選擇補足家數", 1, len(m_list), 10)
        
        col_batch1, col_batch2 = st.columns(2)
        
        with col_batch1:
            if st.button(f"🚀 批次補足 (財報+重訊) - {batch_count} 家", use_container_width=True):
                p_bar = st.progress(0)
                st_status = st.empty()
                success_cnt = 0
                skip_cnt = 0
                
                # 執行前先抓出現有的財報與重訊名單
                existing_fin_codes = get_db_codes()
                existing_news_codes = get_news_db_codes()
                
                for i, row in enumerate(m_list.head(batch_count).itertuples()):
                    # 智慧判斷：如果財報跟重訊都已經存在，就直接跳過
                    if str(row.代號) in existing_fin_codes and str(row.代號) in existing_news_codes:
                        st_status.text(f"⏭️ 略過 ({i+1}/{batch_count}): {row.代號} {row.名稱} (已有完整雙軌資料)")
                        skip_cnt += 1
                        p_bar.progress((i+1)/batch_count)
                        continue
                        
                    st_status.text(f"⏳ 處理中 ({i+1}/{batch_count}): {row.代號} {row.名稱} (抓取財報與重訊...)")
                    res_data = process_data(row.代號, row.名稱, skip_ai=True)
                    if res_data:
                        supabase.table("underwriting_cache").upsert({
                            "code": row.代號, "name": row.名稱, "financial_data": res_data["financials"], "updated_at": datetime.now().isoformat()
                        }).execute()
                        supabase.table("mops_news_cache").upsert({
                            "stock_code": row.代號, "news_data": res_data["news"], "updated_at": datetime.now().isoformat()
                        }).execute()
                        success_cnt += 1
                    p_bar.progress((i+1)/batch_count)
                st.success(f"✅ 批次作業結束！成功更新 {success_cnt} 家，智慧跳過 {skip_cnt} 家。")
                
        with col_batch2:
            if st.button(f"📰 批次極速補足 (僅更新重訊) - {batch_count} 家", type="primary", use_container_width=True):
                p_bar = st.progress(0)
                st_status = st.empty()
                success_cnt = 0
                skip_cnt = 0
                
                # 執行前先抓出現有的重訊名單
                existing_news_codes = get_news_db_codes()
                
                for i, row in enumerate(m_list.head(batch_count).itertuples()):
                    # 智慧判斷：如果重訊資料庫已經有這家公司，就直接跳過
                    if str(row.代號) in existing_news_codes:
                        st_status.text(f"⏭️ 略過 ({i+1}/{batch_count}): {row.代號} {row.名稱} (已有風險重訊資料)")
                        skip_cnt += 1
                        p_bar.progress((i+1)/batch_count)
                        continue
                        
                    st_status.text(f"⏳ 處理中 ({i+1}/{batch_count}): {row.代號} {row.名稱} (專注爬取最新風險重訊...)")
                    only_news = fetch_mops_detailed_news(row.代號)
                    if only_news is not None:
                        supabase.table("mops_news_cache").upsert({
                            "stock_code": row.代號, 
                            "news_data": only_news, 
                            "updated_at": datetime.now().isoformat()
                        }).execute()
                        success_cnt += 1
                    p_bar.progress((i+1)/batch_count)
                st.success(f"✅ 批次重訊作業結束！成功寫入 {success_cnt} 家最新風險，智慧跳過 {skip_cnt} 家。")

with tab2:
    st.markdown("### 📝 單筆數據進件與更新")
    c1, c2 = st.columns(2)
    sc = c1.text_input("股票代號", "2201")
    sn = c2.text_input("公司名稱", "裕隆")

    col_btn1, col_btn2, col_btn3 = st.columns(3)

    with col_btn1:
        if st.button("⚡ 快速更新 (財報+重訊)", use_container_width=True):
            with st.spinner(f"正在同步 {sn} 的財務指標與公開重訊..."):
                res = process_data(sc, sn, skip_ai=True)
                if res:
                    supabase.table("underwriting_cache").upsert({
                        "code": sc, "name": sn, "financial_data": res["financials"], "updated_at": datetime.now().isoformat()
                    }).execute()
                    supabase.table("mops_news_cache").upsert({
                        "stock_code": sc, "news_data": res["news"], "updated_at": datetime.now().isoformat()
                    }).execute()
                    st.success(f"✅ {sn} 財務數據與 {len(res['news'])} 筆重訊已寫入 Supabase！")
                    with st.expander("預覽更新資料"):
                        st.json(res["financials"][:2])

    with col_btn2:
        if st.button("🔍 深度更新 (含 AI)", type="primary", use_container_width=True):
            with st.spinner("正在進行完整核保調查 (含網路搜尋與所有重訊)..."):
                res = process_data(sc, sn, skip_ai=False)
                if res:
                    supabase.table("underwriting_cache").upsert({
                        "code": sc, "name": sn, "financial_data": res["financials"], "updated_at": datetime.now().isoformat()
                    }).execute()
                    supabase.table("mops_news_cache").upsert({
                        "stock_code": sc, "news_data": res["news"], "updated_at": datetime.now().isoformat()
                    }).execute()
                    st.success(f"✅ {sn} 完整調查與重訊已存入中台雙表！")

    with col_btn3:
        if st.button("📰 僅強制更新重訊", use_container_width=True):
            with st.spinner(f"🚀 繞過財務 API，極速單獨抓取 {sn} 的官方重訊..."):
                only_news = fetch_mops_detailed_news(sc)
                
                if only_news is not None:
                    supabase.table("mops_news_cache").upsert({
                        "stock_code": sc, 
                        "news_data": only_news, 
                        "updated_at": datetime.now().isoformat()
                    }).execute()
                    
                    st.success(f"✅ 獨立更新完成！{sn} 的最新 {len(only_news)} 筆重訊已寫入！")
                    with st.expander("預覽最新重訊 (前3筆)"):
                        st.json(only_news[:3] if only_news else [{"訊息": "近三年無重訊"}])
