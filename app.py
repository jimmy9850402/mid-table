import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import os
from datetime import datetime, timedelta
import time
import requests
import ssl
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# 忽略 SSL 警告
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# --- 1. 初始化設定 ---
st.set_page_config(page_title="富邦 D&O 補漏採集器 (V10.1)", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 缺漏資料補足系統 (防火牆突破版)")

# 讀取 Supabase 設定
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("⚠️ 請設定 Supabase URL 與 Key")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. 核心功能：抓取市場總表 (🔥修復 ConnectionResetError) ---
@st.cache_data(ttl=3600)
def get_all_tw_companies():
    """從證交所抓取並合併清單 (加入瀏覽器偽裝)"""
    sources = [
        ("上市", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"),
        ("上櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"),
        ("興櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=5")
    ]
    all_dfs = []
    
    # 建立一個 Session 並設定偽裝 Header
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'
    })

    try:
        for market_name, url in sources:
            # 加入 timeout 與 verify=False
            response = session.get(url, verify=False, timeout=15)
            response.encoding = 'cp950'
            
            dfs = pd.read_html(response.text)
            df = dfs[0]
            
            df.columns = df.iloc[0]
            df = df.iloc[1:]
            df = df[df['有價證券代號及名稱'].notna()]
            df_stock = df[df['有價證券代號及名稱'].str.contains('　')]
            df_stock[['代號', '名稱']] = df_stock['有價證券代號及名稱'].str.split('　', expand=True).iloc[:, :2]
            df_stock['市場別'] = market_name
            
            target_cols = ['代號', '名稱', '市場別', '產業別', '上市日']
            for col in target_cols:
                if col not in df_stock.columns:
                    df_stock[col] = "-"
            
            clean_df = df_stock[target_cols]
            clean_df = clean_df[clean_df['代號'].str.match(r'^\d{4}$')]
            all_dfs.append(clean_df)
            
            # 禮貌性暫停，避免被鎖 IP
            time.sleep(1)
            
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"讀取清單失敗 (請稍後再試): {e}")
        return pd.DataFrame()

def get_existing_codes():
    """從 Supabase 分頁取得所有公司代號 (突破 1000 筆限制)"""
    try:
        all_codes = set()
        start = 0
        batch_size = 1000 
        
        while True:
            response = supabase.table("underwriting_cache").select("code").range(start, start + batch_size - 1).execute()
            data = response.data
            
            if not data:
                break
            for item in data:
                all_codes.add(str(item['code']))
            if len(data) < batch_size:
                break
            start += batch_size
            
        return all_codes
    except Exception as e:
        st.error(f"讀取資料庫失敗: {e}")
        return set()

# --- 3. 輔助函數 ---
def date_to_roc_quarter(date_obj):
    year_roc = date_obj.year - 1911
    quarter = (date_obj.month - 1) // 3 + 1
    return f"{year_roc}年 Q{quarter}"

def date_to_roc_year(date_obj):
    year_roc = date_obj.year - 1911
    return f"{year_roc}年"

# --- 🔥 FinMind 救援投手 (V10 歷史趨勢版) ---
def fetch_finmind_data_history(stock_code):
    """
    使用 FinMind API V4 抓取歷史趨勢數據 (近 5 季)
    """
    try:
        start_date = (datetime.now() - timedelta(days=900)).strftime('%Y-%m-%d')
        base_url = "https://api.finmindtrade.com/api/v4/data"
        
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        def get_fm_dataset(dataset_name):
            params = {
                "dataset": dataset_name,
                "data_id": stock_code,
                "start_date": start_date
            }
            try:
                res = requests.get(base_url, params=params, headers=headers, timeout=5)
                json_data = res.json()
                if json_data.get('msg') == 'success':
                    return json_data.get('data', [])
            except: pass
            return []

        data_income = get_fm_dataset("TaiwanStockFinancialStatements")
        data_balance = get_fm_dataset("TaiwanStockBalanceSheet")
        data_cash = get_fm_dataset("TaiwanStockCashFlowsStatement")
        data_rev = get_fm_dataset("TaiwanStockMonthRevenue")

        if not any([data_income, data_balance, data_cash, data_rev]):
            return None

        parsed_data = {
            "營業收入": {}, 
            "每股盈餘(EPS)": {}, 
            "總資產": {}, 
            "總負債": {},
            "流動資產": {}, 
            "流動負債": {},
            "負債比": {}, 
            "營業活動淨現金流": {}
        }

        # --- 1. EPS ---
        if data_income:
            rows = [x for x in data_income if x['type'] in ['EPS', 'BasicEarningsPerShare']]
            rows.sort(key=lambda x: x['date'], reverse=True)
            for row in rows[:6]:
                q_key = date_to_roc_quarter(datetime.strptime(row['date'], '%Y-%m-%d'))
                parsed_data["每股盈餘(EPS)"][q_key] = f"{row['value']:.2f}"

        # --- 2. 資產負債 ---
        if data_balance:
            assets = {} 
            liabs = {}
            cur_assets = {}
            cur_liabs = {}
            
            for row in data_balance:
                d = row['date']
                v = row['value']
                t = row['type']
                if t == 'TotalAssets': assets[d] = v
                elif t in ['TotalLiabilities', 'Liabilities']: liabs[d] = v
                elif t in ['CurrentAssets']: cur_assets[d] = v
                elif t in ['CurrentLiabilities', 'LiabilitiesCurrent']: cur_liabs[d] = v
            
            sorted_dates = sorted(assets.keys(), reverse=True)[:6]
            for d in sorted_dates:
                q_key = date_to_roc_quarter(datetime.strptime(d, '%Y-%m-%d'))
                
                parsed_data["總資產"][q_key] = f"{int(assets[d]/1000):,}"
                
                if d in liabs:
                    l_val = liabs[d]
                    parsed_data["總負債"][q_key] = f"{int(l_val/1000):,}"
                    if assets[d] > 0:
                        ratio = (l_val / assets[d]) * 100
                        parsed_data["負債比"][q_key] = f"{ratio:.2f}%"
                
                if d in cur_assets: parsed_data["流動資產"][q_key] = f"{int(cur_assets[d]/1000):,}"
                if d in cur_liabs: parsed_data["流動負債"][q_key] = f"{int(cur_liabs[d]/1000):,}"

        # --- 3. 現金流 ---
        if data_cash:
            targets = ['CashFlowFromOperatingActivities', 'CashFlowsFromOperatingActivities', 
                       'NetCashFlowsFromUsedInOperatingActivities', 'NetCashInflowFromOperatingActivities']
            rows = [x for x in data_cash if x['type'] in targets]
            rows.sort(key=lambda x: x['date'], reverse=True)
            for row in rows[:6]:
                q_key = date_to_roc_quarter(datetime.strptime(row['date'], '%Y-%m-%d'))
                parsed_data["營業活動淨現金流"][q_key] = f"{int(row['value']/1000):,}"

        # --- 4. 營收 ---
        if data_rev:
            rows = sorted(data_rev, key=lambda x: x['date'], reverse=True)
            for row in rows[:8]:
                m_key = f"{row['date'][:7]} (月)"
                parsed_data["營業收入"][m_key] = f"{int(row['revenue']/1000):,}"

        # --- 5. 格式化輸出 ---
        formatted_list = []
        order = ["營業收入", "總資產", "總負債", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)", "營業活動淨現金流"]
        
        for item_name in order:
            if parsed_data[item_name]:
                row_dict = {"項目": item_name}
                row_dict.update(parsed_data[item_name])
                formatted_list.append(row_dict)
            else:
                formatted_list.append({"項目": item_name})

        formatted_list.append({"項目": "資料來源", "說明": "FinMind (興櫃備援)"})
        return formatted_list

    except Exception as e:
        print(f"FinMind History Error: {e}")
        return None

# --- 4. 核心爬蟲 ---
def fetch_and_upload_data(stock_code, stock_name_tw=None, market_type="上市"):
    suffix = ".TWO" if market_type in ["上櫃", "興櫃"] else ".TW"
    ticker_symbol = f"{stock_code}{suffix}"
    stock = yf.Ticker(ticker_symbol)
    
    formatted_data = []
    source_used = "yfinance"

    try:
        # 1. 嘗試 yfinance
        q_bs = stock.quarterly_balance_sheet
        q_is = stock.quarterly_financials
        
        if q_bs.empty or q_is.empty:
            # yfinance 失敗 -> 啟動 FinMind V10 (歷史版)
            fm_data_list = fetch_finmind_data_history(stock_code)
            
            if fm_data_list:
                source_used = "FinMind"
                formatted_data = fm_data_list
            else:
                return False, f"❌ 無數據跳過: {stock_name_tw}"
        else:
            # 2. yfinance 成功
            q_cf = stock.quarterly_cashflow 
            df_q = pd.concat([q_is.T, q_bs.T, q_cf.T], axis=1)
            df_q = df_q.loc[:, ~df_q.columns.duplicated()]
            df_q.index = pd.to_datetime(df_q.index)
            df_q_sorted = df_q.sort_index(ascending=False).head(12)

            a_bs = stock.balance_sheet
            a_is = stock.financials
            a_cf = stock.cashflow
            df_a_sorted = pd.DataFrame()
            if not a_is.empty:
                df_a = pd.concat([a_is.T, a_bs.T, a_cf.T], axis=1)
                df_a = df_a.loc[:, ~df_a.columns.duplicated()]
                df_a.index = pd.to_datetime(df_a.index)
                df_a_sorted = df_a.sort_index(ascending=False).head(5)

            mapping = {
                "Total Revenue": "營業收入", "Operating Revenue": "營業收入",
                "Total Assets": "總資產",
                "Total Liabilities Net Minority Interest": "總負債", "Total Liabilities": "總負債",
                "Current Assets": "流動資產", "Current Liabilities": "流動負債",
                "Basic EPS": "每股盈餘(EPS)",
                "Operating Cash Flow": "營業活動淨現金流", 
                "Total Cash From Operating Activities": "營業活動淨現金流", 
                "Cash Flow From Continuing Operating Activities": "營業活動淨現金流"
            }
            
            target_items = ["營業收入", "總資產", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)", "營業活動淨現金流"]

            for target_name in target_items:
                row_dict = {"項目": target_name}
                for date_idx in df_q_sorted.index:
                    key_name = date_to_roc_quarter(date_idx)
                    val = extract_value(df_q_sorted, date_idx, target_name, mapping)
                    row_dict[key_name] = val
                
                if not df_a_sorted.empty:
                    for date_idx in df_a_sorted.index:
                        key_name = date_to_roc_year(date_idx)
                        val = extract_value(df_a_sorted, date_idx, target_name, mapping)
                        row_dict[key_name] = val
                
                formatted_data.append(row_dict)

        # 3. 上傳 Supabase
        final_name = stock_name_tw if stock_name_tw else stock.info.get('longName', stock_code)
        payload = {
            "code": stock_code,
            "name": final_name,
            "financial_data": formatted_data,
            "updated_at": datetime.now().isoformat()
        }
        supabase.table("underwriting_cache").upsert(payload).execute()
        
        icon = "✅" if source_used == "yfinance" else "🚑"
        return True, f"{icon} 成功同步: {final_name} ({source_used})"

    except Exception as e:
        return False, str(e)

# 數值提取
def extract_value(df, date_idx, target_name, mapping):
    if target_name == "負債比":
        try:
            liab = df.loc[date_idx].get("Total Liabilities Net Minority Interest") or df.loc[date_idx].get("Total Liabilities")
            assets = df.loc[date_idx].get("Total Assets")
            if liab and assets: return f"{(liab / assets) * 100:.2f}%"
        except: pass
        return "-"
    else:
        found_val = None
        for eng_col, ch_col in mapping.items():
            if ch_col == target_name and eng_col in df.columns:
                val = df.loc[date_idx, eng_col]
                if pd.notna(val): found_val = val; break
        
        if found_val is not None:
            if target_name != "每股盈餘(EPS)":
                try: return f"{int(found_val / 1000):,}"
                except: return "-"
            else: return f"{found_val:.2f}"
    return "-"

# --- 5. UI 介面 ---
tab1, tab2 = st.tabs(["🔍 補漏監控中心", "📝 單筆手動"])

with tab1:
    st.markdown("### 📉 缺漏名單補足系統 (V10.1)")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 1. 掃描缺漏名單", type="primary"):
            with st.spinner("正在比對中 (讀取清單可能需要 10-20 秒，請稍候)..."):
                full_df = get_all_tw_companies()
                db_codes = get_existing_codes() 
                
                if not full_df.empty:
                    full_df['code_str'] = full_df['代號'].astype(str).str.strip()
                    missing_df = full_df[~full_df['code_str'].isin(db_codes)].copy()
                    
                    st.session_state.missing_df = missing_df
                    st.session_state.db_count = len(db_codes)
                    st.success(f"掃描完成！資料庫現有 {len(db_codes)} 筆，發現 {len(missing_df)} 家缺漏。")

    if 'missing_df' in st.session_state:
        m_df = st.session_state.missing_df
        st.metric("目前資料庫總數", f"{st.session_state.db_count} 家")
        st.metric("缺漏家數", f"{len(m_df)} 家", delta=f"-{len(m_df)}", delta_color="inverse")
        
        if not m_df.empty:
            st.dataframe(m_df[['代號', '名稱', '市場別', '產業別']].head(100), hide_index=True)
            
            if st.button(f"🚀 2. 立即補足 {len(m_df)} 家資料"):
                p_bar = st.progress(0)
                status = st.empty()
                success_cnt = 0
                skip_cnt = 0
                
                total = len(m_df)
                for i, row in enumerate(m_df.itertuples()):
                    code = getattr(row, '代號')
                    name = getattr(row, '名稱')
                    mkt = getattr(row, '市場別')
                    
                    status.text(f"處理中 ({i+1}/{total}): {code} {name} ...")
                    
                    ok, msg = fetch_and_upload_data(code, name, mkt)
                    if ok: success_cnt += 1
                    else: skip_cnt += 1
                    
                    p_bar.progress((i+1)/total)
                    time.sleep(0.1) 
                
                st.success(f"🎉 任務結束！成功補入: {success_cnt} 家，無資料跳過: {skip_cnt} 家")
        else:
            st.success("恭喜！目前資料庫完整無缺漏。")

with tab2:
    st.markdown("### 📝 手動單筆查詢")
    s_in = st.text_input("輸入股票代號", value="1269")
    m_type = st.radio("市場", ["上市", "上櫃/興櫃"], horizontal=True)
    if st.button("執行單筆採集"):
        real_mkt = "上市" if "上市" in m_type else "上櫃"
        with st.spinner(f"正在抓取 {s_in}..."):
            suc, msg = fetch_and_upload_data(s_in, market_type=real_mkt)
            if suc: st.success(msg)
            else: st.error(msg)
