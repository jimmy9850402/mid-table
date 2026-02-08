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
st.set_page_config(page_title="富邦 D&O 補漏採集器 (V15.0)", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 缺漏資料補足系統 (全時段聯集版)")

# 讀取 Supabase 設定
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("⚠️ 請設定 Supabase URL 與 Key")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. 核心功能：抓取市場總表 ---
@st.cache_data(ttl=3600)
def get_all_tw_companies():
    sources = [
        ("上市", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"),
        ("上櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"),
        ("興櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=5")
    ]
    all_dfs = []
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7'
    })

    try:
        for market_name, url in sources:
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
            time.sleep(1)
            
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"讀取清單失敗: {e}")
        return pd.DataFrame()

def get_all_db_data():
    try:
        all_data = []
        start = 0
        batch_size = 1000 
        while True:
            response = supabase.table("underwriting_cache").select("code, name, financial_data").range(start, start + batch_size - 1).execute()
            data = response.data
            if not data: break
            all_data.extend(data)
            if len(data) < batch_size: break
            start += batch_size
        return all_data
    except Exception as e:
        st.error(f"讀取資料庫失敗: {e}")
        return []

# --- 3. 輔助函數 ---
def date_to_roc_quarter(date_obj):
    year_roc = date_obj.year - 1911
    quarter = (date_obj.month - 1) // 3 + 1
    return f"{year_roc}年 Q{quarter}"

def date_to_roc_year(date_obj):
    year_roc = date_obj.year - 1911
    return f"{year_roc}年"

# --- 🔥 FinMind 救援投手 (V15 全時段聯集版) ---
def fetch_finmind_data_history(stock_code):
    try:
        start_date = (datetime.now() - timedelta(days=1200)).strftime('%Y-%m-%d') # 拉長到 3 年
        base_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        def get_fm_dataset(dataset_name):
            params = {"dataset": dataset_name, "data_id": stock_code, "start_date": start_date}
            try:
                res = requests.get(base_url, params=params, headers=headers, timeout=5)
                json_data = res.json()
                if json_data.get('msg') == 'success': return json_data.get('data', [])
            except: pass
            return []

        data_income = get_fm_dataset("TaiwanStockFinancialStatements")
        data_balance = get_fm_dataset("TaiwanStockBalanceSheet")
        data_cash = get_fm_dataset("TaiwanStockCashFlowsStatement")
        data_rev = get_fm_dataset("TaiwanStockMonthRevenue")

        if not any([data_income, data_balance, data_cash, data_rev]): return None

        # 1. 建立日期集合 (所有來源的日期取聯集)
        all_dates = set()

        # 暫存字典 (Date -> Value)
        temp_data = {
            "EPS": {}, "Rev": {}, "Assets": {}, "Liabs": {}, 
            "CurAssets": {}, "CurLiabs": {}, "CF": {}
        }

        # --- A. EPS (含白名單) ---
        if data_income:
            eps_keys = ['EPS', 'BasicEarningsPerShare', 'EarningsPerShare', 'NetIncomePerShare']
            for row in data_income:
                if row['type'] in eps_keys:
                    all_dates.add(row['date'])
                    temp_data["EPS"][row['date']] = row['value']

        # --- B. 營收 (白名單優先) ---
        # 1. 月營收
        if data_rev:
            for row in data_rev:
                # 月營收日期格式較特殊，這裡主要用於顯示最新月，不混入季度排序
                pass 
        
        # 2. 季營收 (當月營收缺漏時)
        if data_income:
            rev_keys = [
                'OperatingRevenue', 'TotalOperatingRevenue', 'Revenue', 'NetRevenue', 
                'InterestIncome', 'InterestNetIncome', 'InsuranceRevenue', 'PremiumIncome', 'GrossProfit'
            ]
            # 找出最佳 Key
            best_rev_key = None
            avail_types = {x['type'] for x in data_income}
            for k in rev_keys:
                if k in avail_types: 
                    best_rev_key = k; break
            
            if best_rev_key:
                for row in data_income:
                    if row['type'] == best_rev_key:
                        all_dates.add(row['date'])
                        temp_data["Rev"][row['date']] = row['value']

        # --- C. 資產負債 (含流動資產擴充) ---
        if data_balance:
            for row in data_balance:
                d, v, t = row['date'], row['value'], row['type']
                
                # 總資產
                if t in ['TotalAssets', 'Assets']: 
                    all_dates.add(d)
                    temp_data["Assets"][d] = v
                
                # 總負債
                if t in ['TotalLiabilities', 'Liabilities']:
                    all_dates.add(d)
                    temp_data["Liabs"][d] = v
                
                # 流動資產 (擴充關鍵字)
                if t in ['CurrentAssets', 'TotalCurrentAssets', 'AssetsCurrent']:
                    temp_data["CurAssets"][d] = v
                
                # 流動負債 (擴充關鍵字)
                if t in ['CurrentLiabilities', 'TotalCurrentLiabilities', 'LiabilitiesCurrent']:
                    temp_data["CurLiabs"][d] = v

        # --- D. 現金流 ---
        if data_cash:
            cf_keys = ['CashFlowFromOperatingActivities', 'CashFlowsFromOperatingActivities', 'NetCashInflowFromOperatingActivities']
            for row in data_cash:
                if row['type'] in cf_keys:
                    all_dates.add(row['date'])
                    temp_data["CF"][row['date']] = row['value']

        # --- 2. 整合輸出 (日期由新到舊) ---
        sorted_dates = sorted(list(all_dates), reverse=True)[:6] # 取最近 6 個有資料的時間點
        
        final_struct = {
            "營業收入": {}, "每股盈餘(EPS)": {}, "總資產": {}, "總負債": {},
            "流動資產": {}, "流動負債": {}, "負債比": {}, "營業活動淨現金流": {}
        }

        for d in sorted_dates:
            q_key = date_to_roc_quarter(datetime.strptime(d, '%Y-%m-%d'))
            
            # EPS
            if d in temp_data["EPS"]: final_struct["每股盈餘(EPS)"][q_key] = f"{temp_data['EPS'][d]:.2f}"
            
            # 營收 (季)
            if d in temp_data["Rev"]: final_struct["營業收入"][q_key] = f"{int(temp_data['Rev'][d]/1000):,}"
            
            # 資產
            if d in temp_data["Assets"]: 
                final_struct["總資產"][q_key] = f"{int(temp_data['Assets'][d]/1000):,}"
                # 計算負債比
                if d in temp_data["Liabs"]:
                    final_struct["總負債"][q_key] = f"{int(temp_data['Liabs'][d]/1000):,}"
                    if temp_data["Assets"][d] > 0:
                        final_struct["負債比"][q_key] = f"{(temp_data['Liabs'][d] / temp_data['Assets'][d]) * 100:.2f}%"
            
            # 流動資產/負債
            if d in temp_data["CurAssets"]: final_struct["流動資產"][q_key] = f"{int(temp_data['CurAssets'][d]/1000):,}"
            if d in temp_data["CurLiabs"]: final_struct["流動負債"][q_key] = f"{int(temp_data['CurLiabs'][d]/1000):,}"
            
            # 現金流
            if d in temp_data["CF"]: final_struct["營業活動淨現金流"][q_key] = f"{int(temp_data['CF'][d]/1000):,}"

        # 月營收特別處理 (不放入季度迴圈)
        if data_rev:
            rows = sorted(data_rev, key=lambda x: x['date'], reverse=True)
            for row in rows[:8]:
                m_key = f"{row['date'][:7]} (月)"
                final_struct["營業收入"][m_key] = f"{int(row['revenue']/1000):,}"

        # 轉 List
        formatted_list = []
        order = ["營業收入", "總資產", "總負債", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)", "營業活動淨現金流"]
        for item_name in order:
            if final_struct[item_name]:
                row_dict = {"項目": item_name}
                row_dict.update(final_struct[item_name])
                formatted_list.append(row_dict)
            else:
                formatted_list.append({"項目": item_name})
        
        formatted_list.append({"項目": "資料來源", "說明": "FinMind (興櫃備援)"})
        return formatted_list

    except: return None

# --- 4. 核心爬蟲 ---
def fetch_and_upload_data(stock_code, stock_name_tw=None, market_type="上市", force_finmind=False):
    suffix = ".TWO" if market_type in ["上櫃", "興櫃"] else ".TW"
    ticker_symbol = f"{stock_code}{suffix}"
    stock = yf.Ticker(ticker_symbol)
    
    formatted_data = []
    source_used = "yfinance"

    try:
        if force_finmind:
             fm_data_list = fetch_finmind_data_history(stock_code)
             if fm_data_list:
                 source_used = "FinMind (強制修補)"
                 formatted_data = fm_data_list
             else:
                 return False, f"❌ FinMind 暫無資料: {stock_name_tw}"
        else:
            q_bs = stock.quarterly_balance_sheet
            q_is = stock.quarterly_financials
            
            if q_bs.empty or q_is.empty:
                fm_data_list = fetch_finmind_data_history(stock_code)
                if fm_data_list:
                    source_used = "FinMind"
                    formatted_data = fm_data_list
                else:
                    return False, f"❌ 無數據跳過: {stock_name_tw}"
            else:
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

        final_name = stock_name_tw if stock_name_tw else stock.info.get('longName', stock_code)
        payload = {
            "code": stock_code,
            "name": final_name,
            "financial_data": formatted_data,
            "updated_at": datetime.now().isoformat()
        }
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True, f"✅ 成功: {final_name} ({source_used})"

    except Exception as e:
        return False, str(e)

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
tab1, tab2, tab3, tab4 = st.tabs(["🔍 補漏監控", "🚑 資料修補 (Fix)", "🕵️ 深度診斷", "📝 單筆手動"])

with tab1:
    st.markdown("### 📉 缺漏名單補足")
    if st.button("🔄 1. 掃描缺漏", type="primary"):
        with st.spinner("掃描中..."):
            full_df = get_all_tw_companies()
            db_data = get_all_db_data()
            db_codes = {str(item['code']) for item in db_data}
            
            if not full_df.empty:
                full_df['code_str'] = full_df['代號'].astype(str).str.strip()
                missing_df = full_df[~full_df['code_str'].isin(db_codes)].copy()
                st.session_state.missing_df = missing_df
                st.session_state.db_count = len(db_codes)
                st.success(f"發現 {len(missing_df)} 家缺漏。")

    if 'missing_df' in st.session_state and not st.session_state.missing_df.empty:
        m_df = st.session_state.missing_df
        st.dataframe(m_df.head(50))
        if st.button(f"🚀 補足 {len(m_df)} 家"):
            p = st.progress(0); stt = st.empty()
            cnt = 0
            for i, row in enumerate(m_df.itertuples()):
                stt.text(f"處理: {row.代號} {row.名稱}")
                ok, _ = fetch_and_upload_data(row.代號, row.名稱, row.市場別)
                if ok: cnt += 1
                p.progress((i+1)/len(m_df))
            st.success(f"補足 {cnt} 家")

with tab2:
    st.markdown("### 🚑 興櫃資料修補中心 (V15.0 全時段)")
    if st.button("🔍 1. 掃描需修補名單"):
        with st.spinner("分析資料庫..."):
            all_data = get_all_db_data()
            repair_list = []
            for item in all_data:
                code = str(item['code'])
                name = item['name']
                fdata = item['financial_data']
                is_finmind = False
                has_rev = False
                eps_count = 0
                if isinstance(fdata, list):
                    for row in fdata:
                        if "說明" in row and "FinMind" in row.get("說明", ""): is_finmind = True
                        if row.get("項目") == "營業收入":
                            if len([k for k in row.keys() if k != "項目"]) > 0: has_rev = True
                        if row.get("項目") == "每股盈餘(EPS)":
                            eps_count = len([k for k in row.keys() if k != "項目"])
                if is_finmind and (not has_rev or eps_count <= 1):
                    repair_list.append({"code": code, "name": name})
            
            if repair_list:
                st.session_state.repair_df = pd.DataFrame(repair_list)
                st.warning(f"發現 {len(repair_list)} 家。")
                st.dataframe(st.session_state.repair_df)
            else: st.success("資料庫品質良好。")

    if 'repair_df' in st.session_state:
        r_df = st.session_state.repair_df
        if st.button(f"🛠️ 2. 強制修補 {len(r_df)} 家"):
            p_bar = st.progress(0)
            status = st.empty()
            fixed_cnt = 0
            total = len(r_df)
            for i, row in enumerate(r_df.itertuples()):
                code = getattr(row, 'code')
                name = getattr(row, 'name')
                status.text(f"修補: {code} {name} ...")
                ok, msg = fetch_and_upload_data(code, name, "興櫃", force_finmind=True)
                if ok: fixed_cnt += 1
                p_bar.progress((i+1)/total)
                time.sleep(1.0)
            st.success(f"完成！更新 {fixed_cnt} 家。")

with tab3:
    st.markdown("### 🕵️ 深度診斷 (Debug)")
    st.info("輸入公司代號，直接查詢 FinMind 回傳的原始欄位名稱 (Key)，用於診斷為何抓不到。")
    debug_code = st.text_input("代號", value="1269")
    if st.button("診斷此公司"):
        with st.spinner("診斷中..."):
            token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
            headers = {"Authorization": f"Bearer {token}"}
            base_url = "https://api.finmindtrade.com/api/v4/data"
            start_date = (datetime.now() - timedelta(days=900)).strftime('%Y-%m-%d')
            
            # 診斷資產負債表
            res = requests.get(base_url, params={"dataset": "TaiwanStockBalanceSheet", "data_id": debug_code, "start_date": start_date}, headers=headers)
            if res.json().get('msg') == 'success':
                df = pd.DataFrame(res.json().get('data', []))
                if not df.empty:
                    st.write("#### 資產負債表所有欄位 (Keys):")
                    st.code(list(df['type'].unique()))
                else: st.warning("資產負債表無資料")
            
            # 診斷損益表
            res = requests.get(base_url, params={"dataset": "TaiwanStockFinancialStatements", "data_id": debug_code, "start_date": start_date}, headers=headers)
            if res.json().get('msg') == 'success':
                df = pd.DataFrame(res.json().get('data', []))
                if not df.empty:
                    st.write("#### 損益表所有欄位 (Keys):")
                    st.code(list(df['type'].unique()))

with tab4:
    st.markdown("### 📝 手動單筆查詢")
    s_in = st.text_input("輸入代號", value="1269", key="manual_in")
    m_type = st.radio("市場", ["上市", "上櫃/興櫃"], horizontal=True, key="manual_mkt")
    if st.button("執行", key="manual_btn"):
        with st.spinner(f"抓取 {s_in}..."):
            ok, msg = fetch_and_upload_data(s_in, market_type=("上市" if "上市" in m_type else "上櫃"))
            if ok: st.success(msg)
            else: st.error(msg)
