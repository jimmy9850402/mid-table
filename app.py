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
st.set_page_config(page_title="富邦 D&O 補漏採集器 (V21.0 股價波動版)", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 缺漏資料補足系統 (V21.0 股價波動版)")

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
def date_to_roc_quarter(date_str):
    try:
        if isinstance(date_str, str):
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            date_obj = date_str
        year_roc = date_obj.year - 1911
        quarter = (date_obj.month - 1) // 3 + 1
        return f"{year_roc}年 Q{quarter}"
    except:
        return "未知季度"

def get_quarter_from_date(date_obj):
    return (date_obj.year, (date_obj.month - 1) // 3 + 1)

# --- 🔥 FinMind 救援投手 (V21 加入股價波動與大盤比較) ---
def fetch_finmind_data_history(stock_code):
    try:
        # 抓取近三年資料 (約 1095 天)
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        base_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        def get_fm_dataset(dataset_name, override_id=None):
            target_id = override_id if override_id else stock_code
            params = {"dataset": dataset_name, "data_id": target_id, "start_date": start_date}
            try:
                res = requests.get(base_url, params=params, headers=headers, timeout=5)
                json_data = res.json()
                if json_data.get('msg') == 'success': return json_data.get('data', [])
            except: pass
            return []

        # 1. 原有財務資料抓取
        data_income = get_fm_dataset("TaiwanStockFinancialStatements")
        data_balance = get_fm_dataset("TaiwanStockBalanceSheet")
        data_cash = get_fm_dataset("TaiwanStockCashFlowsStatement")
        data_rev = get_fm_dataset("TaiwanStockMonthRevenue")

        # 🌟 2. 新增：抓取股價與大盤資料 (TaiwanStockPrice)
        data_stock_price = get_fm_dataset("TaiwanStockPrice")
        data_taiex_price = get_fm_dataset("TaiwanStockPrice", override_id="TAIEX")

        if not any([data_income, data_balance, data_cash, data_rev, data_stock_price]): return None

        quarter_buckets = {}
        monthly_rev_map = {} 
        stock_volatility_results = [] 

        # 🌟 3. 新增：股價與大盤背離計算邏輯
        if data_stock_price:
            df_stock = pd.DataFrame(data_stock_price)
            if not df_stock.empty and 'date' in df_stock.columns:
                df_stock['date'] = pd.to_datetime(df_stock['date'])
                df_stock['Year'] = df_stock['date'].dt.year
                
                stock_yearly = df_stock.groupby('Year').agg(
                    High=('max', 'max'),
                    Low=('min', 'min'),
                    Close_Last=('close', 'last'),
                    Close_First=('close', 'first')
                ).tail(3)

                taiex_yearly_returns = {}
                if data_taiex_price:
                    df_taiex = pd.DataFrame(data_taiex_price)
                    if not df_taiex.empty:
                        df_taiex['date'] = pd.to_datetime(df_taiex['date'])
                        df_taiex['Year'] = df_taiex['date'].dt.year
                        taiex_yearly = df_taiex.groupby('Year').agg(
                            Close_Last=('close', 'last'),
                            Close_First=('close', 'first')
                        ).tail(3)
                        for year, row in taiex_yearly.iterrows():
                            taiex_yearly_returns[year] = (row['Close_Last'] - row['Close_First']) / row['Close_First']

                for year, row in stock_yearly.iterrows():
                    stock_return = (row['Close_Last'] - row['Close_First']) / row['Close_First']
                    taiex_return = taiex_yearly_returns.get(year, 0)
                    
                    is_divergent = "大致相符"
                    if taiex_return > 0.05 and stock_return < -0.10:
                        is_divergent = "明顯背離(弱於大盤)"
                    elif taiex_return < -0.05 and stock_return > 0.10:
                        is_divergent = "明顯背離(強於大盤)"

                    # 🌟 已經加上 float() 修復 JSON 報錯問題！
                    stock_volatility_results.append({
                        "年度": str(year),
                        "高點": float(round(row['High'], 1)),
                        "低點": float(round(row['Low'], 1)),
                        "走勢評估": is_divergent
                    })

        # --- 以下為財務資料整理邏輯 ---
        if data_rev:
            for row in data_rev:
                try:
                    dt = datetime.strptime(row['date'], '%Y-%m-%d')
                    monthly_rev_map[(dt.year, dt.month)] = row['revenue']
                except: pass

        def calculate_quarterly_rev(year, quarter):
            months = []
            if quarter == 1: months = [1, 2, 3]
            elif quarter == 2: months = [4, 5, 6]
            elif quarter == 3: months = [7, 8, 9]
            elif quarter == 4: months = [10, 11, 12]
            
            total = 0
            count = 0
            for m in months:
                if (year, m) in monthly_rev_map:
                    total += monthly_rev_map[(year, m)]
                    count += 1
            if count > 0: return total
            return None

        def add_candidate(date_str, category, key, value):
            q_str = date_to_roc_quarter(date_str)
            if q_str not in quarter_buckets:
                quarter_buckets[q_str] = {
                    "EPS_Candidates": {}, "Rev_Candidates": {}, "CF_Candidates": {},
                    "Assets": None, "Liabs": None, "CurAssets": None, "CurLiabs": None,
                    "DateObj": datetime.strptime(date_str, '%Y-%m-%d') 
                }
            curr_dt = datetime.strptime(date_str, '%Y-%m-%d')
            if curr_dt > quarter_buckets[q_str]["DateObj"]:
                quarter_buckets[q_str]["DateObj"] = curr_dt

            if category == "EPS": quarter_buckets[q_str]["EPS_Candidates"][key] = value
            elif category == "Rev": quarter_buckets[q_str]["Rev_Candidates"][key] = value
            elif category == "CF": quarter_buckets[q_str]["CF_Candidates"][key] = value
            elif category == "Assets": quarter_buckets[q_str]["Assets"] = value
            elif category == "Liabs": quarter_buckets[q_str]["Liabs"] = value
            elif category == "CurAssets": quarter_buckets[q_str]["CurAssets"] = value
            elif category == "CurLiabs": quarter_buckets[q_str]["CurLiabs"] = value

        if data_income:
            eps_keys = ['EPS', 'BasicEarningsPerShare', 'EarningsPerShare', 'NetIncomePerShare']
            rev_keys = ['OperatingRevenue', 'Revenue', 'TotalOperatingRevenue', 'NetRevenue', 'SalesRevenue', 'NetSales', 'InterestIncome', 'InsuranceRevenue', 'GrossProfit']
            for row in data_income:
                if row['type'] in eps_keys:
                    add_candidate(row['date'], "EPS", row['type'], row['value'])
                if row['type'] in rev_keys or ("Revenue" in row['type'] and "Non" not in row['type']):
                    add_candidate(row['date'], "Rev", row['type'], row['value'])
        
        if data_balance:
            for row in data_balance:
                t, d, v = row['type'], row['date'], row['value']
                if t in ['TotalAssets', 'Assets']: add_candidate(d, "Assets", t, v)
                if t in ['TotalLiabilities', 'Liabilities']: add_candidate(d, "Liabs", t, v)
                if t in ['CurrentAssets', 'TotalCurrentAssets', 'AssetsCurrent']: add_candidate(d, "CurAssets", t, v)
                if t in ['CurrentLiabilities', 'TotalCurrentLiabilities', 'LiabilitiesCurrent']: add_candidate(d, "CurLiabs", t, v)

        if data_cash:
            cf_keys = ['NetCashInflowFromOperatingActivities', 'CashFlowsFromOperatingActivities', 'CashFlowFromOperatingActivities']
            for row in data_cash:
                if row['type'] in cf_keys:
                    add_candidate(row['date'], "CF", row['type'], row['value'])

        sorted_quarters = sorted(quarter_buckets.keys(), reverse=True)[:6]
        final_struct = {
            "營業收入": {}, "每股盈餘(EPS)": {}, "總資產": {}, "總負債": {},
            "流動資產": {}, "流動負債": {}, "負債比": {}, "營業活動淨現金流": {}
        }

        REV_PRIORITY = ['OperatingRevenue', 'Revenue', 'TotalOperatingRevenue', 'NetRevenue', 'SalesRevenue', 'NetSales', 'InterestIncome', 'GrossProfit']
        EPS_PRIORITY = ['EPS', 'BasicEarningsPerShare', 'EarningsPerShare']
        CF_PRIORITY = ['NetCashInflowFromOperatingActivities', 'CashFlowsFromOperatingActivities', 'CashFlowFromOperatingActivities']

        for q in sorted_quarters:
            bucket = quarter_buckets[q]
            for p_key in EPS_PRIORITY:
                if p_key in bucket["EPS_Candidates"]:
                    final_struct["每股盈餘(EPS)"][q] = f"{bucket['EPS_Candidates'][p_key]:.2f}"
                    break
            
            found_rev = False
            for p_key in REV_PRIORITY:
                if p_key in bucket["Rev_Candidates"]:
                    final_struct["營業收入"][q] = f"{int(bucket['Rev_Candidates'][p_key]/1000):,}"
                    found_rev = True
                    break
            
            if not found_rev:
                d_obj = bucket["DateObj"]
                y, q_num = get_quarter_from_date(d_obj)
                calc_rev = calculate_quarterly_rev(y, q_num)
                if calc_rev is not None:
                    final_struct["營業收入"][q] = f"{int(calc_rev/1000):,} (月加總)"
                    found_rev = True

            for p_key in CF_PRIORITY:
                if p_key in bucket["CF_Candidates"]:
                    final_struct["營業活動淨現金流"][q] = f"{int(bucket['CF_Candidates'][p_key]/1000):,}"
                    break
            
            if bucket["Assets"]: 
                final_struct["總資產"][q] = f"{int(bucket['Assets']/1000):,}"
                if bucket["Liabs"]:
                    final_struct["總負債"][q] = f"{int(bucket['Liabs']/1000):,}"
                    if bucket["Assets"] > 0:
                        final_struct["負債比"][q] = f"{(bucket['Liabs'] / bucket['Assets']) * 100:.2f}%"
            if bucket["CurAssets"]: final_struct["流動資產"][q] = f"{int(bucket['CurAssets']/1000):,}"
            if bucket["CurLiabs"]: final_struct["流動負債"][q] = f"{int(bucket['CurLiabs']/1000):,}"

        if data_rev:
            rows = sorted(data_rev, key=lambda x: x['date'], reverse=True)
            for row in rows[:8]:
                m_key = f"{row['date'][:7]} (月)"
                final_struct["營業收入"][m_key] = f"{int(row['revenue']/1000):,}"

        formatted_list = []
        order = ["營業收入", "總資產", "總負債", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)", "營業活動淨現金流"]
        for item_name in order:
            if final_struct[item_name]:
                row_dict = {"項目": item_name}
                row_dict.update(final_struct[item_name])
                formatted_list.append(row_dict)
            else:
                formatted_list.append({"項目": item_name})
        
        # 🌟 4. 將股價波動資料附掛在 JSON 結構最後
        if stock_volatility_results:
            formatted_list.append({
                "項目": "近三年股價與大盤",
                "股價分析數據": stock_volatility_results
            })

        formatted_list.append({"項目": "資料來源", "說明": "FinMind (綜合財報與股價)"})
        return formatted_list

    except Exception as e:
        print(f"Error: {e}")
        return None

# --- 4. 核心爬蟲 ---
def fetch_and_upload_data(stock_code, stock_name_tw=None, market_type="上市", force_finmind=False):
    suffix = ".TWO" if market_type in ["上櫃", "興櫃"] else ".TW"
    ticker_symbol = f"{stock_code}{suffix}"
    stock = yf.Ticker(ticker_symbol)
    
    formatted_data = []
    source_used = "yfinance"

    try:
        # 強制使用 FinMind 以確保同時拿到財報與股價資料
        fm_data_list = fetch_finmind_data_history(stock_code)

        if fm_data_list:
             source_used = "FinMind (全面接管)"
             formatted_data = fm_data_list
        else:
             return False, f"❌ 查無資料 (可能無財報或股價): {stock_name_tw}"

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
    st.markdown("### 🚑 興櫃資料修補中心 (V21.0 股價波動)")
    if st.button("🔍 1. 掃描需修補名單"):
        with st.spinner("分析資料庫品質中..."):
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
    debug_code = st.text_input("代號", value="4546")
    if st.button("診斷此公司"):
        with st.spinner("診斷中..."):
            token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
            headers = {"Authorization": f"Bearer {token}"}
            base_url = "https://api.finmindtrade.com/api/v4/data"
            start_date = (datetime.now() - timedelta(days=900)).strftime('%Y-%m-%d')
            
            res = requests.get(base_url, params={"dataset": "TaiwanStockBalanceSheet", "data_id": debug_code, "start_date": start_date}, headers=headers)
            if res.json().get('msg') == 'success':
                df = pd.DataFrame(res.json().get('data', []))
                if not df.empty:
                    st.write("#### 資產負債表所有欄位 (Keys):")
                    st.code(list(df['type'].unique()))
                else: st.warning("資產負債表無資料")

with tab4:
    st.markdown("### 📝 手動單筆查詢")
    s_in = st.text_input("輸入代號", value="1269", key="manual_in")
    m_type = st.radio("市場", ["上市", "上櫃/興櫃"], horizontal=True, key="manual_mkt")
    if st.button("執行", key="manual_btn"):
        with st.spinner(f"抓取 {s_in}..."):
            ok, msg = fetch_and_upload_data(s_in, market_type=("上市" if "上市" in m_type else "上櫃"))
            if ok: st.success(msg)
            else: st.error(msg)
