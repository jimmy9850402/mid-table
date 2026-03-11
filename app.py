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
st.set_page_config(page_title="富邦 D&O 補漏採集器 (V25.0 異常清洗版)", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 缺漏資料補足系統 (V25.0 異常清洗版)")

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
                if col not in df_stock.columns: df_stock[col] = "-"
            
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

def date_to_roc_quarter(date_str):
    try:
        if isinstance(date_str, str): date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        else: date_obj = date_str
        year_roc = date_obj.year - 1911
        quarter = (date_obj.month - 1) // 3 + 1
        return f"{year_roc}年 Q{quarter}"
    except: return "未知季度"

def get_quarter_from_date(date_obj):
    return (date_obj.year, (date_obj.month - 1) // 3 + 1)

# ==========================================
# 🔥 核心引擎 1：完整抓取 (財報 + 股價)
# ==========================================
def fetch_finmind_data_history(stock_code):
    try:
        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        base_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        def get_fm_dataset(dataset_name, override_id=None):
            target_id = override_id if override_id else stock_code
            params = {"dataset": dataset_name, "data_id": target_id, "start_date": start_date}
            try:
                time.sleep(1.5) # 🛡️ 防護機制
                res = requests.get(base_url, params=params, headers=headers, timeout=5)
                json_data = res.json()
                if json_data.get('msg') == 'success': return json_data.get('data', [])
            except: pass
            return []

        data_income = get_fm_dataset("TaiwanStockFinancialStatements")
        data_balance = get_fm_dataset("TaiwanStockBalanceSheet")
        data_cash = get_fm_dataset("TaiwanStockCashFlowsStatement")
        data_rev = get_fm_dataset("TaiwanStockMonthRevenue")
        data_stock_price = get_fm_dataset("TaiwanStockPrice")
        data_taiex_price = get_fm_dataset("TaiwanStockPrice", override_id="TAIEX")

        if not any([data_income, data_balance, data_cash, data_rev, data_stock_price]): return None

        quarter_buckets = {}
        monthly_rev_map = {} 
        stock_volatility_results = [] 

        # --- 股價波動計算 (已排除0元) ---
        if data_stock_price:
            df_stock = pd.DataFrame(data_stock_price)
            if not df_stock.empty and 'date' in df_stock.columns:
                df_stock['date'] = pd.to_datetime(df_stock['date'])
                df_stock['Year'] = df_stock['date'].dt.year
                
                df_clean_stock = df_stock[df_stock['close'] > 0].copy()
                
                if not df_clean_stock.empty:
                    stock_yearly = df_clean_stock.groupby('Year').agg(
                        High=('max', 'max'), Low=('min', 'min'),
                        Close_Last=('close', 'last'), Close_First=('close', 'first')
                    ).tail(3)

                    taiex_yearly_returns = {}
                    if data_taiex_price:
                        df_taiex = pd.DataFrame(data_taiex_price)
                        if not df_taiex.empty:
                            df_taiex['date'] = pd.to_datetime(df_taiex['date'])
                            df_taiex['Year'] = df_taiex['date'].dt.year
                            df_clean_taiex = df_taiex[df_taiex['close'] > 0].copy()
                            taiex_yearly = df_clean_taiex.groupby('Year').agg(
                                Close_Last=('close', 'last'), Close_First=('close', 'first')
                            ).tail(3)
                            for year, row in taiex_yearly.iterrows():
                                taiex_yearly_returns[year] = (row['Close_Last'] - row['Close_First']) / row['Close_First']

                    for year, row in stock_yearly.iterrows():
                        stock_return = (row['Close_Last'] - row['Close_First']) / row['Close_First']
                        taiex_return = taiex_yearly_returns.get(year, 0)
                        is_divergent = "大致相符"
                        if taiex_return > 0.05 and stock_return < -0.10: is_divergent = "明顯背離(弱於大盤)"
                        elif taiex_return < -0.05 and stock_return > 0.10: is_divergent = "明顯背離(強於大盤)"

                        stock_volatility_results.append({
                            "年度": str(year), 
                            "高點": f"{float(row['High']):.1f}", 
                            "低點": f"{float(row['Low']):.1f}", 
                            "走勢評估": is_divergent
                        })

        # --- 財務資料整理 ---
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
            total, count = 0, 0
            for m in months:
                if (year, m) in monthly_rev_map: total += monthly_rev_map[(year, m)]; count += 1
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
            if curr_dt > quarter_buckets[q_str]["DateObj"]: quarter_buckets[q_str]["DateObj"] = curr_dt
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
                if row['type'] in eps_keys: add_candidate(row['date'], "EPS", row['type'], row['value'])
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
                if row['type'] in cf_keys: add_candidate(row['date'], "CF", row['type'], row['value'])

        sorted_quarters = sorted(quarter_buckets.keys(), reverse=True)[:6]
        final_struct = { "營業收入": {}, "每股盈餘(EPS)": {}, "總資產": {}, "總負債": {}, "流動資產": {}, "流動負債": {}, "負債比": {}, "營業活動淨現金流": {} }
        REV_PRIORITY = ['OperatingRevenue', 'Revenue', 'TotalOperatingRevenue', 'NetRevenue', 'SalesRevenue', 'NetSales', 'InterestIncome', 'GrossProfit']
        EPS_PRIORITY = ['EPS', 'BasicEarningsPerShare', 'EarningsPerShare']
        CF_PRIORITY = ['NetCashInflowFromOperatingActivities', 'CashFlowsFromOperatingActivities', 'CashFlowFromOperatingActivities']

        for q in sorted_quarters:
            bucket = quarter_buckets[q]
            for p_key in EPS_PRIORITY:
                if p_key in bucket["EPS_Candidates"]:
                    final_struct["每股盈餘(EPS)"][q] = f"{bucket['EPS_Candidates'][p_key]:.2f}"; break
            found_rev = False
            for p_key in REV_PRIORITY:
                if p_key in bucket["Rev_Candidates"]:
                    final_struct["營業收入"][q] = f"{int(bucket['Rev_Candidates'][p_key]/1000):,}"; found_rev = True; break
            if not found_rev:
                y, q_num = get_quarter_from_date(bucket["DateObj"])
                calc_rev = calculate_quarterly_rev(y, q_num)
                if calc_rev is not None: final_struct["營業收入"][q] = f"{int(calc_rev/1000):,} (月加總)"; found_rev = True
            for p_key in CF_PRIORITY:
                if p_key in bucket["CF_Candidates"]:
                    final_struct["營業活動淨現金流"][q] = f"{int(bucket['CF_Candidates'][p_key]/1000):,}"; break
            if bucket["Assets"]: 
                final_struct["總資產"][q] = f"{int(bucket['Assets']/1000):,}"
                if bucket["Liabs"]:
                    final_struct["總負債"][q] = f"{int(bucket['Liabs']/1000):,}"
                    if bucket["Assets"] > 0: final_struct["負債比"][q] = f"{(bucket['Liabs'] / bucket['Assets']) * 100:.2f}%"
            if bucket["CurAssets"]: final_struct["流動資產"][q] = f"{int(bucket['CurAssets']/1000):,}"
            if bucket["CurLiabs"]: final_struct["流動負債"][q] = f"{int(bucket['CurLiabs']/1000):,}"

        if data_rev:
            rows = sorted(data_rev, key=lambda x: x['date'], reverse=True)
            for row in rows[:8]:
                final_struct["營業收入"][f"{row['date'][:7]} (月)"] = f"{int(row['revenue']/1000):,}"

        formatted_list = []
        order = ["營業收入", "總資產", "總負債", "負債比", "流動資產", "流動負債", "每股盈餘(EPS)", "營業活動淨現金流"]
        for item_name in order:
            if final_struct[item_name]:
                row_dict = {"項目": item_name}; row_dict.update(final_struct[item_name]); formatted_list.append(row_dict)
            else: formatted_list.append({"項目": item_name})
        
        if stock_volatility_results: formatted_list.append({"項目": "近三年股價與大盤", "股價分析數據": stock_volatility_results})
        formatted_list.append({"項目": "資料來源", "說明": "FinMind (綜合財報與股價)"})
        return formatted_list

    except Exception as e:
        print(f"Error: {e}")
        return None

def fetch_and_upload_data(stock_code, stock_name_tw=None, market_type="上市", force_finmind=False):
    try:
        fm_data_list = fetch_finmind_data_history(stock_code)
        if fm_data_list:
             final_name = stock_name_tw if stock_name_tw else stock_code
             payload = {"code": stock_code, "name": final_name, "financial_data": fm_data_list, "updated_at": datetime.now().isoformat()}
             supabase.table("underwriting_cache").upsert(payload).execute()
             return True, f"✅ 完整更新成功: {final_name}"
        else:
             return False, f"❌ 查無資料 (可能無財報或股價): {stock_name_tw}"
    except Exception as e: return False, str(e)


# ==========================================
# ⚡ 核心引擎 2：增量更新 (只抓股價，省額度，排錯0)
# ==========================================
def fetch_and_update_price_only(stock_code):
    try:
        res = supabase.table("underwriting_cache").select("*").eq("code", stock_code).execute()
        if not res.data:
            return False, f"❌ 資料庫中找不到 {stock_code}，請先執行「完整抓取」。"
        
        existing_data = res.data[0].get('financial_data', [])
        final_name = res.data[0].get('name', stock_code)

        start_date = (datetime.now() - timedelta(days=1095)).strftime('%Y-%m-%d')
        base_url = "https://api.finmindtrade.com/api/v4/data"
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        def get_fm_dataset_price(dataset_name, override_id=None):
            target_id = override_id if override_id else stock_code
            params = {"dataset": dataset_name, "data_id": target_id, "start_date": start_date}
            try:
                time.sleep(1.5)
                r = requests.get(base_url, params=params, headers=headers, timeout=5)
                if r.json().get('msg') == 'success': return r.json().get('data', [])
            except: pass
            return []

        data_stock_price = get_fm_dataset_price("TaiwanStockPrice")
        data_taiex_price = get_fm_dataset_price("TaiwanStockPrice", override_id="TAIEX")

        if not data_stock_price:
            return False, f"❌ 抓不到 {stock_code} 的歷史股價"

        df_stock = pd.DataFrame(data_stock_price)
        df_stock['date'] = pd.to_datetime(df_stock['date'])
        df_stock['Year'] = df_stock['date'].dt.year
        
        df_clean_stock = df_stock[df_stock['close'] > 0].copy()
        
        if df_clean_stock.empty:
            return False, f"❌ {stock_code} 所有的股價數值皆異常(為0)"

        stock_yearly = df_clean_stock.groupby('Year').agg(
            High=('max', 'max'), Low=('min', 'min'),
            Close_Last=('close', 'last'), Close_First=('close', 'first')
        ).tail(3)

        taiex_yearly_returns = {}
        if data_taiex_price:
            df_taiex = pd.DataFrame(data_taiex_price)
            df_taiex['date'] = pd.to_datetime(df_taiex['date'])
            df_taiex['Year'] = df_taiex['date'].dt.year
            df_clean_taiex = df_taiex[df_taiex['close'] > 0].copy()
            taiex_yearly = df_clean_taiex.groupby('Year').agg(
                Close_Last=('close', 'last'), Close_First=('close', 'first')
            ).tail(3)
            for year, row in taiex_yearly.iterrows():
                taiex_yearly_returns[year] = (row['Close_Last'] - row['Close_First']) / row['Close_First']

        stock_volatility_results = []
        for year, row in stock_yearly.iterrows():
            stock_return = (row['Close_Last'] - row['Close_First']) / row['Close_First']
            taiex_return = taiex_yearly_returns.get(year, 0)
            is_divergent = "大致相符"
            if taiex_return > 0.05 and stock_return < -0.10: is_divergent = "明顯背離(弱於大盤)"
            elif taiex_return < -0.05 and stock_return > 0.10: is_divergent = "明顯背離(強於大盤)"

            stock_volatility_results.append({
                "年度": str(year),
                "高點": f"{float(row['High']):.1f}",
                "低點": f"{float(row['Low']):.1f}",
                "走勢評估": is_divergent
            })

        new_financial_data = [item for item in existing_data if item.get("項目") != "近三年股價與大盤"]
        new_financial_data.append({"項目": "近三年股價與大盤", "股價分析數據": stock_volatility_results})

        payload = {
            "code": stock_code,
            "name": final_name,
            "financial_data": new_financial_data,
            "updated_at": datetime.now().isoformat()
        }
        supabase.table("underwriting_cache").upsert(payload).execute()
        return True, f"⚡ 股價增量更新成功: {final_name}"

    except Exception as e:
        return False, f"❌ 增量更新失敗: {str(e)}"

# --- 5. UI 介面 ---
tab1, tab2, tab3, tab4 = st.tabs(["🔍 補漏監控", "🚑 資料修補 (Fix)", "🕵️ 深度診斷", "📝 單筆手動"])

with tab1:
    st.markdown("### 📉 缺漏名單補足")
    st.info("💡 批次掃描建議在非尖峰時段執行，以免觸發 FinMind API 流量限制。")
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
    st.markdown("### 🚑 興櫃資料修補中心")
    st.warning("⚠️ 批次強制修補非常耗費 API 額度，請謹慎使用。")
    if st.button("🔍 1. 掃描需修補名單 (無營收/EPS)"):
        with st.spinner("分析資料庫品質中..."):
            all_data = get_all_db_data()
            repair_list = []
            for item in all_data:
                code = str(item['code']); name = item['name']; fdata = item['financial_data']
                is_finmind, has_rev, eps_count = False, False, 0
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
            
    st.markdown("---")
    st.markdown("### ⚡ 全庫股價增量大補丸 (Smart Batch)")
    st.info("💡 系統會自動掃描，**自動跳過已經有股價的公司**。若中斷，可隨時按鈕接續進度！")
    
    if st.button("🚀 一鍵補齊所有公司股價"):
        with st.spinner("讀取全庫資料中..."):
            all_data = get_all_db_data()
            total = len(all_data)
            
            missing_price_list = []
            for item in all_data:
                fdata = item.get('financial_data', [])
                if isinstance(fdata, list):
                    has_price = any(row.get("項目") == "近三年股價與大盤" for row in fdata if isinstance(row, dict))
                    if not has_price:
                        missing_price_list.append(str(item['code']))
            
        if not missing_price_list:
            st.success(f"🎉 太棒了！全庫 {total} 家公司都已具備股價資料，無需更新。")
        else:
            st.warning(f"🔍 掃描完畢：共 {total} 家公司，有 {len(missing_price_list)} 家需要補足股價。")
            p_bar = st.progress(0)
            status = st.empty()
            success_cnt = 0
            
            for i, code in enumerate(missing_price_list):
                status.text(f"⏳ 正在補足: {code} ({i+1}/{len(missing_price_list)})...")
                ok, msg = fetch_and_update_price_only(code)
                if ok: 
                    success_cnt += 1
                p_bar.progress((i+1)/len(missing_price_list))
                
            st.success(f"✅ 批次更新完成！成功為 {success_cnt} 家公司補上股價。")

    # 🌟 新增的 0 元清洗工具 🌟
    st.markdown("---")
    st.markdown("### 🛠️ 股價異常 (0元) 專項修補工具")
    st.info("💡 專門掃描並修復股價低點/高點出現 `0.0` 的異常公司。")
    
    if st.button("🔍 1. 掃描 0 元異常公司"):
        with st.spinner("全庫掃描異常數據中..."):
            all_data = get_all_db_data()
            zero_price_list = []
            for item in all_data:
                fdata = item.get('financial_data', [])
                if isinstance(fdata, list):
                    for row in fdata:
                        if isinstance(row, dict) and row.get("項目") == "近三年股價與大盤":
                            stock_data = row.get("股價分析數據", [])
                            has_zero = False
                            for s_data in stock_data:
                                try:
                                    if float(s_data.get("低點", 1)) == 0.0 or float(s_data.get("高點", 1)) == 0.0:
                                        has_zero = True
                                        break
                                except: pass
                            if has_zero:
                                zero_price_list.append({"code": item['code'], "name": item['name']})
                            break
                            
            if zero_price_list:
                st.session_state.zero_price_list = zero_price_list
                st.warning(f"⚠️ 發現 {len(zero_price_list)} 家公司存在 0 元異常！")
                st.dataframe(pd.DataFrame(zero_price_list))
            else:
                st.success("🎉 太棒了！資料庫中沒有發現股價為 0 的異常公司。")
                if 'zero_price_list' in st.session_state:
                    del st.session_state.zero_price_list

    if 'zero_price_list' in st.session_state and st.session_state.zero_price_list:
        if st.button(f"🚑 2. 一鍵修復這 {len(st.session_state.zero_price_list)} 家公司"):
            p_bar = st.progress(0)
            status = st.empty()
            success_cnt = 0
            target_list = st.session_state.zero_price_list
            
            for i, comp in enumerate(target_list):
                code = comp['code']
                status.text(f"⏳ 正在修復: {code} {comp['name']} ({i+1}/{len(target_list)})...")
                ok, msg = fetch_and_update_price_only(code)
                if ok: 
                    success_cnt += 1
                p_bar.progress((i+1)/len(target_list))
                
            st.success(f"✅ 修復完成！成功更新 {success_cnt} 家公司的異常股價。")
            del st.session_state.zero_price_list

with tab3:
    st.markdown("### 🕵️ 深度診斷 (Debug)")
    debug_code = st.text_input("代號", value="4546")
    if st.button("診斷此公司"):
        st.info("功能維護中")

with tab4:
    st.markdown("### 📝 手動單筆查詢 (防鎖推薦)")
    s_in = st.text_input("輸入代號", value="2330", key="manual_in")
    m_type = st.radio("市場", ["上市", "上櫃/興櫃"], horizontal=True, key="manual_mkt")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📥 執行 (完整財報+股價)", type="primary"):
            with st.spinner(f"完整抓取 {s_in} (需消耗 6 次 API)..."):
                ok, msg = fetch_and_upload_data(s_in, market_type=("上市" if "上市" in m_type else "上櫃"))
                if ok: st.success(msg)
                else: st.error(msg)
                
    with col2:
        if st.button("⚡ 執行 (僅更新股價)", type="secondary"):
            with st.spinner(f"為 {s_in} 增量補上股價數據 (僅消耗 2 次 API)..."):
                ok, msg = fetch_and_update_price_only(s_in)
                if ok: st.success(msg)
                else: st.error(msg)
