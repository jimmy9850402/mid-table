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
st.set_page_config(page_title="富邦 D&O 補漏採集器 (V9.0)", layout="wide", page_icon="🛡️")
st.title("🛡️ D&O 智能核保 - 缺漏資料補足系統 (FinMind V8核心)")

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
    """從證交所抓取並合併清單"""
    sources = [
        ("上市", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"),
        ("上櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"),
        ("興櫃", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=5")
    ]
    all_dfs = []
    
    try:
        for market_name, url in sources:
            response = requests.get(url, verify=False)
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
            
        if all_dfs:
            return pd.concat(all_dfs, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"讀取清單失敗: {e}")
        return pd.DataFrame()

def get_existing_codes():
    """從 Supabase 取得目前已存在的公司代號"""
    try:
        response = supabase.table("underwriting_cache").select("code").range(0, 3000).execute()
        existing_codes = {str(item['code']) for item in response.data}
        return existing_codes
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

# --- 🔥 FinMind 救援投手 (V8 邏輯整合版) ---
def fetch_finmind_data(stock_code):
    """
    使用 FinMind API V4 抓取完整財報 (包含 V8 的所有修正與備援邏輯)
    """
    try:
        start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        base_url = "https://api.finmindtrade.com/api/v4/data"
        
        # 使用您的 Token
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMi0wNiAxNDoxNToxMSIsInVzZXJfaWQiOiJqaW1teTk4NTA0MDIiLCJlbWFpbCI6IjExMDI1NTAyNEBnLm5jY3UuZWR1LnR3IiwiaXAiOiIyMjMuMTM3LjEwMC4xMjgifQ.2ou0rtCaMqV7XXPBh28jGWFJ7_4EQrtr2CdhNQ5YznI"
        headers = {"Authorization": f"Bearer {token}"}

        # 共用請求函數
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

        # 抓取 4 大報表
        data_income = get_fm_dataset("TaiwanStockFinancialStatements")
        data_balance = get_fm_dataset("TaiwanStockBalanceSheet")
        data_cash = get_fm_dataset("TaiwanStockCashFlowsStatement")
        data_rev = get_fm_dataset("TaiwanStockMonthRevenue")

        if not any([data_income, data_balance, data_cash, data_rev]):
            return None

        result = {}
        
        # --- A. 解析 EPS & 季營收備援 ---
        if data_income:
            # EPS
            eps_rows = [x for x in data_income if x['type'] in ['EPS', 'BasicEarningsPerShare']]
            if eps_rows:
                latest = eps_rows[-1]
                key = date_to_roc_quarter(datetime.strptime(latest['date'], '%Y-%m-%d'))
                result['EPS_Key'] = key
                result['EPS_Val'] = f"{latest['value']:.2f}"
            
            # 備援營收 (如果月營收沒抓到，就用季營收)
            rev_rows = [x for x in data_income if x['type'] in ['OperatingRevenue', 'Revenue', 'TotalOperatingRevenue']]
            if rev_rows:
                latest = rev_rows[-1]
                key = date_to_roc_quarter(datetime.strptime(latest['date'], '%Y-%m-%d'))
                result['Quarterly_Rev_Key'] = key
                result['Quarterly_Rev_Val'] = f"{int(latest['value']/1000):,}"

        # --- B. 解析 資產負債 (修正 Key: TotalLiabilities & Liabilities) ---
        if data_balance:
            assets_rows = [x for x in data_balance if x['type'] == 'TotalAssets']
            liab_rows = [x for x in data_balance if x['type'] in ['TotalLiabilities', 'Liabilities']]
            
            if assets_rows and liab_rows:
                latest_asset = assets_rows[-1]
                latest_liab = liab_rows[-1]
                
                # 若日期接近 (取資產的日期當 Key)
                key = date_to_roc_quarter(datetime.strptime(latest_asset['date'], '%Y-%m-%d'))
                
                asset_val = latest_asset['value']
                liab_val = latest_liab['value']
                
                result['Assets_Key'] = key
                result['Assets_Val'] = f"{int(asset_val/1000):,}"
                
                result['Liab_Key'] = key
                result['Liab_Val'] = f"{int(liab_val/1000):,}"
                
                if asset_val > 0:
                    ratio = (liab_val / asset_val) * 100
                    result['DebtRatio_Val'] = f"{ratio:.2f}%"

        # --- C. 解析 現金流 (修正 Key: Flows 複數) ---
        if data_cash:
            target_types = [
                'CashFlowFromOperatingActivities', 
                'CashFlowsFromOperatingActivities', # 興櫃常見
                'NetCashFlowsFromUsedInOperatingActivities',
                'NetCashInflowFromOperatingActivities'
            ]
            cf_rows = [x for x in data_cash if x['type'] in target_types]
            if cf_rows:
                latest = cf_rows[-1]
                key = date_to_roc_quarter(datetime.strptime(latest['date'], '%Y-%m-%d'))
                result['CF_Key'] = key
                result['CF_Val'] = f"{int(latest['value']/1000):,}"

        # --- D. 解析 營收 (優先用月營收，沒有則用季營收) ---
        if data_rev:
            latest = data_rev[-1]
            key = f"{latest['date'][:7]} (月)"
            result['Rev_Key'] = key
            result['Rev_Val'] = f"{int(latest['revenue']/1000):,}"
        elif 'Quarterly_Rev_Val' in result:
            # 啟動備援
            result['Rev_Key'] = result['Quarterly_Rev_Key']
            result['Rev_Val'] = result['Quarterly_Rev_Val']

        return result

    except Exception as e:
        print(f"FinMind Error: {e}")
        return None

# --- 4. 核心爬蟲 (混合雙打) ---
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
            # yfinance 失敗 -> 啟動 FinMind V8
            fm_data = fetch_finmind_data(stock_code)
            
            if fm_data:
                source_used = "FinMind"
                
                # --- 組裝 FinMind 數據 ---
                # 1. 營收
                row_rev = {"項目": "營業收入"}
                if 'Rev_Key' in fm_data: row_rev[fm_data['Rev_Key']] = fm_data['Rev_Val']
                
                # 2. EPS
                row_eps = {"項目": "每股盈餘(EPS)"}
                if 'EPS_Key' in fm_data: row_eps[fm_data['EPS_Key']] = fm_data['EPS_Val']
                
                # 3. 總資產
                row_assets = {"項目": "總資產"}
                if 'Assets_Key' in fm_data: row_assets[fm_data['Assets_Key']] = fm_data['Assets_Val']
                
                # 4. 負債比
                row_debt = {"項目": "負債比"}
                if 'Assets_Key' in fm_data and 'DebtRatio_Val' in fm_data: 
                    row_debt[fm_data['Assets_Key']] = fm_data['DebtRatio_Val']
                
                # 5. 現金流
                row_cf = {"項目": "營業活動淨現金流"}
                if 'CF_Key' in fm_data: row_cf[fm_data['CF_Key']] = fm_data['CF_Val']

                # 6. 流動資產/負債 (FinMind 簡易版暫缺，顯示 "-")
                row_cur_assets = {"項目": "流動資產"}
                row_cur_liab = {"項目": "流動負債"}

                formatted_data = [
                    row_rev, row_assets, row_debt, 
                    row_cur_assets, row_cur_liab, # 補齊欄位避免前端報錯
                    row_eps, row_cf,
                    {"項目": "資料來源", "說明": "FinMind (興櫃備援)"}
                ]
            else:
                return False, f"❌ 無數據跳過: {stock_name_tw}"
        else:
            # 2. yfinance 成功 -> 正常處理 (不補算 Q4)
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
    st.markdown("### 📉 缺漏名單補足系統 (V9.0)")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 1. 掃描缺漏名單", type="primary"):
            with st.spinner("正在比對中..."):
                full_df = get_all_tw_companies()
                db_codes = get_existing_codes()
                
                if not full_df.empty:
                    full_df['code_str'] = full_df['代號'].astype(str).str.strip()
                    missing_df = full_df[~full_df['code_str'].isin(db_codes)].copy()
                    
                    st.session_state.missing_df = missing_df
                    st.session_state.db_count = len(db_codes)
                    st.success(f"掃描完成！發現 {len(missing_df)} 家缺漏。")

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
