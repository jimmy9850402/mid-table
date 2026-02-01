import streamlit as st
import pandas as pd
import yfinance as yf
from supabase import create_client
import os
from datetime import datetime

# --- 1. 初始化設定 ---
st.set_page_config(page_title="富邦 D&O 數據採集站", layout="wide")
st.title("📊 D&O 智能核保 - 數據採集終端")

# 讀取 Secrets (請確保 .streamlit/secrets.toml 或環境變數已設定)
# 若在本地運行，也可直接將 URL/KEY 填入下方字串 (但不建議 commit 到 github)
SUPABASE_URL = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY") or st.secrets.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("⚠️ 請設定 Supabase URL 與 Key")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- 2. 輔助函數：日期轉民國季別 ---
def date_to_roc_quarter(date_obj):
    """將 datetime 物件轉為 '114年 Q3' 格式"""
    year_roc = date_obj.year - 1911
    quarter = (date_obj.month - 1) // 3 + 1
    return f"{year_roc}年 Q{quarter}"

# --- 3. 核心爬蟲邏輯 (yfinance 版) ---
def fetch_and_upload_data(stock_code):
    status_text = st.empty()
    status_text.info(f"🔍 正在連線 Yahoo Finance 抓取 {stock_code}...")
    
    # 處理台股代號 (加上 .TW)
    ticker_symbol = f"{stock_code}.TW" if not stock_code.endswith(".TW") else stock_code
    stock = yf.Ticker(ticker_symbol)
    
    try:
        # A. 抓取三大報表 (Quarterly)
        # yfinance 的 quarterly_xxx 通常預設回傳近 4-5 季，我們盡量抓取
        bs = stock.quarterly_balance_sheet  # 資產負債表
        is_ = stock.quarterly_financials    # 損益表
        cf = stock.quarterly_cashflow       # 現金流量表 (關鍵!)

        if bs.empty or is_.empty:
            st.error(f"❌ 找不到 {stock_code} 的財務數據，請確認代號是否正確。")
            return

        # B. 合併報表 (以日期為 Index)
        # 轉置(T)讓日期變 Index，方便 concat
        df_merged = pd.concat([is_.T, bs.T, cf.T], axis=1)
        
        # 移除重複欄位 (有些項目名稱可能重複)
        df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]
        
        # C. 篩選與改名 (Mapping)
        # 定義我們要抓的項目 (英文 -> 中文)
        # 註：yfinance 的欄位名稱可能會隨版本變動，這裡列出常見名稱
        mapping = {
            "Total Revenue": "營業收入",
            "Operating Revenue": "營業收入", # 備用
            "Total Assets": "總資產",
            "Total Liabilities Net Minority Interest": "總負債", # 用於計算負債比
            "Total Liabilities": "總負債", # 備用
            "Current Assets": "流動資產",
            "Current Liabilities": "流動負債",
            "Basic EPS": "每股盈餘(EPS)",
            "Operating Cash Flow": "營業活動淨現金流", # 🔥 關鍵新增
            "Operating Cash Flow": "營業活動淨現金流"
        }
        
        # 準備打包的資料結構
        # 為了要讓 API 能算出 YoY，我們嘗試取最近 8 個時間點 (如果有的話)
        # sort_index(ascending=False) 確保最新的在前面
        df_merged.index = pd.to_datetime(df_merged.index)
        df_sorted = df_merged.sort_index(ascending=False).head(8) # 抓近 8 季
        
        # 建立目標項目清單
        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", "每股盈餘(EPS)", 
            "營業活動淨現金流"
        ]
        
        formatted_data = [] # 準備存成 List[Dict]

        for target_name in target_items:
            row_dict = {"項目": target_name}
            
            for date_idx in df_sorted.index:
                key_name = date_to_roc_quarter(date_idx) # 轉成 "114年 Q3"
                
                # 1. 負債比特殊計算
                if target_name == "負債比":
                    try:
                        liab = df_sorted.loc[date_idx].get("Total Liabilities Net Minority Interest") or df_sorted.loc[date_idx].get("Total Liabilities")
                        assets = df_sorted.loc[date_idx].get("Total Assets")
                        if liab and assets:
                            val = (liab / assets) * 100
                            row_dict[key_name] = f"{val:.2f}%"
                        else:
                            row_dict[key_name] = "-"
                    except:
                        row_dict[key_name] = "-"
                        
                # 2. 其他一般項目
                else:
                    # 找對應的英文欄位
                    found_val = None
                    for eng_col, ch_col in mapping.items():
                        if ch_col == target_name:
                            if eng_col in df_sorted.columns:
                                val = df_sorted.loc[date_idx, eng_col]
                                # 檢查是否為 NaN
                                if pd.notna(val):
                                    found_val = val
                                    break
                    
                    if found_val is not None:
                        # 單位換算：除了 EPS 和百分比，其他轉為「千元」
                        if target_name != "每股盈餘(EPS)":
                            # 原始數據通常是元，除以 1000
                            val_thousands = int(found_val / 1000)
                            # 格式化加上逗號
                            row_dict[key_name] = f"{val_thousands:,}"
                        else:
                            # EPS 保持原樣
                            row_dict[key_name] = f"{found_val:.2f}"
                    else:
                        row_dict[key_name] = "-"
            
            formatted_data.append(row_dict)

        # D. 上傳 Supabase
        stock_name = stock.info.get('longName', stock_code) # 嘗試抓中文名
        
        payload = {
            "code": stock_code,
            "name": stock_name,
            "financial_data": formatted_data,
            "updated_at": datetime.now().isoformat()
        }
        
        # Upsert (有則更新，無則新增)
        data, count = supabase.table("underwriting_cache").upsert(payload).execute()
        
        status_text.success(f"✅ 成功！{stock_code} ({stock_name}) 數據已同步至中台。")
        st.json(formatted_data) # 顯示預覽

    except Exception as e:
        st.error(f"❌ 發生錯誤: {str(e)}")
        # 顯示詳細錯誤以便除錯
        import traceback
        st.text(traceback.format_exc())

# --- 4. Streamlit UI 介面 ---
col1, col2 = st.columns([1, 2])

with col1:
    st.markdown("### 📥 數據同步中心")
    stock_input = st.text_input("輸入股票代號", value="2330", help="例如 2330, 2881")
    
    if st.button("🚀 執行採集 / 更新數據", type="primary"):
        if stock_input:
            fetch_and_upload_data(stock_input)
        else:
            st.warning("請輸入代號")

    st.markdown("---")
    st.markdown("""
    **功能說明：**
    * 來源：Yahoo Finance (即時)
    * 範圍：嘗試抓取近 8 季數據
    * 項目：包含現金流、營收、負債比
    * 單位：自動換算為「千元」
    """)

with col2:
    st.markdown("### 💾 中台數據預覽 (Supabase)")
    # 簡單的查詢功能查看目前 DB 狀況
    if st.button("🔄 重新整理資料庫列表"):
        try:
            res = supabase.table("underwriting_cache").select("code, name, updated_at").order("updated_at", desc=True).limit(10).execute()
            if res.data:
                df_db = pd.DataFrame(res.data)
                st.dataframe(df_db, use_container_width=True)
            else:
                st.info("目前資料庫為空")
        except Exception as e:
            st.error(f"讀取失敗: {e}")
