import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

# 1. 安全連線設定 (讀取 Render 環境變數)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="Fubon D&O API - Middleware Distributor")

@app.post("/analyze")
async def analyze(request: Request):
    try:
        body = await request.json()
        query = str(body.get("company", "")).strip()
        
        # 提取純數字代碼 (如 2330)
        stock_id = "".join(filter(str.isdigit, query))
        if not stock_id:
            return JSONResponse({"error": "請提供正確的公司代號 (如 2881)"}, status_code=200)

        # 2. 直接從 Supabase 中台讀取已校準的 JSON 資料
        res = supabase.table("underwriting_cache").select("*").eq("code", stock_id).execute()
        
        if not res.data:
            return JSONResponse({"error": f"中台尚無 {stock_id} 數據，請先在 Streamlit 執行同步。"}, status_code=200)

        # 取得採集端存入的完整記錄
        record = res.data[0]
        table_rows = record['financial_data'] # 這裡就是您剛才看到的 4 季表格 JSON

        # 3. 執行 D&O 核保邏輯判定 (Group A 門檻)
        # 以最新一季營收為準 (第一列的 rev 欄位)
        latest_rev_str = str(table_rows[0].get('營業收入', '0')).replace(',', '')
        latest_rev = float(latest_rev_str)
        
        # 判定規則：營收 >= 150 億 (15,000,000 千元)
        is_group_a = latest_rev >= 15000000
        
        # 4. 回傳 Copilot 專用 JSON 格式
        return {
            "header": f"【D&O 核保分析 - {record['name']} ({stock_id})】",
            "table": table_rows,
            "conclusion": "✅ 本案符合 Group A 核決授權門檻" if is_group_a else "⚠️ 營收未達門檻，建議由總公司核決人員評估。",
            "sync_time": f"數據最後同步時間：{record['updated_at']}",
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({"error": f"API 處理異常：{str(e)}"}, status_code=200)
