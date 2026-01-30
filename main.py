import os
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

# 1. 安全連線設定 (讀取 Render 環境變數)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(title="Fubon Insurance - D&O Data Middleware")

@app.post("/analyze")
async def analyze(request: Request):
    try:
        body = await request.json()
        query = str(body.get("company", "")).strip()
        
        # 提取代碼 (例如：2330)
        stock_id = "".join(filter(str.isdigit, query))
        if not stock_id:
            return JSONResponse({"error": "請輸入公司代號 (如 2881)"}, status_code=200)

        # 2. 從 Supabase 中台精準讀取
        # 查詢 underwriting_cache 表中的 financial_data 欄位
        res = supabase.table("underwriting_cache").select("*").eq("code", stock_id).execute()
        
        if not res.data:
            return JSONResponse({"error": f"中台尚無 {stock_id} 資料，請先至 Streamlit 執行同步。"}, status_code=200)

        # 3. 數據解析與核保邏輯判定
        raw_record = res.data[0]
        # financial_data 在 Supabase 為 JSONB 格式，Python 會自動轉為 List[Dict]
        table_rows = raw_record['financial_data'] 
        
        # 執行 Group A 門檻檢核 (150 億 = 15,000,000 千元)
        latest = table_rows[0]
        # 假設 latest 包含 'rev' 與 'dr' 欄位
        rev_val = float(str(latest.get('rev', '0')).replace(',', ''))
        dr_val = float(str(latest.get('dr', '0')).replace('%', ''))
        
        is_group_a = (rev_val >= 15000000) and (dr_val < 80)
        
        # 4. 回傳 Copilot 專用格式
        return {
            "header": f"【D&O 智能核保報告 - {raw_record['name']} ({stock_id})】",
            "table": table_rows,
            "conclusion": "✅ 符合 Group A" if is_group_a else "⚠️ 建議由總公司核決人員評估。",
            "cmcr": {"score": "2.1", "level": "低"}, # 評分可由中台預算好存入
            "source": f"📊 數據源：Fubon 數據中台 (同步時間：{raw_record['updated_at']})"
        }

    except Exception as e:
        return JSONResponse({"error": f"中台讀取異常：{str(e)}"}, status_code=200)
