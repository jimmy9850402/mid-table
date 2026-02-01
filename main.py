import os
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

app = FastAPI(title="Fubon D&O API - Middleware Distributor")

# 1. 安全連線
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

try:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("環境變數未設定")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"⚠️ 連線警告: {e}")
    supabase = None

@app.post("/analyze")
async def analyze(request: Request):
    try:
        body = await request.json()
        query = str(body.get("company", "")).strip()
        
        # --- 🔥 MA 核心升級：雙軌搜尋邏輯 ---
        if not query:
             return JSONResponse({
                "error": "Input Empty",
                "markdown_table": "❌ 系統未收到輸入值。請確認 Copilot 是否正確傳遞了 `System.LastMessage.Text`。"
            }, status_code=200)

        stock_id = "".join(filter(str.isdigit, query))
        res = None

        if stock_id:
            # Case A: 使用者輸入代碼 (如 "2881") -> 查 code 欄位
            res = supabase.table("underwriting_cache").select("*").eq("code", stock_id).execute()
        else:
            # Case B: 使用者輸入中文 (如 "富邦金") -> 查 name 欄位 (模糊搜尋)
            res = supabase.table("underwriting_cache").select("*").ilike("name", f"%{query}%").execute()
            
        # ----------------------------------------

        # 檢查是否在中台找到資料
        if not res or not res.data:
            search_key = stock_id if stock_id else query
            return JSONResponse({
                "error": "Not Found",
                "markdown_table": f"⚠️ 中台尚未採集到 **{search_key}** 的數據。\n\n請先至 Streamlit 採集端執行同步任務。",
                "conclusion": "無法判定 (缺數據)"
            }, status_code=200)

        # 取得資料記錄
        record = res.data[0]
        table_rows = record.get('financial_data', [])
        
        # --- Markdown 表格生成 (保持不變) ---
        first_row_keys = list(table_rows[0].keys())
        quarters = sorted([k for k in first_row_keys if k != "項目"], reverse=True)
        
        md_header = "| 項目 | " + " | ".join(quarters) + " |"
        md_separator = "| :--- | " + " | ".join([":---"] * len(quarters)) + " |"
        
        md_rows = []
        for row in table_rows:
            values = [str(row.get(q, "-")) for q in quarters]
            line = f"| **{row.get('項目', '未知')}** | " + " | ".join(values) + " |"
            md_rows.append(line)
            
        final_markdown = f"{md_header}\n{md_separator}\n" + "\n".join(md_rows)

        # --- 核保判定邏輯 (保持不變) ---
        rev_row = next((item for item in table_rows if item["項目"] == "營業收入"), None)
        conclusion = "⚠️ 無法自動判定"
        if rev_row and quarters:
            try:
                latest_rev = float(str(rev_row.get(quarters[0], "0")).replace(",", ""))
                if latest_rev >= 15000000:
                    conclusion = "✅ **符合 Group A 核決授權門檻** (營收 > 150億)"
                else:
                    conclusion = "⚠️ **營收未達 Group A 門檻**，建議由總公司核決。"
            except: pass

        return {
            "header": f"【D&O 核保分析 - {record.get('name')} ({record.get('code')})】",
            "markdown_table": final_markdown,
            "conclusion": conclusion,
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({
            "error": str(e),
            "markdown_table": f"❌ 處理異常: {str(e)}"
        }, status_code=200)
