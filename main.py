import os
import json
import re
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

app = FastAPI(title="Fubon D&O API - Smart Report")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

@app.post("/analyze")
async def analyze(request: Request):
    try:
        body = await request.json()
        query = str(body.get("company", "")).strip()
        
        if not query: return JSONResponse({"markdown_table": "❌ 輸入為空"}, status_code=200)

        # 搜尋邏輯
        stock_id = "".join(filter(str.isdigit, query))
        if stock_id:
            res = supabase.table("underwriting_cache").select("*").eq("code", stock_id).execute()
        else:
            res = supabase.table("underwriting_cache").select("*").ilike("name", f"%{query}%").execute()

        if not res or not res.data:
            return JSONResponse({"markdown_table": f"⚠️ 查無 {query} 數據"}, status_code=200)

        record = res.data[0]
        raw_rows = record.get('financial_data', [])
        
        # --- MA 智慧填補引擎 ---
        
        # 1. 找出所有時間 Key
        if not raw_rows: return JSONResponse({"markdown_table": "❌ 數據異常"}, status_code=200)
        all_keys = [k for k in raw_rows[0].keys() if k != "項目"]
        
        # 2. 定義時間點
        # A. 最新季
        q_keys = sorted([k for k in all_keys if "Q" in k], reverse=True) # ['114年 Q3', '114年 Q2'...]
        latest_q = q_keys[0] if q_keys else "N/A"
        
        # B. 去年同期 (嘗試計算，若無則找最舊的一季充當)
        target_last_year_q = "N/A"
        match = re.match(r"(\d+)年 (Q\d)", latest_q)
        if match:
            target_last_year_q = f"{int(match.group(1)) - 1}年 {match.group(2)}"
        
        # C. 年度資料 (112年=2023, 113年=2024)
        # 邏輯：如果找不到 '112年'，就找 '112年 Q4' 當作暫定年報
        def find_year_data(roc_year, row_data):
            key_full = f"{roc_year}年"
            key_q4 = f"{roc_year}年 Q4"
            
            if key_full in row_data:
                return str(row_data[key_full])
            elif key_q4 in row_data:
                return str(row_data[key_q4]) + " (Q4)"
            else:
                return "-"

        # 3. 欄位定義 (顯示名稱 -> 邏輯處理)
        target_cols = ["最新季", "去年同期", "2023 年", "2024 年"]
        
        # 4. 目標項目
        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", "營業活動淨現金流"
        ]

        # 5. 組建 Markdown
        md_header = "| 項目 | " + " | ".join(target_cols) + " |"
        md_sep = "| :--- | " + " | ".join([":---"] * len(target_cols)) + " |"
        md_rows = []

        for item_name in target_items:
            # 模糊比對項目名稱 (例如 '現金流' 可能叫 '營業現金流')
            row_data = next((r for r in raw_rows if item_name in r["項目"]), None)
            
            vals = []
            if row_data:
                # 最新季
                vals.append(str(row_data.get(latest_q, "-")))
                
                # 去年同期 (若找不到準確的，就填 '-' 保持誠實，或填最舊的一季)
                vals.append(str(row_data.get(target_last_year_q, "-")))
                
                # 2023年 (民國112)
                vals.append(find_year_data(112, row_data))
                
                # 2024年 (民國113)
                vals.append(find_year_data(113, row_data))
            else:
                vals = ["-", "-", "-", "-"] # 該項目完全沒抓到

            md_rows.append(f"| **{item_name}** | " + " | ".join(vals) + " |")

        final_markdown = f"{md_header}\n{md_sep}\n" + "\n".join(md_rows)
        
        # --- 核保判定 (150億門檻) ---
        conclusion = "⚠️ 無法判定"
        try:
            rev_row = next((r for r in raw_rows if "營業收入" in r["項目"]), None)
            if rev_row:
                val = float(str(rev_row.get(latest_q, "0")).replace(",", "").split(" ")[0]) # 移除 (Q4) 等註記
                if val > 15000000:
                    conclusion = "⚠️ **本案不符合 Group A** (營收 > 150億，屬大型企業)"
                else:
                    conclusion = "✅ **符合 Group A** (營收 < 150億)"
        except: pass

        return {
            "header": f"【D&O 財報分析 - {record.get('name')}】",
            "markdown_table": final_markdown,
            "conclusion": conclusion,
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({"markdown_table": f"❌ 處理異常: {str(e)}"}, status_code=200)
