import os
import json
import re
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

app = FastAPI(title="Fubon D&O API - Intelligent Report")

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
            return JSONResponse({"markdown_table": f"⚠️ 查無 {query} 數據，請至 Streamlit 採集。"}, status_code=200)

        record = res.data[0]
        raw_rows = record.get('financial_data', [])
        
        if not raw_rows: return JSONResponse({"markdown_table": "❌ 數據異常"}, status_code=200)

        # --- 🔥 MA 關鍵升級：智慧鎖定有效季度 ---
        
        all_keys = [k for k in raw_rows[0].keys() if k != "項目"]
        q_keys = sorted([k for k in all_keys if "Q" in k], reverse=True) # 所有季度 Key

        # 1. 找出「真正有數據」的最新一季 (過濾掉尚未公佈的空季度)
        latest_q = "N/A"
        rev_row = next((r for r in raw_rows if "營業收入" in r["項目"]), None)
        
        if rev_row:
            for q in q_keys:
                val = str(rev_row.get(q, "-"))
                # 如果該季營收不是 "-", "0", "N/A"，就認定這是最新的有效季度
                if val not in ["-", "0", "N/A", "None"]:
                    latest_q = q
                    break
        
        # 若真的都沒數據，就只好回退到原本的邏輯
        if latest_q == "N/A" and q_keys:
            latest_q = q_keys[0]

        # 2. 計算去年同期 (YoY)
        target_last_year_q = "N/A"
        match = re.match(r"(\d+)年 (Q\d)", latest_q)
        if match:
            target_last_year_q = f"{int(match.group(1)) - 1}年 {match.group(2)}"
        
        # 3. 年度資料處理
        def find_year_data(roc_year, row_data):
            key_full = f"{roc_year}年"
            key_q4 = f"{roc_year}年 Q4"
            if key_full in row_data and str(row_data[key_full]) not in ["-", "None"]:
                return str(row_data[key_full])
            elif key_q4 in row_data:
                return str(row_data[key_q4]) + " (Q4)"
            else:
                return "-"

        # --- 🔥 新增 EPS 至顯示列表 ---
        target_cols = ["最新季", "去年同期", "2023 年", "2024 年"]
        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", 
            "每股盈餘(EPS)",       # <--- 補上這個！
            "營業活動淨現金流"
        ]

        # 組建 Markdown
        # 標題自動更新顯示目前的最新季是哪一季
        md_header = f"| 項目 | 最新季 ({latest_q}) | 去年同期 | 2023 年 | 2024 年 |"
        md_sep = "| :--- | :--- | :--- | :--- | :--- |"
        md_rows = []

        for item_name in target_items:
            row_data = next((r for r in raw_rows if item_name in r["項目"]), None)
            vals = []
            if row_data:
                # 最新季
                vals.append(str(row_data.get(latest_q, "-")))
                # 去年同期
                vals.append(str(row_data.get(target_last_year_q, "-")))
                # 2023 (112)
                vals.append(find_year_data(112, row_data))
                # 2024 (113)
                vals.append(find_year_data(113, row_data))
            else:
                vals = ["-", "-", "-", "-"]

            md_rows.append(f"| **{item_name}** | " + " | ".join(vals) + " |")

        final_markdown = f"{md_header}\n{md_sep}\n" + "\n".join(md_rows)
        
        # --- 核保判定 (150億門檻) ---
        conclusion = "⚠️ 無法判定"
        try:
            if rev_row:
                # 取得有效最新季的數值
                val_str = str(rev_row.get(latest_q, "0")).replace(",", "").split(" ")[0]
                val = float(val_str)
                
                # 150 億 (15,000,000 千元)
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
