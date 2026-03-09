import os
import json
import re
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

app = FastAPI(title="Fubon D&O API - Smart Clean Report")

# --- 1. 初始化與連線 ---
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
        
        # --- 2. 搜尋邏輯 ---
        if not query:
             return JSONResponse({"error": "Input Empty", "markdown_table": "❌ 未收到輸入值"}, status_code=200)

        stock_id = "".join(filter(str.isdigit, query))
        res = None
        if stock_id:
            res = supabase.table("underwriting_cache").select("*").eq("code", stock_id).execute()
        else:
            res = supabase.table("underwriting_cache").select("*").ilike("name", f"%{query}%").execute()

        if not res or not res.data:
            return JSONResponse({
                "error": "Not Found", 
                "markdown_table": f"⚠️ 中台無 **{query}** 數據，請先至 Streamlit 採集。",
                "conclusion": "缺數據"
            }, status_code=200)

        record = res.data[0]
        raw_rows = record.get('financial_data', [])
        
        if not raw_rows: 
            return JSONResponse({"markdown_table": "❌ 數據異常"}, status_code=200)

        # ==========================================
        # 🔥 核心邏輯：過濾空季度 (Smart Filter)
        # ==========================================
        
        # 1. 取得所有含 Q 的季度 Key
        all_keys = [k for k in raw_rows[0].keys() if k != "項目"]
        all_quarters = sorted([k for k in all_keys if "Q" in k], reverse=True)
        
        # 2. 【過濾邏輯】找出「營業收入」不是空值的季度
        rev_row = next((r for r in raw_rows if "營業收入" in r["項目"]), None)
        
        valid_quarters = []
        if rev_row:
            for q in all_quarters:
                val = str(rev_row.get(q, "-"))
                if val not in ["-", "0", "N/A", "None", "nan"]:
                    valid_quarters.append(q)
        else:
            valid_quarters = all_quarters

        # 3. 只取「有效季度」的前 5 個
        display_quarters = valid_quarters[:5]
        
        # 4. 處理年度 (Years) - 指定 2023(112) 與 2024(113)
        target_years_roc = [112, 113]
        display_years_labels = ["2023年", "2024年"]
        
        def get_year_val(roc_year, row):
            key_full = f"{roc_year}年"
            key_q4 = f"{roc_year}年 Q4"
            if key_full in row and str(row[key_full]) not in ["-", "None", "0"]:
                return str(row[key_full])
            elif key_q4 in row:
                return str(row[key_q4]) + " (Q4)"
            else:
                return "-"

        # 5. 組合表頭
        all_headers = display_quarters + display_years_labels
        
        header_str = "| 項目 | " + " | ".join(all_headers) + " |"
        sep_str = "| :--- | " + " | ".join([":---"] * len(all_headers)) + " |"
        
        # 6. 定義顯示項目
        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", 
            "每股盈餘(EPS)",
            "營業活動淨現金流"
        ]

        md_rows = []

        # 7. 填入數據
        for item_name in target_items:
            row_data = next((r for r in raw_rows if item_name in r["項目"]), None)
            vals = []
            
            if row_data:
                for q in display_quarters:
                    vals.append(str(row_data.get(q, "-")))
                
                for yr in target_years_roc:
                    vals.append(get_year_val(yr, row_data))
            else:
                vals = ["-"] * len(all_headers)

            md_rows.append(f"| **{item_name}** | " + " | ".join(vals) + " |")

        final_markdown = f"{header_str}\n{sep_str}\n" + "\n".join(md_rows)
        
        # ==========================================
        # 🌟 新增邏輯：抓取並組合「股價波動資訊」Markdown
        # ==========================================
        stock_row = next((r for r in raw_rows if r.get("項目") == "近三年股價與大盤"), None)
        if stock_row and "股價分析數據" in stock_row:
            stock_data = stock_row["股價分析數據"]
            
            # 建立股價表格的 Markdown
            vol_header = "\n\n### 四、股價波動資訊與大盤比較\n| 年度 | 高點 | 低點 | 走勢評估 |\n| :--- | :--- | :--- | :--- |\n"
            vol_rows = []
            divergence_summaries = [] # 收集背離說明
            
            for item in stock_data:
                # 組合表格的每一列
                vol_rows.append(f"| {item['年度']} | {item['高點']} | {item['低點']} | {item['走勢評估']} |")
                
                # 若有背離，準備寫入摘要說明
                if "背離" in item['走勢評估']:
                    divergence_summaries.append(f"({item['年度']}) 走勢{item['走勢評估']}。")
            
            # 組合最終的股價 Markdown 區塊
            stock_markdown = vol_header + "\n".join(vol_rows)
            
            # 加上摘要文字
            if divergence_summaries:
                stock_markdown += f"\n\n波動摘要說明：發現明顯背離。請核保人員留意以下年度：" + " ".join(divergence_summaries)
            else:
                stock_markdown += "\n\n波動摘要說明：近三年走勢與大盤大致相符。"
                
            # 將股價區塊拼接到原本的 final_markdown 後面
            final_markdown += stock_markdown

        # ==========================================
        # ⚖️ 核保判定邏輯 (Group A check)
        # ==========================================
        conclusion = "⚠️ 無法判定"
        try:
            if rev_row and display_quarters:
                latest_q = display_quarters[0]
                val_str = str(rev_row.get(latest_q, "0")).replace(",", "").split(" ")[0]
                val = float(val_str)
                
                if val > 15000000:
                    conclusion = "⚠️ **本案不符合 Group A** (營收 > 150億，屬大型企業)"
                else:
                    conclusion = "✅ **符合 Group A** (營收 < 150億)"
        except: pass

        return {
            "header": f"【D&O 完整財報 - {record.get('name')}】",
            "markdown_table": final_markdown,
            "conclusion": conclusion,
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({"error": str(e), "markdown_table": "❌ 系統處理錯誤"}, status_code=200)
