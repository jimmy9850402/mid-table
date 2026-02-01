import os
import json
import re
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

app = FastAPI(title="Fubon D&O API - Hybrid Report")

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
        
        # --- 2. 搜尋邏輯 (代號/名稱) ---
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
        # 🔥 核心邏輯：混合顯示 (最新 5 季 + 近 2 年)
        # ==========================================
        
        # 1. 處理季度 (Quarters) - 取最新的 5 季
        all_keys = [k for k in raw_rows[0].keys() if k != "項目"]
        # 篩選出含 "Q" 的 Key，並由新到舊排序 (例如 114年 Q3, 114年 Q2...)
        sorted_quarters = sorted([k for k in all_keys if "Q" in k], reverse=True)
        display_quarters = sorted_quarters[:5] # 只拿前 5 個
        
        # 2. 處理年度 (Years) - 指定 2023(112) 與 2024(113)
        target_years_roc = [112, 113] # 民國年
        display_years_labels = ["2023年", "2024年"] # 表頭顯示名稱
        
        # 輔助函數：找年度數據 (找不到找 Q4 替代)
        def get_year_val(roc_year, row):
            key_full = f"{roc_year}年"
            key_q4 = f"{roc_year}年 Q4"
            
            # A. 優先找整年數據 (且不是空值)
            if key_full in row and str(row[key_full]) not in ["-", "None", "0"]:
                return str(row[key_full])
            # B. 其次找 Q4 暫代，並標註
            elif key_q4 in row:
                return str(row[key_q4]) + " (Q4)"
            else:
                return "-"

        # 3. 組合表頭 (季度在前，年度在後)
        # 最終欄位 = [項目] + [Q1] [Q2] [Q3] [Q4] [Q5] + [2023] [2024]
        all_headers = display_quarters + display_years_labels
        
        header_str = "| 項目 | " + " | ".join(all_headers) + " |"
        sep_str = "| :--- | " + " | ".join([":---"] * len(all_headers)) + " |"
        
        # 4. 定義顯示項目 (對應 Streamlit 抓取的內容)
        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", 
            "每股盈餘(EPS)",
            "營業活動淨現金流"
        ]

        md_rows = []

        # 5. 填入數據
        for item_name in target_items:
            # 在 JSON 中找到對應的項目列
            row_data = next((r for r in raw_rows if item_name in r["項目"]), None)
            vals = []
            
            if row_data:
                # A. 填入 5 季數據
                for q in display_quarters:
                    vals.append(str(row_data.get(q, "-")))
                
                # B. 填入 2 年數據
                for yr in target_years_roc:
                    vals.append(get_year_val(yr, row_data))
            else:
                vals = ["-"] * len(all_headers)

            md_rows.append(f"| **{item_name}** | " + " | ".join(vals) + " |")

        final_markdown = f"{header_str}\n{sep_str}\n" + "\n".join(md_rows)
        
        # ==========================================
        # ⚖️ 核保判定邏輯 (Group A check)
        # ==========================================
        conclusion = "⚠️ 無法判定"
        try:
            rev_row = next((r for r in raw_rows if "營業收入" in r["項目"]), None)
            if rev_row and display_quarters:
                # 抓取「最新一季」的營收來判斷
                latest_q = display_quarters[0]
                val_str = str(rev_row.get(latest_q, "0")).replace(",", "").split(" ")[0] # 移除 (Q4) 等註記
                val = float(val_str)
                
                # 門檻：150億 (單位為千元 -> 15,000,000)
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
