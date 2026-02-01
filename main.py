import os
import json
import re
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

app = FastAPI(title="Fubon D&O API - Smart Report")

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
        
        # --- 雙軌搜尋 (代碼/名稱) ---
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
                "markdown_table": f"⚠️ 中台無 **{query}** 數據，請先採集。",
                "conclusion": "缺數據"
            }, status_code=200)

        record = res.data[0]
        raw_rows = record.get('financial_data', [])

        # ==========================================
        # 🔥 MA 客製化邏輯：固定四欄位報表引擎
        # ==========================================
        
        # 1. 定義目標列 (您指定的6個項目)
        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", "營業活動淨現金流"
        ]

        # 2. 智慧鎖定時間欄位
        # 先找出所有可用的時間 Key (排除 '項目')
        if not raw_rows: 
            return JSONResponse({"markdown_table": "❌ 數據異常"}, status_code=200)
            
        all_keys = [k for k in raw_rows[0].keys() if k != "項目"]
        # 排序找出最新季 (例如 '114年 Q3')
        sorted_keys = sorted([k for k in all_keys if "Q" in k], reverse=True)
        latest_q = sorted_keys[0] if sorted_keys else "N/A"
        
        # 計算去年同期 (YoY)
        last_year_same_q = "N/A"
        match = re.match(r"(\d+)年 (Q\d)", latest_q)
        if match:
            roc_year = int(match.group(1))
            q_part = match.group(2)
            last_year_same_q = f"{roc_year - 1}年 {q_part}"

        # 定義顯示欄位對照表 (Display Name -> Data Key)
        # 註：2023年 = 民國112年; 2024年 = 民國113年
        col_mapping = [
            ("最新季", latest_q),
            ("去年同期", last_year_same_q),
            ("2023 年", "112年"),
            ("2024 年", "113年")  # 注意：若年報未出，此欄可能為空或需抓累計
        ]

        # 3. 建構 Markdown 表格
        # 表頭
        md_header = "| 項目 | " + " | ".join([c[0] for c in col_mapping]) + " |"
        md_sep = "| :--- | " + " | ".join([":---"] * len(col_mapping)) + " |"
        
        md_rows = []
        
        # 遍歷目標項目，依序抓取數據
        for item_name in target_items:
            # 在原始數據中找到這一列 (若找不到則建立假資料避免報錯)
            row_data = next((r for r in raw_rows if r["項目"] == item_name), None)
            
            vals = []
            for _, data_key in col_mapping:
                if row_data:
                    # 嘗試抓取，若無數據則顯示 "-"
                    val = str(row_data.get(data_key, "-"))
                else:
                    val = "N/A" # 該公司完全沒有這個會計項目 (如現金流)
                vals.append(val)
            
            md_rows.append(f"| **{item_name}** | " + " | ".join(vals) + " |")

        final_markdown = f"{md_header}\n{md_sep}\n" + "\n".join(md_rows)
        # ==========================================

        # --- 核保判定 (維持 Group A 150億門檻) ---
        conclusion = "⚠️ 無法判定"
        try:
            # 抓取最新季營收進行判斷
            rev_row = next((r for r in raw_rows if r["項目"] == "營業收入"), None)
            if rev_row:
                val_str = str(rev_row.get(latest_q, "0")).replace(",", "")
                # 門檻 150億 (單位千元 -> 15,000,000)
                if float(val_str) > 15000000:
                    conclusion = "⚠️ **本案不符合 Group A** (營收 > 150億，屬大型企業)"
                else:
                    conclusion = "✅ **符合 Group A** (營收 < 150億，屬中小型良質業務)"
        except: pass

        return {
            "header": f"【D&O 財報分析 - {record.get('name')}】",
            "markdown_table": final_markdown,
            "conclusion": conclusion,
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({"error": str(e), "markdown_table": "❌ 系統處理錯誤"}, status_code=200)
