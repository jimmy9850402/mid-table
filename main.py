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
        
        # 🔍 第一步：先去財報表找公司代號與財務數據
        if stock_id:
            res = supabase.table("underwriting_cache").select("*").eq("code", stock_id).execute()
        else:
            res = supabase.table("underwriting_cache").select("*").ilike("name", f"%{query}%").execute()

        if not res or not res.data:
            return JSONResponse({
                "error": "Not Found", 
                "markdown_table": f"⚠️ 中台無 **{query}** 財務數據，請先至 Streamlit 採集。",
                "conclusion": "缺數據"
            }, status_code=200)

        record = res.data[0]
        real_stock_code = record.get("code")
        company_name = record.get("name")
        raw_rows = record.get('financial_data', [])
        
        if not raw_rows: 
            return JSONResponse({"markdown_table": "❌ 財務數據異常"}, status_code=200)

        # ==========================================
        # 🔥 核心邏輯：動態抓取 4 個時間點 (修復版)
        # ==========================================
        
        # 1. 找到營業收入列，用來當作抓取欄位的基準 (解決第一列變成基本資訊的問題)
        rev_row = next((r for r in raw_rows if "營業收入" in r["項目"]), None)
        if not rev_row:
            return JSONResponse({"markdown_table": "❌ 找不到營業收入數據"}, status_code=200)

        all_keys = [k for k in rev_row.keys() if k != "項目"]
        all_quarters = sorted([k for k in all_keys if "Q" in k], reverse=True)
        
        # 2. 找出「營業收入」不是空值的有效最新季
        valid_quarters = []
        for q in all_quarters:
            val = str(rev_row.get(q, "-"))
            if val not in ["-", "0", "N/A", "None", "nan", "nan%"]:
                valid_quarters.append(q)

        latest_q = valid_quarters[0] if valid_quarters else "-"
        
        # 3. 自動推算「去年同期」
        ly_q = "-"
        if "年" in latest_q:
            y_str, q_str = latest_q.split("年")
            ly_q = f"{int(y_str)-1}年{q_str}"

        # 4. 自動抓取「最新完結的兩年」
        all_roc_years = set()
        for k in all_keys:
            if "年" in k and "Q" not in k:
                num_str = "".join(filter(str.isdigit, k))
                if num_str:
                    all_roc_years.add(int(num_str))
                    
        # 過濾出有營收數字的年度
        finished_years = sorted([y for y in all_roc_years if str(rev_row.get(f"{y}年", "-")) not in ["-", "None", "0", "nan%"]])
        
        if len(finished_years) >= 2:
            yr1_roc = finished_years[-2]
            yr2_roc = finished_years[-1]
        elif len(finished_years) == 1:
            yr1_roc = finished_years[0] - 1
            yr2_roc = finished_years[0]
        else:
            yr1_roc = 112
            yr2_roc = 113

        # ==========================================
        # 🌟 組合文字輸出給 AI (防止表格跑版)
        # ==========================================
        
        # 提取基本資訊給 AI
        basic_info_row = next((r for r in raw_rows if r.get("項目") == "公司基本資訊"), {})
        market_type = basic_info_row.get("市場別", "未知")
        industry_type = basic_info_row.get("產業別", "未知")
        company_name_db = basic_info_row.get("公司名稱", company_name)

        md_rows = [f"### 🏢 系統基本資訊\n* 公司名稱：{company_name_db}\n* 市場別：{market_type}\n* 產業別：{industry_type}\n"]
        md_rows.append("### 📊 核心財務數據 (系統精準提取)")

        target_items = [
            "營業收入", "總資產", "負債比", 
            "流動資產", "流動負債", 
            "每股盈餘(EPS)",
            "營業活動淨現金流"
        ]

        def get_year_val(roc_year, row):
            key_full = f"{roc_year}年"
            key_q4 = f"{roc_year}年 Q4"
            if key_full in row and str(row[key_full]) not in ["-", "None", "0", "nan%"]:
                return str(row[key_full])
            elif key_q4 in row and str(row[key_q4]) not in ["-", "None", "nan%"]:
                return str(row[key_q4]) + " (Q4)"
            else:
                return "-"

        # 依序組合 4 個時間點的文字
        for item_name in target_items:
            row_data = next((r for r in raw_rows if item_name in r["項目"]), None)
            if row_data:
                val_lq = str(row_data.get(latest_q, "-"))
                val_lyq = str(row_data.get(ly_q, "-"))
                val_y1 = get_year_val(yr1_roc, row_data)
                val_y2 = get_year_val(yr2_roc, row_data)
                
                md_rows.append(f"* **{item_name}** ── 最新季({latest_q}): {val_lq} / 去年同期({ly_q}): {val_lyq} / {yr1_roc+1911}年({yr1_roc}): {val_y1} / {yr2_roc+1911}年({yr2_roc}): {val_y2}")
            else:
                md_rows.append(f"* **{item_name}** ── 缺資料")

        final_markdown = "\n".join(md_rows)
        
        # ==========================================
        # 📈 新增邏輯：抓取並組合「股價波動資訊」
        # ==========================================
        stock_row = next((r for r in raw_rows if r.get("項目") == "近三年股價與大盤"), None)
        if stock_row and "股價分析數據" in stock_row:
            stock_data = stock_row["股價分析數據"]
            
            vol_header = "\n\n### 四、股價波動資訊與大盤比較\n| 年度 | 高點 | 低點 | 走勢評估 |\n| :--- | :--- | :--- | :--- |\n"
            vol_rows = []
            divergence_summaries = [] 
            
            for item in stock_data:
                vol_rows.append(f"| {item['年度']} | {item['高點']} | {item['低點']} | {item['走勢評估']} |")
                if "背離" in item['走勢評估']:
                    divergence_summaries.append(f"({item['年度']}) 走勢{item['走勢評估']}。")
            
            stock_markdown = vol_header + "\n".join(vol_rows)
            
            if divergence_summaries:
                stock_markdown += f"\n\n波動摘要說明：發現明顯背離。請核保人員留意以下年度：" + " ".join(divergence_summaries)
            else:
                stock_markdown += "\n\n波動摘要說明：近三年走勢與大盤大致相符。"
                
            final_markdown += stock_markdown

        # ==========================================
        # 🏆 核心功能：CMCR 財務信評分析
        # ==========================================
        cmcr_row = next((r for r in raw_rows if r.get("項目") == "CMCR評等"), None)
        cmcr_rating = cmcr_row.get(latest_q, "依系統規則試算中") if cmcr_row else "依系統規則試算中"
        final_markdown += f"\n\n### 五、CMCR 財務信評分析\n* **當前信評等級**：{cmcr_rating}\n* **數據來源**：優先採用 CMoney 財務平台數據\n"

        # ==========================================
        # 🚨 終極擴充：向第二張表調用「風險重訊」
        # ==========================================
        news_markdown = "\n\n### 六、近三年重大風險訊息 (公開資訊觀測站)\n"
        
        res_news = supabase.table("mops_news_cache").select("news_data").eq("stock_code", real_stock_code).execute()
        
        if res_news and res_news.data:
            news_list = res_news.data[0].get("news_data", [])
            
            if not news_list:
                news_markdown += "✅ **系統判定：目標公司近三年無重大風險新聞紀錄**。"
            else:
                news_markdown += "| 日期 | 主旨 | 內文摘要 |\n| :--- | :--- | :--- |\n"
                for news in news_list:
                    clean_subj = str(news.get("主旨", "")).replace("\n", "").replace("\r", "")
                    clean_content = str(news.get("內文", "")).replace("\n", "").replace("\r", "")
                    
                    if len(clean_content) > 150:
                        clean_content = clean_content[:150] + "..."
                        
                    news_markdown += f"| {news.get('日期', '-')} | {clean_subj} | {clean_content} |\n"
        else:
            news_markdown += "⚠️ 尚未採集此公司的風險重訊資料，請至數據中台補足。"
            
        final_markdown += news_markdown

        # ==========================================
        # ⚖️ 核保判定邏輯 (Group A check)
        # ==========================================
        conclusion = "⚠️ 無法判定"
        try:
            if rev_row and valid_quarters:
                val_str = str(rev_row.get(latest_q, "0")).replace(",", "").split(" ")[0]
                val = float(val_str)
                
                if val > 15000000:
                    conclusion = "⚠️ **本案不符合 Group A** (營收 > 150億，屬大型企業)"
                else:
                    conclusion = "✅ **符合 Group A** (營收 < 150億)"
        except: pass

        return {
            "header": f"【D&O 完整核保數據總匯 - {company_name}】",
            "markdown_table": final_markdown,
            "conclusion": conclusion,
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({"error": str(e), "markdown_table": "❌ 系統處理錯誤"}, status_code=200)
