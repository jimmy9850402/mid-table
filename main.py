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
        actual_stock_code = str(record.get('code'))
        raw_rows = record.get('financial_data', [])
        
        if not raw_rows: 
            return JSONResponse({"markdown_table": "❌ 數據異常"}, status_code=200)

        # ==========================================
        # 🔥 核心邏輯：過濾空季度 (Smart Filter)
        # ==========================================
        all_keys = [k for k in raw_rows[0].keys() if k != "項目"]
        all_quarters = sorted([k for k in all_keys if "Q" in k], reverse=True)
        
        rev_row = next((r for r in raw_rows if "營業收入" in r["項目"]), None)
        
        valid_quarters = []
        if rev_row:
            for q in all_quarters:
                val = str(rev_row.get(q, "-"))
                if val not in ["-", "0", "N/A", "None", "nan"]:
                    valid_quarters.append(q)
        else:
            valid_quarters = all_quarters

        display_quarters = valid_quarters[:5]
        
        # ==========================================
        # 🌟 升級功能：動態抓取最新年度，解除時間鎖定
        # ==========================================
        all_roc_years = set()
        for k in all_keys:
            if "年" in k and "Q" not in k:
                num_str = "".join(filter(str.isdigit, k))
                if num_str:
                    all_roc_years.add(int(num_str))
                    
        if all_roc_years:
            target_years_roc = sorted(list(all_roc_years))[-3:] # 永遠抓最新的三年
        else:
            target_years_roc = [113, 114, 115] # 萬一沒抓到，給予預設值

        # 直接在表頭寫清楚西元與民國年，例如 "2026年(115)"
        display_years_labels = [f"{yr+1911}年({yr})" for yr in target_years_roc]
        
        def get_year_val(roc_year, row):
            key_full = f"{roc_year}年"
            key_q4 = f"{roc_year}年 Q4"
            if key_full in row and str(row[key_full]) not in ["-", "None", "0"]:
                return str(row[key_full])
            elif key_q4 in row and str(row[key_q4]) not in ["-", "None"]:
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
        # 抓取並組合「股價波動資訊」Markdown
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
        # 🏆 核心功能：CMCR 財務信評分析 (保留優先權)
        # ==========================================
        cmcr_row = next((r for r in raw_rows if r.get("項目") == "CMCR評等"), None)
        cmcr_rating = cmcr_row.get(display_quarters[0], "依系統規則試算中") if cmcr_row and display_quarters else "依系統規則試算中"
        final_markdown += f"\n\n### 五、CMCR 財務信評分析\n* **當前信評等級**：{cmcr_rating}\n* **數據來源**：優先採用 CMoney 財務平台數據\n"

        # ==========================================
        # 🚨 跨表抓取高風險重訊 (mops_news_cache)
        # ==========================================
        news_markdown = "\n\n### 六、近期重大負面與風險重訊 (AI 智能過濾)\n"
        try:
            news_res = supabase.table("mops_news_cache").select("news_data").eq("stock_code", actual_stock_code).execute()
            
            if news_res and news_res.data:
                news_list = news_res.data[0].get("news_data", [])
                
                if news_list and len(news_list) > 0:
                    for idx, news in enumerate(news_list[:5]):
                        news_date = news.get("日期", "未知日期")
                        news_subject = news.get("主旨", "無主旨").replace('\n', '')
                        news_content = str(news.get("內文", ""))[:150] + "..." 
                        
                        news_markdown += f"**{idx+1}. [{news_date}] {news_subject}**\n> 摘要：{news_content}\n\n"
                    
                    if len(news_list) > 5:
                        news_markdown += f"*(註：系統另有 {len(news_list)-5} 筆早期風險紀錄，已省略顯示)*\n"
                else:
                    news_markdown += "✅ 近三年內無觸發高風險關鍵字 (如訴訟、掏空等) 之重大訊息。\n"
            else:
                news_markdown += "✅ 中台暫無該公司風險重訊紀錄。\n"
        except Exception as e:
            news_markdown += f"⚠️ 重訊資料庫連線或讀取失敗: {e}\n"

        final_markdown += news_markdown

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
                    conclusion = "⚠️ **本案不符合 Group A** (營收 大於 150億，屬大型企業)"
                else:
                    conclusion = "✅ **符合 Group A** (營收 小於 150億)"
        except: pass

        return {
            "header": f"【D&O 完整財報 - {record.get('name')}】",
            "markdown_table": final_markdown,
            "conclusion": conclusion,
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({"error": str(e), "markdown_table": "❌ 系統處理錯誤"}, status_code=200)
