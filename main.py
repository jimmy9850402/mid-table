import os
import json
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from supabase import create_client

# --- 1. 初始化與安全連線 ---
app = FastAPI(title="Fubon D&O API - Middleware Distributor")

# 讀取 Render 環境變數 (請確保已在 Render Dashboard 設定好)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# 建立 Supabase 連線
try:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("環境變數 SUPABASE_URL 或 SUPABASE_KEY 未設定")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    print(f"⚠️ 連線警告: {e}")
    supabase = None

# --- 2. 核心分析路由 ---
@app.post("/analyze")
async def analyze(request: Request):
    """
    接收 Copilot 請求 -> 讀取 Supabase 緩存 -> 生成 Markdown 表格 -> 回傳分析結果
    """
    try:
        # 解析請求
        body = await request.json()
        query = str(body.get("company", "")).strip()
        
        # 提取數字代碼 (例如 "2330")
        stock_id = "".join(filter(str.isdigit, query))
        if not stock_id:
            return JSONResponse({
                "error": "請提供正確的公司代號 (如 2881)", 
                "markdown_table": "❌ 無效的輸入，請輸入股票代碼。"
            }, status_code=200)

        # 檢查資料庫連線
        if not supabase:
            return JSONResponse({"error": "伺服器資料庫連線異常"}, status_code=500)

        # 3. 從 Supabase 讀取數據 (Single Source of Truth)
        # 直接查詢 underwriting_cache 表
        res = supabase.table("underwriting_cache").select("*").eq("code", stock_id).execute()
        
        if not res.data:
            return JSONResponse({
                "error": f"中台尚無 {stock_id} 數據",
                "markdown_table": f"⚠️ 系統中台尚未採集到 **{stock_id}** 的數據。\n\n請先至 Streamlit 採集端執行同步任務。",
                "conclusion": "無法判定 (缺數據)"
            }, status_code=200)

        # 取得資料記錄
        record = res.data[0]
        table_rows = record.get('financial_data', [])
        
        if not table_rows:
            return JSONResponse({"error": "數據格式異常 (空表格)"}, status_code=200)

        # --- 🔥 關鍵功能：後端生成 Markdown 表格 (解決方案 B) ---
        # 目的：讓 Copilot Studio 不需要寫任何公式，直接顯示字串即可。
        
        # 1. 動態抓取季度標籤 (Key)，例如 "114年 Q3", "114年 Q2"...
        # 排除 "項目" 這個 Key，並進行排序確保順序 (由新到舊)
        first_row_keys = list(table_rows[0].keys())
        quarters = sorted([k for k in first_row_keys if k != "項目"], reverse=True)
        
        # 2. 構建 Markdown 表頭
        # 格式範例: | 項目 | 114年 Q3 | 114年 Q2 | ... |
        md_header = "| 項目 | " + " | ".join(quarters) + " |"
        md_separator = "| :--- | " + " | ".join([":---"] * len(quarters)) + " |"
        
        # 3. 構建數據列
        md_rows = []
        for row in table_rows:
            # 依序取出該列在各個季度的數值，若無則填 N/A
            values = [str(row.get(q, "-")) for q in quarters]
            # 組合該行: | **營業收入** | 989,918,318 | ... |
            line = f"| **{row.get('項目', '未知')}** | " + " | ".join(values) + " |"
            md_rows.append(line)
            
        # 4. 組合最終字串
        final_markdown = f"{md_header}\n{md_separator}\n" + "\n".join(md_rows)
        # --------------------------------------------------------

        # --- 4. 執行 Group A 核保邏輯判定 ---
        # 找到「營業收入」那一列
        rev_row = next((item for item in table_rows if item["項目"] == "營業收入"), None)
        conclusion = "⚠️ 無法自動判定 (缺營收數據)"
        
        if rev_row and quarters:
            latest_q = quarters[0] # 取最新一季 (排序後的第一個)
            # 移除千分位逗號並轉為浮點數
            try:
                latest_rev_str = str(rev_row.get(latest_q, "0")).replace(",", "")
                latest_rev = float(latest_rev_str)
                
                # 判定門檻：150億 (單位為千元，故為 15,000,000)
                if latest_rev >= 15000000:
                    conclusion = "✅ **符合 Group A 核決授權門檻** (營收 > 150億)"
                else:
                    conclusion = "⚠️ **營收未達 Group A 門檻**，建議由總公司核決。"
            except:
                conclusion = "⚠️ 數據格式異常，無法計算門檻。"

        # --- 5. 回傳完整 Payload ---
        return {
            "header": f"【D&O 核保分析 - {record.get('name', stock_id)} ({stock_id})】",
            "markdown_table": final_markdown,  # <--- Copilot 直接顯示這個！
            "conclusion": conclusion,
            "raw_data": table_rows,            # 保留原始數據供 AI 分析用
            "sync_time": record.get("updated_at"),
            "status": "success"
        }

    except Exception as e:
        return JSONResponse({
            "error": f"API 處理異常: {str(e)}",
            "markdown_table": "❌ 系統內部發生錯誤，請聯繫 MA 開發人員。"
        }, status_code=200)

# --- 本地測試用 (Render 會自動使用 uvicorn 啟動) ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
