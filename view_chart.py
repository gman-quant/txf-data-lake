import argparse
from datetime import datetime
import os
import sys
import polars as pl
import pandas as pd

# 路徑設定：確保能引用 core 和 visualization
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.loader import DataLoader
from core.processor import DataProcessor
from visualization.chart_builder import ChartBuilder

# --- [Core Logic] Polars 價格校正函數 ---
def apply_adjustment(df, adj_table_path, timeframe):
    if not os.path.exists(adj_table_path):
        return df

    # 1. 讀取校正表
    adj_pd = pd.read_csv(adj_table_path)
    adj_pl = pl.from_pandas(adj_pd[['date', 'cum_delta']])
    
    # --- 🟢 關鍵修正：將校正邊界精確設定在結算抽離時刻 13:45:00 (5分鐘緩衝) ---
    adj_pl = adj_pl.with_columns([
        pl.col("date").str.to_date(format="%Y/%m/%d").alias("adj_date"),
        # 結算日 13:50 之後的資料就不該再吃這一筆修正值
        (pl.col("date") + " 13:50:00").str.to_datetime(format="%Y/%m/%d %H:%M:%S").alias("adj_dt")
    ]).sort("adj_dt")

    # 2. 準備 K 線資料
    if timeframe == '1d':
        left_on, right_on = "date", "adj_date"
        # 日線資料通常以日期為準
        df = df.with_columns(pl.col("date").cast(pl.Date))
    else:
        left_on, right_on = "ts", "adj_dt"
        # 分時線資料 (包含夜盤) 使用原始時間戳 ts
        df = df.with_columns(pl.col("ts").cast(pl.Datetime))

    # 3. 執行 Join Asof
    df = df.sort(left_on)
    df_adjusted = df.join_asof(
        adj_pl,
        left_on=left_on,
        right_on=right_on,
        strategy="forward"
    )

    # 4. 執行價格修正
    df_adjusted = df_adjusted.with_columns(pl.col("cum_delta").fill_null(0))

    # --- 診斷列印：檢查換盤點 ---
    if timeframe != '1d':
        # 抓取 1/21 13:45 與 15:00 的資料來對比
        check_points = df_adjusted.filter(
            (pl.col("ts").dt.date() == datetime(2026, 1, 21).date()) & 
            (pl.col("ts").dt.hour().is_in([13, 15]))
        )
        if not check_points.is_empty():
            print("📊 [Transition Check] 1/21 結算日轉換：")
            print(check_points.select(["ts", "close", "cum_delta"]))

    price_cols = ["open", "high", "low", "close"]
    df_adjusted = df_adjusted.with_columns([
        (pl.col(c) + pl.col("cum_delta")).alias(c) for c in price_cols
    ])

    return df_adjusted.select(df.columns)

# --- [Main Entry] ---
def main():
    # 1. 參數解析
    parser = argparse.ArgumentParser(description="TXF Interactive Chart Viewer")
    parser.add_argument('--symbol', type=str, default='TXF', help="商品代碼")
    parser.add_argument('--date', type=str, default=datetime.now().strftime('%Y-%m-%d'), help="開始日期")
    parser.add_argument('--end-date', type=str, default=None, help="結束日期")
    parser.add_argument('--tf', type=str, default='5m', help="K棒週期")
    parser.add_argument('--combine', action='store_true', help="合併日夜盤")
    # 新增校正開關
    parser.add_argument('--adjust', action='store_true', help="顯示校正後的連續價格")
    args = parser.parse_args()

    if args.end_date is None: args.end_date = args.date
    
    print(f"🔍 Task: {args.symbol} {args.tf} | {args.date} ~ {args.end_date} {'[Adjusted]' if args.adjust else '[Raw]'}")

    # 2. ETL 流程
    # [E]xtract: 讀取資料
    df_raw = DataLoader.load_kbars(args.symbol, args.tf, args.date, args.end_date)
    
    if df_raw.is_empty():
        print("❌ Data not found.")
        return

    # [A]djust: 價格校正 (在處理指標前執行)
    if args.adjust:
        print("🔧 Applying price adjustments...")
        # 指向你在 D 槽建立的數據中心
        ADJ_PATH = r"D:\txf-data\adjustments\txf_adjustment_table_final.csv"
        df_raw = apply_adjustment(df_raw, ADJ_PATH, args.tf)

    # [T]ransform: 資料運算 (顏色、指標)
    print("⚡️ Processing...")
    df_processed = DataProcessor.process_data(df_raw, args.tf, args.combine)

    # [L]oad/Visualize: 繪圖
    title_suffix = f"({args.date}~{args.end_date})"
    if args.combine: title_suffix += " [Comb]"
    if args.adjust: title_suffix += " [ADJ]"
    
    viewer = ChartBuilder(args.symbol, args.tf, title_suffix)
    try:
        viewer.plot(df_processed)
    except KeyboardInterrupt:
        print("\n👋 Chart closed by user. Exiting...")
        sys.exit(0)

if __name__ == "__main__":
    main()