import argparse
from datetime import datetime, timedelta
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
            print("[Check] [Transition Check] 1/21 Transition (CSV format):")
            print(check_points.select(["ts", "close", "cum_delta"]).to_pandas().to_csv(index=False))

    price_cols = ["open", "high", "low", "close"]
    df_adjusted = df_adjusted.with_columns([
        (pl.col(c) + pl.col("cum_delta")).alias(c) for c in price_cols
    ])

    return df_adjusted.select(df.columns)

# --- [Core Logic] 輔助載入與防爆處理 ---
def load_and_process_data(symbol, timeframe, date, end_date, combined, adjust, no_tse, max_bars=20000, actual_max_date=None):
    # 1. 智慧回推機制：防爆保護
    bars_per_day = 1
    if timeframe.endswith('m'):
        try:
            m = int(timeframe[:-1])
            bars_per_day = 1440 / m
        except:
            bars_per_day = 1440
    elif timeframe.endswith('h'):
        try:
            h = int(timeframe[:-1])
            bars_per_day = 24 / h
        except:
            bars_per_day = 24

    est_calendar_days = int((max_bars / bars_per_day) * 1.5) + 5
    
    # 決定計算基準日 (避免 end_date 在遙遠的未來導致回推落空)
    calc_end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if actual_max_date:
        calc_end_dt = min(calc_end_dt, datetime.strptime(actual_max_date, "%Y-%m-%d"))
    else:
        # 初次載入若無基準，則最多不超過系統當前時間
        calc_end_dt = min(calc_end_dt, datetime.now())
        
    orig_start_dt = datetime.strptime(date, "%Y-%m-%d")
    
    est_start_dt = calc_end_dt - timedelta(days=est_calendar_days)
    actual_start_dt = max(orig_start_dt, est_start_dt)
    actual_start_str = actual_start_dt.strftime("%Y-%m-%d")
    
    if actual_start_dt > orig_start_dt:
        print(f"[Info] {timeframe} timeframe selected. Date range truncated to {actual_start_str} ~ {end_date} (base: {calc_end_dt.strftime('%Y-%m-%d')}) to prevent memory issues.")

    # [E]xtract: 讀取資料
    df_raw = DataLoader.load_kbars(symbol, timeframe, actual_start_str, end_date, combine_sessions=combined)
    
    if df_raw.is_empty():
        print(f"[Warning] Data not found for {timeframe}.")
        return pl.DataFrame()

    df_tse = None
    if symbol == 'TXF' and not no_tse:
        df_tse = DataLoader.load_kbars('TSE', timeframe, actual_start_str, end_date, combine_sessions=combined)

    # [A]djust: 價格校正 (在處理指標前執行)
    if adjust:
        # 指向你在 D 槽建立的數據中心
        ADJ_PATH = r"D:\txf-data\adjustments\txf_adjustment_table_final.csv"
        df_raw = apply_adjustment(df_raw, ADJ_PATH, timeframe)

    # [T]ransform: 資料運算 (顏色、指標)
    df_processed = DataProcessor.process_data(df_raw, timeframe, combined)

    if df_tse is not None and not df_tse.is_empty():
        df_tse_processed = DataProcessor.process_data(df_tse, timeframe, combined)
        if timeframe == '1d':
            tse_join = df_tse_processed.select([pl.col("date"), pl.col("close").alias("TAIEX")])
            df_processed = df_processed.join(tse_join, on="date", how="left")
        else:
            tse_join = df_tse_processed.select([pl.col("time"), pl.col("close").alias("TAIEX")])
            df_processed = df_processed.join(tse_join, on="time", how="left")
            
        df_processed = df_processed.with_columns(
            pl.col("TAIEX").fill_null(strategy="forward").fill_null(strategy="backward")
        )
        
    # 裁切確保不超過 max_bars
    if len(df_processed) > max_bars:
        df_processed = df_processed.tail(max_bars)

    return df_processed

# --- [Main Entry] ---
def main():
    # 1. 參數解析
    parser = argparse.ArgumentParser(description="TXF Interactive Chart Viewer")
    parser.add_argument('--symbol', type=str, default='TXF', help="商品代碼")
    # 將預設的開始日期設為很久以前 (例如 2000 年)，讓防爆機制自動去裁切
    parser.add_argument('--date', type=str, default=None, help="開始日期 (預設自動抓取最近的 K 棒)")
    parser.add_argument('--end-date', type=str, default=None, help="結束日期 (預設為今天)")
    parser.add_argument('--tf', type=str, default='1d', help="K棒週期")
    parser.add_argument('--combined', '--combine', dest='combined', action='store_true', help="合併日夜盤")
    # 新增校正開關
    parser.add_argument('--adjust', action='store_true', help="顯示校正後的連續價格")
    # 新增 TSE 對照開關 (預設為顯示)
    parser.add_argument('--no-tse', action='store_true', help="在繪製 TXF 時不載入與顯示 TSE (TAIEX) 對照線")
    parser.add_argument('--tfs', type=str, nargs='+', default=['1m', '5m', '15m', '30m', '1h', '4h', '1d'], help="圖表切換器上顯示的預設週期選項")
    # 新增最大 K 棒數量參數
    parser.add_argument('--max-bars', type=int, default=20000, help="最多載入的 K 棒數量，防止記憶體爆滿")
    args = parser.parse_args()

    if args.end_date is None: 
        args.end_date = datetime.now().strftime('%Y-%m-%d')
    if args.date is None:
        args.date = "2000-01-01"
    
    print(f"[Task] {args.symbol} {args.tf} | {args.date} ~ {args.end_date} {'[Adjusted]' if args.adjust else '[Raw]'}")

    # 快取字典與實際最大日期
    cache = {}
    actual_max_date = None

    def get_data(tf: str):
        nonlocal actual_max_date
        if tf in cache:
            print(f"[Cache] Loaded {tf} data from memory cache ({len(cache[tf])} bars).")
            return cache[tf]
            
        print(f"[Process] Loading and processing data for {tf}...")
        df_proc = load_and_process_data(
            args.symbol, tf, args.date, args.end_date, 
            args.combined, args.adjust, args.no_tse,
            max_bars=args.max_bars,
            actual_max_date=actual_max_date
        )
        if not df_proc.is_empty():
            cache[tf] = df_proc
            # 更新已知的最大日期，作為後續其他週期回推的精確基準
            if actual_max_date is None:
                last_time_str = df_proc["time"][-1]
                actual_max_date = last_time_str.split(" ")[0]
        return df_proc

    # 2. ETL 流程與初次載入
    df_processed = get_data(args.tf)
    
    if df_processed.is_empty():
        print("[Error] Initial data not found.")
        return

    # [L]oad/Visualize: 繪圖
    title_suffix = f"({args.date}~{args.end_date})"
    if args.combined: title_suffix += " [Comb]"
    if args.adjust: title_suffix += " [ADJ]"
    
    viewer = ChartBuilder(
        symbol=args.symbol, 
        timeframe=args.tf, 
        title_suffix=title_suffix, 
        combine_sessions=args.combined,
        on_timeframe_change_cb=get_data,
        available_tfs=args.tfs
    )
    try:
        viewer.plot(df_processed)
    except KeyboardInterrupt:
        print("\n[Info] Chart closed by user. Exiting...")
        sys.exit(0)

if __name__ == "__main__":
    main()