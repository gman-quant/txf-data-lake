# view_chart.py

from datetime import datetime
import polars as pl
import pandas as pd
from lightweight_charts import Chart
import argparse
import os
import sys

# 確保路徑設定正確
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.settings import DATA_ROOT
from visualization.style_config import ColorScheme

def view_kbars(symbol='TXF', start_date='2025-12-05', end_date=None, timeframe='1m', combine_sessions=False):
    # 1. 處理日期範圍
    # 如果沒有給結束日，就預設只看開始日那一天
    if end_date is None:
        end_date = start_date

    print(f"🔍 Requesting Data: {symbol} {timeframe} | {start_date} to {end_date}")

    # 使用 Pandas 產生日期範圍 (包含 start 和 end)
    # 這能自動處理跨月、跨年問題
    target_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    df_list = []

    # === 分流讀取邏輯 ===
    
    # CASE A: 日線 (1d) - 讀取年檔並過濾
    if timeframe == '1d':
        # 找出這個日期範圍跨了哪幾年 (例如 2024-12-31 到 2025-01-02 跨了兩年)
        years = sorted(list(set([d.strftime('%Y') for d in target_dates])))
        
        for year in years:
            # 路徑: data/kbars/1d/TXF/TXF_1d_2025.parquet
            path = os.path.join(DATA_ROOT, "kbars", timeframe, symbol, f"{symbol}_{timeframe}_{year}.parquet")
            
            if os.path.exists(path):
                print(f"📂 Loading Year File: {path}")
                df_year = pl.read_parquet(path)
                df_list.append(df_year)
            else:
                print(f"⚠️ Warning: Year file not found: {path}")

        if not df_list:
            print("❌ No data found.")
            return

        # 合併多年份並過濾日期
        df = pl.concat(df_list).unique(subset=["date", "session"], keep="last").sort("ts")
        
        # 轉換輸入字串為 date 物件以進行過濾
        s_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        e_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        # 過濾出指定區間
        df = df.filter((pl.col("date") >= s_dt) & (pl.col("date") <= e_dt))

    # CASE B: 分時線 (1h, 1m...) - 讀取多個日檔並拼接
    else:
        for dt in target_dates:
            d_str = dt.strftime('%Y-%m-%d')
            year = dt.strftime('%Y')
            
            # 路徑: data/kbars/1m/TXF/2025/2025-12-05_TXF_1m.parquet
            path = os.path.join(DATA_ROOT, "kbars", timeframe, symbol, year, f"{d_str}_{symbol}_{timeframe}.parquet")
            
            if os.path.exists(path):
                # 為了避免洗版，這裡不印出每一個檔案，只在最後統計
                df_day = pl.read_parquet(path)
                df_list.append(df_day)
            # 若某天沒檔案 (例如週末)，直接跳過不報錯
        
        if not df_list:
            print(f"❌ No data found between {start_date} and {end_date}")
            return
            
        print(f"📦 Concatenating {len(df_list)} daily files...")
        # 🟢 [修正] 加入 .unique() 去重邏輯
        # 這是讓圖表復活的關鍵！
        df = (
            pl.concat(df_list)
            .unique(subset=["ts"], keep="last") # 根據時間去重，保留最新的一筆
            .sort("ts")                         # 確保時間嚴格遞增
        )

    # === 資料處理邏輯 (通用) ===

    # 🟢 全日聚合邏輯 (修正版 v2：處理週末跨日)
    if symbol == 'TXF' and timeframe == '1d' and combine_sessions:
        print("🔗 Combining Day + Night (handling Weekend shift)...")
        
        # 1. 日期位移邏輯 (包含週末判斷)
        df = df.with_columns(
            pl.when(pl.col("session") == "Night")
            .then(
                # 如果是夜盤，再檢查是星期幾
                pl.when(pl.col("date").dt.weekday() == 5)  # 5 = Friday
                .then(pl.col("date").dt.offset_by("3d"))   # 週五夜盤 -> 加3天變週一
                .otherwise(pl.col("date").dt.offset_by("1d")) # 其他平日 -> 加1天
            )
            .otherwise(pl.col("date")) # 日盤維持原樣
            .alias("trading_date")
        )

        # 2. 依據新的 trading_date 進行聚合
        df = (
            df.lazy()
            .sort("ts") 
            .group_by("trading_date") # 改用位移後的日期分組
            .agg([
                pl.col("ts").first(),   # 時間取最早 (會是前一晚 15:00)
                pl.col("open").first(), # Open 取最早
                pl.col("high").max(),   # High 取最大
                pl.col("low").min(),    # Low 取最小
                pl.col("close").last(), # Close 取最晚
                pl.col("volume").sum(), # 量加總
            ])
            .rename({"trading_date": "date"}) # 改回 date
            .sort("date")
            .collect()
        )

    # === 時間格式化 (Time Formatting) ===
    # 這裡決定圖表下方 X 軸顯示的時間
    
    if symbol == 'TXF' and timeframe == '1d' and combine_sessions:
        # 🟢 [修正點] 合併模式：只取日期 (YYYY-MM-DD)
        # 這樣 TradingView 就會把它當作標準日線，畫在正確的日期上
        df = df.with_columns(
            pl.col("date").dt.strftime("%Y-%m-%d").alias("time_str")
        )
    else:
        # 一般模式：精確到秒 (YYYY-MM-DD HH:MM:SS)
        # 顯示 08:45 或 15:00 以區分日夜盤
        df = df.with_columns(
            pl.col("ts").dt.strftime("%Y-%m-%d %H:%M:%S").alias("time_str")
        )

    # 準備資料列表
    bars_data = []
    for row in df.iter_rows(named=True):
        is_up = row['close'] >= row['open']

        # 取得 Session (若無則預設 Day)
        session_tag = row.get('session', 'Day')
        
        # 顏色邏輯
        main_color = ColorScheme.get_color(is_up, session_tag)

        bars_data.append({
            'time': row['time_str'], 
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume'],
            'color': main_color,
            'wickColor': main_color,
            'borderColor': main_color
        })

    # 轉 Pandas
    df_view = pd.DataFrame(bars_data)
    
    # 確認最後餵進去的資料
    print("👀 Data Preview (String Time):")
    print(df_view.head(3))
    print(f"... Total {len(df_view)} bars loaded ...")

    # 啟動 Chart
    chart = Chart(toolbox=True)

    # 🟢 [修改] 一行指令搞定所有樣式設定
    # 所有的顏色、背景、圖例設定都封裝在 style_config 裡了
    ColorScheme.apply_theme(chart)
    
    # 設定標題
    title_suffix = f"({start_date})" if start_date == end_date else f"({start_date} ~ {end_date})"
    if combine_sessions: title_suffix += " [Combined]"
    chart.topbar.textbox('symbol', f'{symbol} {timeframe} {title_suffix}')
    
    # 設定資料
    chart.set(df_view)
    
    # 強制適應螢幕
    chart.fit()
    
    print(f"🚀 Chart launching with {len(df_view)} candles...")
    chart.show(block=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    # Symbol
    parser.add_argument('--symbol', type=str, choices=['TSE', 'TXF'], default='TXF', help="Symbol code (default: TXF)")
    
    # Date Range
    today_str = datetime.now().strftime('%Y-%m-%d')
    parser.add_argument('--date', type=str, default=today_str, help=f"Start Date (default: {today_str})")
    
    # 🟢 [新增] End Date
    parser.add_argument('--end_date', type=str, default=None, help="End Date (YYYY-MM-DD). If not set, only show start_date.")

    # Timeframe
    parser.add_argument('--tf', type=str, choices=['1d', '1h', '60m', '5m', '1m', '5s'], default='5m', help="Timeframe (default: 5m)")
    
    # Combine
    parser.add_argument('--combine', action='store_true', help="Combine Day/Night sessions (default: False)")
    
    args = parser.parse_args()
    
    # 呼叫函式
    view_kbars(args.symbol, args.date, args.end_date, args.tf, args.combine)