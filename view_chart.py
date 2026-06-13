import argparse
from datetime import datetime, timedelta
import os
import sys
import time
import threading
import polars as pl
import pandas as pd

# 路徑設定：確保能引用 core 和 visualization
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.loader import DataLoader
from core.processor import DataProcessor
from visualization.chart_builder import ChartBuilder

# --- [Core Logic] Live Resampler ---
class LiveResampler:
    def __init__(self, timeframe: str):
        self.timeframe = timeframe
        self.tf_seconds = self._parse_timeframe_to_seconds(timeframe)

    def _parse_timeframe_to_seconds(self, tf: str) -> int:
        if tf.endswith('m'): return int(tf[:-1]) * 60
        elif tf.endswith('h'): return int(tf[:-1]) * 3600
        elif tf.endswith('s'): return int(tf[:-1])
        elif tf.endswith('d'): return int(tf[:-1]) * 86400
        return 60

    def get_bar_time(self, dt: datetime) -> datetime:
        from datetime import time as dt_time
        if self.timeframe == '1d':
            hr = dt.hour
            mi = dt.minute
            is_night_before_midnight = (hr > 13) or (hr == 13 and mi >= 45)
            
            trading_date = dt.date()
            if is_night_before_midnight:
                days_to_add = 1
                if trading_date.weekday() == 4: days_to_add = 3
                elif trading_date.weekday() == 5: days_to_add = 2
                trading_date = trading_date + timedelta(days=days_to_add)
            return datetime.combine(trading_date, dt_time(0, 0))

        t = dt.time()
        is_day = (t >= dt_time(8, 30)) and (t < dt_time(13, 45, 5))
        
        if is_day:
            aligned = dt - timedelta(hours=8, minutes=45)
            aligned_seconds = int(aligned.timestamp())
            aligned_seconds_floored = (aligned_seconds // self.tf_seconds) * self.tf_seconds
            dt_floored = datetime.fromtimestamp(aligned_seconds_floored)
            bar_dt = dt_floored + timedelta(hours=8, minutes=45)
        else:
            aligned = dt - timedelta(hours=15)
            aligned_seconds = int(aligned.timestamp())
            aligned_seconds_floored = (aligned_seconds // self.tf_seconds) * self.tf_seconds
            dt_floored = datetime.fromtimestamp(aligned_seconds_floored)
            bar_dt = dt_floored + timedelta(hours=15)
            
        return bar_dt

# --- [Core Logic] Live Delta Manager (全局內存池) ---
class LiveDeltaManager:
    """
    統一管理 Kafka 收到的 Live Delta。
    啟動時載入歷史缺口，之後背景執行緒不斷將新 Ticks 加入此記憶體池。
    """
    def __init__(self, kafka_reader):
        self.kafka_reader = kafka_reader
        self.live_ticks: pl.DataFrame = pl.DataFrame()
        self.lock = threading.Lock()
        
    def initialize(self, base_ts_ms: int):
        print(f"[LiveDelta] Fetching global gap ticks from timestamp {base_ts_ms}...")
        self.live_ticks = self.kafka_reader.fetch_gap_ticks(base_ts_ms)
        print(f"[LiveDelta] Fetched {len(self.live_ticks)} ticks for global memory cache.")
        
    def add_ticks(self, new_ticks: list):
        if not new_ticks:
            return
        new_df = pl.DataFrame(new_ticks).sort("ts")
        with self.lock:
            if self.live_ticks.is_empty():
                self.live_ticks = new_df
            else:
                self.live_ticks = self.live_ticks.vstack(new_df)
                
    def get_ticks(self) -> pl.DataFrame:
        with self.lock:
            return self.live_ticks.clone()

# --- [Core Logic] Delta Merge 拼接歷史與實時增量 ---
def perform_delta_merge(df_raw, ticks_df, timeframe, symbol, combined):
    """
    將全局的 Live Ticks 動態聚合為對應週期的 K 棒，並附加到歷史資料後。
    完全在記憶體中運作，極度快速 ($O(1)$ 網路呼叫)。
    """
    from datetime import datetime, time as dt_time
    
    if ticks_df.is_empty():
        return df_raw
        
    resampler = LiveResampler(timeframe)
    ts_list = ticks_df['ts'].to_list()
    close_list = ticks_df['close'].to_list()
    vol_list = ticks_df['volume'].to_list()
    
    bars = {}
    for i in range(len(ts_list)):
        ts_ms = ts_list[i]
        price = close_list[i]
        vol = vol_list[i]
        
        tick_dt = datetime.fromtimestamp(ts_ms / 1000.0)
        bar_dt = resampler.get_bar_time(tick_dt)
        
        t = tick_dt.time()
        is_day = (t >= dt_time(8, 30)) and (t < dt_time(13, 45, 5))
        session = "Day" if is_day else "Night"
        
        if not combined and session == "Night":
            continue
            
        bar_key = bar_dt.timestamp() if isinstance(bar_dt, datetime) else bar_dt
        
        if bar_key not in bars:
            bars[bar_key] = {
                "symbol": symbol,
                "date": bar_dt.date() if isinstance(bar_dt, datetime) else bar_dt,
                "session": session,
                "open": price, "high": price, "low": price, "close": price,
                "volume": float(vol),
            }
            if not df_raw.is_empty() and "ts" in df_raw.columns:
                bars[bar_key]["ts"] = bar_dt
            if not df_raw.is_empty() and "time" in df_raw.columns:
                bars[bar_key]["time"] = bar_dt.strftime("%H:%M:%S") if isinstance(bar_dt, datetime) else str(bar_dt)
        else:
            b = bars[bar_key]
            b['high'] = max(b['high'], price)
            b['low'] = min(b['low'], price)
            b['close'] = price
            b['volume'] += vol
            
    if not bars:
        return df_raw
        
    sorted_bars = [bars[k] for k in sorted(bars.keys())]
    replay_df = pl.DataFrame(sorted_bars)
    
    if df_raw.is_empty():
        return replay_df
        
    for col in df_raw.columns:
        if col in replay_df.columns:
            replay_df = replay_df.with_columns(pl.col(col).cast(df_raw.schema[col]))
        else:
            replay_df = replay_df.with_columns(pl.lit(None).cast(df_raw.schema[col]).alias(col))
            
    replay_df = replay_df.select(df_raw.columns)
    
    # 防止重複拼接：移除掉歷史資料中被 Live 涵蓋的最後一根未完成 K 棒
    if "ts" in df_raw.columns:
        first_replay_ts = replay_df["ts"][0]
        df_raw = df_raw.filter(pl.col("ts") < first_replay_ts)
    elif "date" in df_raw.columns:
        first_replay_date = replay_df["date"][0]
        df_raw = df_raw.filter(pl.col("date") < first_replay_date)
        
    return df_raw.vstack(replay_df)

# --- [Core Logic] Polars 價格校正函數 ---
def apply_adjustment(df, adj_table_path, timeframe):
    if not os.path.exists(adj_table_path):
        return df

    adj_pd = pd.read_csv(adj_table_path)
    adj_pl = pl.from_pandas(adj_pd[['date', 'cum_delta']])
    
    adj_pl = adj_pl.with_columns([
        pl.col("date").str.to_date(format="%Y/%m/%d").alias("adj_date"),
        (pl.col("date") + " 13:50:00").str.to_datetime(format="%Y/%m/%d %H:%M:%S").alias("adj_dt")
    ]).sort("adj_dt")

    if timeframe == '1d':
        left_on, right_on = "date", "adj_date"
        if "date" in df.columns:
            df = df.with_columns(pl.col("date").cast(pl.Date))
    else:
        left_on, right_on = "ts", "adj_dt"
        if "ts" in df.columns:
            df = df.with_columns(pl.col("ts").cast(pl.Datetime))

    if left_on not in df.columns:
        return df

    df = df.sort(left_on)
    df_adjusted = df.join_asof(
        adj_pl,
        left_on=left_on,
        right_on=right_on,
        strategy="forward"
    )

    df_adjusted = df_adjusted.with_columns(pl.col("cum_delta").fill_null(0))

    price_cols = ["open", "high", "low", "close"]
    df_adjusted = df_adjusted.with_columns([
        (pl.col(c) + pl.col("cum_delta")).alias(c) for c in price_cols if c in df_adjusted.columns
    ])

    return df_adjusted.select(df.columns)

# --- [Core Logic] 歷史 Parquet 讀取防爆處理 ---
def load_historical_raw(symbol, timeframe, date, end_date, combined, max_bars=20000, actual_max_date=None):
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
    
    calc_end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if actual_max_date:
        calc_end_dt = min(calc_end_dt, datetime.strptime(actual_max_date, "%Y-%m-%d"))
    else:
        calc_end_dt = min(calc_end_dt, datetime.now())
        
    orig_start_dt = datetime.strptime(date, "%Y-%m-%d")
    
    est_start_dt = calc_end_dt - timedelta(days=est_calendar_days)
    actual_start_dt = max(orig_start_dt, est_start_dt)
    actual_start_str = actual_start_dt.strftime("%Y-%m-%d")
    
    df_raw = DataLoader.load_kbars(symbol, timeframe, actual_start_str, end_date, combine_sessions=combined)
    return df_raw

# --- [Core Logic] 背景即時更新執行緒 ---
def start_live_kafka_listener(delta_manager, viewer, symbol: str, combined: bool):
    import time
    
    while not getattr(viewer, '_chart_shown', False):
        time.sleep(0.1)
        
    print(f"[Kafka Live] Thread started. Background polling activated.")
    last_update_time = 0
    
    def on_tick_cb(tick_data):
        nonlocal last_update_time
        # 背景只要單純把 ticks 加入全域記憶體即可
        if tick_data is not None:
            delta_manager.add_ticks([tick_data])
            
        if getattr(viewer, '_is_switching', False):
            return
        
        now = time.time()
        # 限制 UI 每半秒鐘最多更新一次，保持極致順暢不卡頓
        if now - last_update_time >= 0.5:
            last_update_time = now
            try:
                # 無論 UI 切換到哪個 Timeframe，都只拿「算好所有均線與指標」的最後一根 K 棒
                df_proc = viewer.on_timeframe_change_cb(viewer.timeframe, background_update=True)
                if df_proc is not None and not df_proc.is_empty():
                    last_row = df_proc.tail(1)
                    
                    live_bar = {
                        "time": last_row["time"][0],
                        "open": float(last_row["open"][0]),
                        "high": float(last_row["high"][0]),
                        "low": float(last_row["low"][0]),
                        "close": float(last_row["close"][0]),
                        "volume": float(last_row["volume"][0]),
                        "color": last_row["color"][0],
                        "borderColor": last_row["borderColor"][0],
                        "wickColor": last_row["wickColor"][0],
                        "vol_color": last_row["vol_color"][0]
                    }
                    if "TAIEX" in last_row.columns and last_row["TAIEX"][0] is not None:
                        live_bar["TAIEX"] = float(last_row["TAIEX"][0])
                        
                    for col in last_row.columns:
                        if col.startswith("ma"):
                            period = int(col.replace("ma", ""))
                            from visualization.style_config import ColorScheme
                            ma_type = ColorScheme.MA_SETTINGS[period].get('type', 'SMA')
                            label = f"{ma_type}{period}"
                            val = last_row[col][0]
                            live_bar[label] = float(val) if val is not None else None
                            
                    viewer.update_live_bar(live_bar)
            except Exception as e:
                print(f"[LiveDelta] Warning during background UI update: {e}")
                
    try:
        while getattr(viewer, '_chart_shown', False):
            ticks = delta_manager.kafka_reader.poll_new_ticks()
            if ticks:
                for t in ticks:
                    on_tick_cb(t)
            else:
                # 若無新資料，手動呼叫 on_tick_cb 觸發每 0.5s 的 UI 更新檢查
                on_tick_cb(None)
            
            time.sleep(0.05)
    except Exception as e:
        print(f"[Kafka Live] Thread exception: {e}")
    finally:
        print("[Kafka] Closing Reader...")
        delta_manager.kafka_reader.close()

# --- [Main Entry] ---
def main():
    # 1. 參數解析
    parser = argparse.ArgumentParser(description="TXF Interactive Chart Viewer (Pro Architecture)")
    parser.add_argument('--symbol', type=str, default='TXF', help="商品代碼")
    parser.add_argument('--date', type=str, default=None, help="開始日期")
    parser.add_argument('--end-date', type=str, default=None, help="結束日期")
    parser.add_argument('--tf', type=str, default='1d', help="K棒週期")
    parser.add_argument('--combined', '--combine', dest='combined', action='store_true', help="合併日夜盤")
    parser.add_argument('--adjust', action='store_true', help="顯示校正後的連續價格")
    parser.add_argument('--no-tse', action='store_true', help="不載入 TSE 對照線")
    parser.add_argument('--tfs', type=str, nargs='+', default=['1m', '5m', '15m', '30m', '1h', '4h', '1d'], help="圖表週期選項")
    parser.add_argument('--max-bars', type=int, default=20000, help="最多載入的 K 棒數量")
    
    # Kafka Live 參數
    parser.add_argument('--live', action='store_true', help="啟用 Kafka 實時看盤系統")
    parser.add_argument('--kafka-broker', type=str, default='192.168.1.50:9092', help="Kafka Broker 地址")
    parser.add_argument('--kafka-topic', type=str, default='txf-tick', help="Kafka 訂閱主題")
    args = parser.parse_args()

    if args.end_date is None: args.end_date = datetime.now().strftime('%Y-%m-%d')
    if args.date is None: args.date = "2000-01-01"
    
    print(f"[Task] {args.symbol} {args.tf} | {args.date} ~ {args.end_date} {'[Adjusted]' if args.adjust else '[Raw]'}")

    # 快取字典 (僅存最原始的 Historical Parquet DataFrame，不存合併後的結果)
    cache_raw = {}
    cache_tse_raw = {}
    actual_max_date = None

    # 初始化 LiveDeltaManager
    delta_manager_instance = None
    kafka_reader_instance = None

    if args.live:
        from core.kafka_reader import KafkaTickReader
        import uuid
        try:
            unique_group = f"txf_chart_live_{uuid.uuid4().hex[:8]}"
            kafka_reader_instance = KafkaTickReader(broker_url=args.kafka_broker, topic=args.kafka_topic, group_id=unique_group)
            delta_manager_instance = LiveDeltaManager(kafka_reader_instance)
            
            # 使用 1m 的最新 Parquet 資料來定位時間，抓取 3 天作為緩衝，尋找最近一根 K 棒的真實毫秒數
            recent_dt = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            df_recent = load_historical_raw(args.symbol, '1m', recent_dt, args.end_date, False, max_bars=5000)
            
            base_ts_ms = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
            if not df_recent.is_empty() and "ts" in df_recent.columns:
                last_dt = df_recent["ts"][-1]
                if isinstance(last_dt, str):
                    last_dt = datetime.fromisoformat(last_dt)
                if isinstance(last_dt, datetime):
                    base_ts_ms = int(last_dt.timestamp() * 1000)

            delta_manager_instance.initialize(base_ts_ms)
        except Exception as e:
            print(f"[Kafka Warning] Could not connect or initialize: {e}")
            args.live = False

    # 核心資料獲取閉包 (Closure)
    def get_data(tf: str, background_update=False):
        nonlocal actual_max_date
        
        # 1. 取得歷史資料 (Parquet)
        if tf not in cache_raw:
            if not background_update: 
                print(f"[Process] Loading historical Parquet data for {tf}...")
            df_hist = load_historical_raw(
                args.symbol, tf, args.date, args.end_date, 
                args.combined, max_bars=args.max_bars,
                actual_max_date=actual_max_date
            )
            cache_raw[tf] = df_hist
            
            if actual_max_date is None and not df_hist.is_empty():
                if "time" in df_hist.columns:
                    actual_max_date = str(df_hist["time"][-1]).split(" ")[0]
                elif "date" in df_hist.columns:
                    actual_max_date = str(df_hist["date"][-1]).split(" ")[0]

        df_hist = cache_raw[tf]
        
        # 2. 動態拼接 Live Delta
        if delta_manager_instance is not None:
            ticks_df = delta_manager_instance.get_ticks()
            df_raw = perform_delta_merge(df_hist, ticks_df, tf, args.symbol, args.combined)
        else:
            df_raw = df_hist

        if df_raw.is_empty():
            return pl.DataFrame()
            
        # 3. 套用價格校正
        if args.adjust:
            ADJ_PATH = r"D:\txf-data\adjustments\txf_adjustment_table_final.csv"
            df_raw = apply_adjustment(df_raw, ADJ_PATH, tf)
            
        # 4. 指標與顏色處理
        df_proc = DataProcessor.process_data(df_raw, tf, args.combined)
        
        # 5. TSE 對照線處理
        if not args.no_tse and args.symbol == 'TXF':
            if tf not in cache_tse_raw:
                tse_hist = load_historical_raw('TSE', tf, args.date, args.end_date, args.combined, max_bars=args.max_bars)
                cache_tse_raw[tf] = tse_hist
            
            tse_hist = cache_tse_raw[tf]
            
            if delta_manager_instance is not None:
                ticks_df = delta_manager_instance.get_ticks()
                if "underlying_price" in ticks_df.columns:
                    tse_ticks = ticks_df.select([
                        pl.col("ts"), 
                        pl.col("underlying_price").alias("close"), 
                        pl.col("volume")
                    ])
                    tse_raw = perform_delta_merge(tse_hist, tse_ticks, tf, 'TSE', args.combined)
                else:
                    tse_raw = tse_hist
            else:
                tse_raw = tse_hist
                
            tse_proc = DataProcessor.process_data(tse_raw, tf, args.combined)
            
            if not tse_proc.is_empty():
                if tf == '1d':
                    if "date" in tse_proc.columns:
                        tse_join = tse_proc.select([pl.col("date"), pl.col("close").alias("TAIEX")])
                        df_proc = df_proc.join(tse_join, on="date", how="left")
                else:
                    if "time" in tse_proc.columns:
                        tse_join = tse_proc.select([pl.col("time"), pl.col("close").alias("TAIEX")])
                        df_proc = df_proc.join(tse_join, on="time", how="left")
                        
                if "TAIEX" in df_proc.columns:
                    df_proc = df_proc.with_columns(
                        pl.col("TAIEX").fill_null(strategy="forward").fill_null(strategy="backward")
                    )
            
        # Ensure strict chronological order for lightweight-charts
        if "ts" in df_proc.columns:
            df_proc = df_proc.sort("ts")
        elif "time" in df_proc.columns:
            df_proc = df_proc.sort("time")
            
        # 完美解決 lightweight-charts 的時間軸 Bug：
        # 將字串轉回時間物件，並強制指定為奈秒 ("ns")。
        # 徹底繞過 lightweight-charts-python 的 datetime64[us] 轉換 Bug：
        # 強制所有時間欄位轉型為 Datetime("ns")，讓 Pandas astype('int64') 計算出正確秒數
        if "time" in df_proc.columns:
            if tf == '1d' and args.combined:
                df_proc = df_proc.with_columns(
                    pl.col("time").str.to_datetime(format="%Y-%m-%d").cast(pl.Datetime("ns"))
                )
            else:
                df_proc = df_proc.with_columns(
                    pl.col("time").str.to_datetime(format="%Y-%m-%d %H:%M:%S").cast(pl.Datetime("ns"))
                )
            
        # 裁切最大顯示數量
        if len(df_proc) > args.max_bars:
            df_proc = df_proc.tail(args.max_bars)
            
        return df_proc

    # 初始載入
    df_processed = get_data(args.tf)
    
    if df_processed.is_empty():
        print("[Error] Initial data not found.")
        return

    # 繪圖
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
    
    # 啟動實時更新背景執行緒
    if args.live and kafka_reader_instance and delta_manager_instance:
        kafka_thread = threading.Thread(
            target=start_live_kafka_listener,
            args=(delta_manager_instance, viewer, args.symbol, args.combined),
            daemon=True
        )
        kafka_thread.start()

    try:
        viewer.plot(df_processed)
    except KeyboardInterrupt:
        print("\n[Info] Chart closed by user. Exiting...")
        sys.exit(0)

if __name__ == "__main__":
    main()