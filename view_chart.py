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



# --- [Core Logic] Live Delta Manager (全局內存池) ---
class LiveDeltaManager:
    """
    統一管理從 Kafka 讀取到的 Live Delta，
    讓抓取歷史缺口與之後背景執行緒收到新 Ticks 寫入此記憶體池。
    """
    def __init__(self, kafka_reader):
        self.kafka_reader = kafka_reader
        self.live_ticks: pl.DataFrame = pl.DataFrame()
        self.lock = threading.Lock()
        self.simulation_queue = []
        
    def initialize(self, base_ts_ms: int, simulate_flow: bool = False):
        print(f"[LiveDelta] Fetching global gap ticks from timestamp {base_ts_ms}...")
        self.live_ticks = self.kafka_reader.fetch_gap_ticks(base_ts_ms)
        print(f"[LiveDelta] Fetched {len(self.live_ticks)} ticks for global memory cache.")
        
        if simulate_flow and not self.live_ticks.is_empty():
            print("[LiveDelta] Simulation Mode: Stashing ticks into simulation queue for 1000x replay.")
            self.simulation_queue = self.live_ticks.to_dicts()
            # 清空目前記憶體，讓圖表先畫歷史資料，後續再讓 Kafka Thread 把 queue 裡的資料慢慢塞進來
            self.live_ticks = pl.DataFrame()
        
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
        
    from core.resampler import resample_to_kbars
    
    # 1. 處理 ticks_df：將 Epoch ms 轉換為 Local Naive Datetime
    ticks_df_proc = ticks_df.with_columns([
        pl.from_epoch(pl.col("ts"), time_unit="ms")
          .dt.replace_time_zone("UTC")
          .dt.convert_time_zone("Asia/Taipei")
          .dt.replace_time_zone(None)
          .alias("ts"),
        pl.lit(symbol).alias("symbol")
    ])
    
    # 2. 直接使用 Parquet 的聚合引擎 (100% 邏輯一致)
    replay_df = resample_to_kbars(ticks_df_proc, timeframe)
    
    # 如果使用者沒有要求合併日夜盤，而且 timeframe 是 1d，resample_to_kbars 預設會分開日夜盤
    # 如果使用者要求合併日夜盤，我們在這裡不能提前合併，要交給後面的 DataProcessor._aggregate_sessions 處理！
    
    if df_raw.is_empty():
        return replay_df
        
    # 3. 型別對齊
    for col in df_raw.columns:
        if col in replay_df.columns:
            replay_df = replay_df.with_columns(pl.col(col).cast(df_raw.schema[col]))
        else:
            replay_df = replay_df.with_columns(pl.lit(None).cast(df_raw.schema[col]).alias(col))
            
    replay_df = replay_df.select(df_raw.columns)
    
    # 防止重複拼接：移除掉歷史資料中被 Live 涵蓋的最後一根未完成 K 棒
    if "ts" in df_raw.columns and "ts" in replay_df.columns and not replay_df.is_empty():
        first_replay_ts = replay_df["ts"][0]
        # 抹除毫秒誤差，確保精準濾除 Parquet 最後一根未完成的 K 棒
        first_replay_ts = first_replay_ts.replace(microsecond=0)
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
def load_historical_raw(symbol, timeframe, date, end_date, combined, max_bars=20000, actual_max_date=None, simulate_cut_date=None):
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
    
    if simulate_cut_date and not df_raw.is_empty():
        cut_dt = datetime.strptime(simulate_cut_date, "%Y-%m-%d").date()
        if "ts" in df_raw.columns:
            df_raw = df_raw.filter(pl.col("ts").dt.date() <= cut_dt)
        elif "date" in df_raw.columns:
            df_raw = df_raw.filter(pl.col("date").cast(pl.Date) <= cut_dt)
            
    return df_raw

# --- [Core Logic] 背景即時更新執行緒 ---
def start_live_kafka_listener(delta_manager, viewer, symbol: str, combined: bool, simulate_flow: bool = False):
    import time
    
    while not getattr(viewer, '_chart_shown', False):
        time.sleep(0.1)
        
    print(f"[Kafka Live] Thread started. Background polling activated. Simulate Flow: {simulate_flow}")
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
                            
                    # 依照您的要求，1d 不再需要跟著 Tick 瘋狂跳動即時更新
                    if viewer.timeframe != '1d':
                        viewer.update_live_bar(live_bar)
            except Exception as e:
                print(f"[LiveDelta] Warning during background UI update: {e}")
                
    sim_queue = getattr(delta_manager, 'simulation_queue', [])
    sim_idx = 0
    sim_start_real_time = time.time()
    sim_start_tick_time = sim_queue[0]['ts'] if sim_queue else 0
                
    try:
        while getattr(viewer, '_chart_shown', False):
            if simulate_flow and sim_idx < len(sim_queue):
                now = time.time()
                elapsed_real = now - sim_start_real_time
                # 1000倍速：現實世界 1 秒 = 模擬世界 1000 秒
                current_sim_time = sim_start_tick_time + (elapsed_real * 1000 * 1000)
                
                batch = []
                while sim_idx < len(sim_queue) and sim_queue[sim_idx]['ts'] <= current_sim_time:
                    batch.append(sim_queue[sim_idx])
                    sim_idx += 1
                    
                if batch:
                    # 批次餵進去，只呼叫最後一次 on_tick_cb(None) 觸發 UI 更新
                    delta_manager.add_ticks(batch)
                    on_tick_cb(None)
                else:
                    on_tick_cb(None)
            else:
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
    parser.add_argument('--simulate-cut-date', type=str, default=None, help="模擬 Parquet 只載入到此日期為止（例如 2026-06-11）")
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
            df_recent = load_historical_raw(args.symbol, '1m', recent_dt, args.end_date, False, max_bars=5000, simulate_cut_date=args.simulate_cut_date)
            
            base_ts_ms = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
            if not df_recent.is_empty() and "ts" in df_recent.columns:
                last_dt = df_recent["ts"][-1]
                if isinstance(last_dt, str):
                    last_dt = datetime.fromisoformat(last_dt)
                if isinstance(last_dt, datetime):
                    # --- 計算下一個交易時段的開始時間 ---
                    t = last_dt.time()
                    from datetime import time as dt_time
                    
                    if dt_time(5, 5) <= t <= dt_time(14, 0):
                        # 日盤結束 (通常在 13:45 或 13:50)，下一個是今天的夜盤 15:00
                        next_session_start = last_dt.replace(hour=15, minute=0, second=0, microsecond=0)
                    else:
                        # 夜盤結束 (通常在 05:00)，下一個是日盤 08:45
                        if t.hour >= 15:
                            next_date = last_dt.date() + timedelta(days=1)
                        else:
                            next_date = last_dt.date()
                            
                        if next_date.weekday() == 5: # 星期六 -> 星期一
                            next_date += timedelta(days=2)
                        elif next_date.weekday() == 6: # 星期日 -> 星期一
                            next_date += timedelta(days=1)
                            
                        next_session_start = datetime.combine(next_date, dt_time(8, 45, 0))
                        
                    base_ts_ms = int(next_session_start.timestamp() * 1000)

            simulate_flow = bool(args.simulate_cut_date)
            delta_manager_instance.initialize(base_ts_ms, simulate_flow)
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
                actual_max_date=actual_max_date,
                simulate_cut_date=args.simulate_cut_date
            )
            cache_raw[tf] = df_hist
            
            if actual_max_date is None and not df_hist.is_empty():
                if "time" in df_hist.columns:
                    actual_max_date = str(df_hist["time"][-1]).split(" ")[0]
                elif "date" in df_hist.columns:
                    actual_max_date = str(df_hist["date"][-1]).split(" ")[0]

        df_hist = cache_raw[tf]
        
        # 2. 動態拼接 Live Delta (依照使用者要求，1d 完全禁止 Kafka 更新，只吃 Parquet)
        if delta_manager_instance is not None and tf != '1d':
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
                tse_hist = load_historical_raw('TSE', tf, args.date, args.end_date, args.combined, max_bars=args.max_bars, simulate_cut_date=args.simulate_cut_date)
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
        simulate_flow = bool(args.simulate_cut_date)
        kafka_thread = threading.Thread(
            target=start_live_kafka_listener,
            args=(delta_manager_instance, viewer, args.symbol, args.combined, simulate_flow),
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