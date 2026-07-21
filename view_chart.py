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

# 全域結算日快取
GLOBAL_SETTLEMENT_DATES = None


def _detect_today_settlement_date() -> tuple[str, bool]:
    """
    啟動時即時判斷「今日是否為結算日」，完全不依賴 Parquet 歷史資料。

    三層確認（依可靠度排序）：
    1. settlement_calendar.csv 靜態查找
       → 最可靠，且已涵蓋未來月份（2026-07-21 起）
    2. 演算法推算：當月「第三個禮拜三」
       → 台指期標準結算日規則
    3. 假日順延：若第三個禮拜三是國定假日（非週末），Parquet 當日不存在，
       則逐日往後找，最多推 5 個工作日，直到找到有 Parquet 的交易日為止

    回傳: (today_trading_date_str: str, is_settlement: bool)
    """
    from datetime import date as _date, timedelta as _td
    import os as _os

    # 當前期交所「交易日」（+9h 使夜盤 15:00 對齊隔日）
    trading_date = (datetime.now() + timedelta(hours=9)).date()
    today_str = trading_date.strftime("%Y-%m-%d")

    # ── 層 1：CSV 靜態查找 ──────────────────────────────────────────
    try:
        from config.settings import SETTLEMENT_CALENDAR_PATH
        if _os.path.exists(SETTLEMENT_CALENDAR_PATH):
            _csv = pd.read_csv(SETTLEMENT_CALENDAR_PATH)
            if today_str in set(_csv['date'].astype(str).tolist()):
                print(f"[Settlement] Today {today_str} confirmed via CSV.")
                return today_str, True
    except Exception:
        pass

    # ── 層 2：演算法推算第三個禮拜三 ───────────────────────────────
    y, m = trading_date.year, trading_date.month
    first_day = _date(y, m, 1)
    # 第幾天才是第一個「禮拜三」(weekday 2)
    first_wed_offset = (2 - first_day.weekday()) % 7
    third_wed = first_day + _td(days=first_wed_offset + 14)  # 再加 14 天 = 第三個

    # ── 層 3：假日順延 ──────────────────────────────────────────────
    # 台指期規則：若第三個禮拜三是國定假日，結算日順延到下一個交易日。
    # 實作策略：嘗試讀取該日的 1m Parquet 是否存在；
    #   若不存在 → 假日（或週末）→ +1 天繼續試，最多試 5 天。
    # 若 Parquet 根目錄不可知，則退回僅跳過週末（保守版本）。
    try:
        from config.settings import DATA_ROOT
        parquet_root = _os.path.join(DATA_ROOT, "kbars", "1m", "TXF")

        def _parquet_exists(d: _date) -> bool:
            """嘗試判斷某日是否有 1m Parquet（= 是否為交易日）"""
            year_str = d.strftime("%Y")
            date_str = d.strftime("%Y-%m-%d")
            path = _os.path.join(parquet_root, year_str, f"{date_str}_TXF_1m.parquet")
            return _os.path.exists(path)

        candidate = third_wed
        # 最長連假估計：台灣春節可達 9-10 天（除夕前後各含補假、調整日）
        # 設 20 天上限，完全覆蓋任何已知連假情境
        _MAX_SEARCH_DAYS = 20
        _found = False
        for _i in range(_MAX_SEARCH_DAYS):
            # ── 邊界 1：候選日已超過今日 ──────────────────────────────────
            # 日盤結束後 Parquet 才產生，所以「今天」及「未來」的 Parquet 不存在。
            # 若走到今日之後還沒找到 Parquet，代表今日就是連假後第一個開市日
            # = 實際結算日，直接以今日為候選。
            if candidate > trading_date:
                candidate = trading_date   # 回退到今日
                _found = True
                print(f"[Settlement] Candidate overshot today; "
                      f"settlement day is today ({trading_date}).")
                break

            # ── 邊界 2：候選日 == 今日 ────────────────────────────────────
            # 日盤正在進行中，當然找不到今日的 Parquet。
            # 既然從第三個禮拜三到今日之間找不到任何過去的 Parquet，
            # 就代表今日是連假後第一個開市日 = 結算日。
            if candidate == trading_date:
                _found = True
                print(f"[Settlement] Today ({trading_date}) is the first trading day "
                      f"after the holiday (day session still running, no Parquet yet). "
                      f"Treating today as settlement day.")
                break

            # ── 正常情況：查過去日期的 Parquet ─────────────────────────────
            if candidate.weekday() < 5 and _parquet_exists(candidate):
                _found = True
                break  # 找到了有 Parquet 的工作日
            candidate += _td(days=1)

        if not _found:
            # 20 天內找不到 → 資料問題，退回純演算法第三禮拜三
            print(f"[Settlement] ⚠️  Could not find any Parquet within {_MAX_SEARCH_DAYS} days "
                  f"from {third_wed}. Falling back to raw 3rd Wednesday.")
            candidate = third_wed

    except Exception:
        # Fallback：只跳過週末（無法讀取 Parquet 路徑時）
        candidate = third_wed
        while candidate.weekday() >= 5:  # 5=Sat, 6=Sun
            candidate += _td(days=1)

    candidate_str = candidate.strftime("%Y-%m-%d")
    is_settlement = (today_str == candidate_str)


    if is_settlement:
        print(f"[Settlement] Today {today_str} is algorithmically detected as settlement day "
              f"(3rd Wed of {y}/{m:02d} → adjusted to {candidate_str}).")
    else:
        print(f"[Settlement] Today {today_str} is NOT a settlement day "
              f"(expected: {candidate_str}).")

    return today_str, is_settlement


# --- [Core Logic] Live Delta Manager (全局內存池) ---
class LiveDeltaManager:
    """
    統一管理從 Kafka 讀取到的 Live Delta，
    讓抓取歷史缺口與之後背景執行緒收到新 Ticks 寫入此記憶體池。
    """
    def __init__(self):
        self.live_ticks: pl.DataFrame = pl.DataFrame()
        self.lock = threading.Lock()
        self.simulation_queue = []
        
    def initialize(self, gap_ticks: pl.DataFrame, simulate_flow: bool = False):
        print(f"[LiveDelta] Initializing memory cache with {len(gap_ticks)} ticks.")
        self.live_ticks = gap_ticks
        
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
                # [效能優化] 保留最近 26 小時的 tick
                # TXF 一實完整交易循環: 夜盤 15:00 至雔日日盤 13:45 共約 22h45m
                # 設 26h 淡保留 Parquet 尚未更新時所有透過 Kafka 止接的交易資料
                _MAX_LIVE_MS = 26 * 3_600_000
                _cutoff = self.live_ticks["ts"][-1] - _MAX_LIVE_MS
                self.live_ticks = self.live_ticks.filter(pl.col("ts") >= _cutoff)
                
    def get_ticks(self) -> pl.DataFrame:
        with self.lock:
            if self.live_ticks.is_empty():
                return self.live_ticks
            # [説明] 直接回傳完整的 live_ticks（已由 add_ticks 統一守門 26h 上限）
            # 不做二次截斷，避免夜盤資料在凌晨被誤刪
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

# --- [效能優化] EMA/SMA 增量更新函數 (O(num_periods)，不重算全量歷史) ---
def _incremental_ma_update(cached_df: "pl.DataFrame", new_close: float, tf: str, is_combined: bool, is_new_bar: bool = False) -> dict:
    """
    以遞推公式計算最新一根 K 棒的 MA 值，避免每 0.5s 重跑 DataProcessor(N 行)。
    - EMA: ema_new = α × close_new + (1-α) × ema_prev
    - SMA: sma_new = sma_prev + (close_new - close_drop) / period
    收斂精度：初始全量計算後永遠 100% 正確（不因 tail(1500) 截斷而失準）。

    is_new_bar=True  → 正在附加新 K 棒：prev = cached[-1]，drop = cached[-period]
    is_new_bar=False → 正在更新當根 K 棒：prev = cached[-2]，drop = cached[-(period+1)]
    """
    from visualization.style_config import ColorScheme

    # 分開日夜盤的日線每天有兩根 K 棒，MA 週期需加倍
    ma_multiplier = 2 if (tf == '1d' and not is_combined) else 1
    n = len(cached_df)
    ma_vals = {}

    for period_d, cfg in ColorScheme.MA_SETTINGS.items():
        col = f"ma{period_d}"
        period = period_d * ma_multiplier
        ma_type = cfg.get('type', 'SMA')

        if col not in cached_df.columns or n < 1:
            ma_vals[col] = None
            continue

        # 根據是否為新 K 棒決定「前根」與「掉出窗口那根」的索引
        if is_new_bar:
            prev_idx = -1            # 快取最後一行 = 已完成的上一根
            drop_idx = -period       # 即將掉出 SMA 窗口的那根（距現在 period 根前）
        else:
            prev_idx = -2            # 倒數第二行 = 已完成的上一根（[-1] 是正在更新的當根）
            drop_idx = -(period + 1) # 距現在 period 根前（含當根佔位後移一位）

        if ma_type == 'EMA':
            alpha = 2.0 / (period + 1)
            if abs(prev_idx) <= n:
                prev_val = cached_df[col][prev_idx]
                ma_vals[col] = (alpha * new_close + (1.0 - alpha) * float(prev_val)) if prev_val is not None else None
            else:
                ma_vals[col] = None
        else:  # SMA
            prev_sma = cached_df[col][prev_idx] if abs(prev_idx) <= n else None
            drop_close = (
                cached_df['close'][drop_idx]
                if abs(drop_idx) <= n and 'close' in cached_df.columns
                else None
            )
            if prev_sma is not None and drop_close is not None:
                ma_vals[col] = float(prev_sma) + (new_close - float(drop_close)) / period
            else:
                ma_vals[col] = None

    return ma_vals

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
def start_live_kafka_listener(kafka_reader, delta_manager, viewer, symbol: str, combined: bool, simulate_flow: bool = False, is_simulating_history: bool = False, delta_manager_r2=None, main_topic: str = "txf-tick", r2_topic: str = "txfr2-tick"):
    import time
    
    while not getattr(viewer, '_chart_shown', False):
        time.sleep(0.1)
        
    print(f"[Kafka Live] Thread started. Background polling activated. Simulate Flow: {simulate_flow}, Instant History: {is_simulating_history and not simulate_flow}")
    last_update_time = 0
    
    def on_tick_cb(tick_data, topic=None):
        nonlocal last_update_time
        # 背景只要單純把 ticks 加入全域記憶體即可
        if tick_data is not None and topic is not None:
            if topic == main_topic:
                delta_manager.add_ticks([tick_data])
            elif topic == r2_topic and delta_manager_r2 is not None:
                delta_manager_r2.add_ticks([tick_data])
            
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
                    if "session" in last_row.columns:
                        live_bar["session"] = last_row["session"][0]
                        
                    if "TAIEX" in last_row.columns and last_row["TAIEX"][0] is not None:
                        live_bar["TAIEX"] = float(last_row["TAIEX"][0])
                        
                    if "basis" in last_row.columns and last_row["basis"][0] is not None:
                        live_bar["basis"] = float(last_row["basis"][0])
                        
                    if "r2_basis" in last_row.columns and last_row["r2_basis"][0] is not None:
                        live_bar["r2_basis"] = float(last_row["r2_basis"][0])
                        
                    if "calendar_spread" in last_row.columns and last_row["calendar_spread"][0] is not None:
                        live_bar["calendar_spread"] = float(last_row["calendar_spread"][0])
                        
                    if "vwap" in last_row.columns and last_row["vwap"][0] is not None:
                        live_bar["VWAP"] = float(last_row["vwap"][0])
                        
                    from visualization.style_config import ColorScheme as _CSy  # [D] 移到迴圈外，避免每 tick 重複 sys.modules 查找
                    for col in last_row.columns:
                        if col.startswith("ma"):
                            period = int(col.replace("ma", ""))
                            ma_type = _CSy.MA_SETTINGS[period].get('type', 'SMA')
                            label = f"{ma_type}{period}"
                            val = last_row[col][0]
                            live_bar[label] = float(val) if val is not None else None
                            
                    # 依照您的要求，1d 不再需要跟著 Tick 瘋狂跳動即時更新
                    if not viewer.timeframe.startswith('1d'):
                        viewer.update_live_bar(live_bar)
            except Exception as e:
                print(f"[LiveDelta] Warning during background UI update: {e}")
                
    sim_queue = getattr(delta_manager, 'simulation_queue', [])
    sim_idx = 0
                
    try:
        while getattr(viewer, '_chart_shown', False):
            if simulate_flow and sim_idx < len(sim_queue):
                batch = []
                # 全速重播：每次迴圈直接塞入 500 筆 Tick，不受現實時間限制
                while sim_idx < len(sim_queue) and len(batch) < 500:
                    batch.append(sim_queue[sim_idx])
                    sim_idx += 1
                    
                if batch:
                    # 批次餵進去，只呼叫最後一次 on_tick_cb(None) 觸發 UI 更新
                    delta_manager.add_ticks(batch)
                    on_tick_cb(None, None)
                else:
                    on_tick_cb(None, None)
            else:
                ticks_dict = kafka_reader.poll_new_ticks()
                
                if ticks_dict:
                    # 先把所有收到的資料放進記憶體
                    if main_topic in ticks_dict:
                        delta_manager.add_ticks(ticks_dict[main_topic])
                    if r2_topic in ticks_dict and delta_manager_r2:
                        delta_manager_r2.add_ticks(ticks_dict[r2_topic])
                        
                    # 有新 tick 才觸發 UI 更新
                    on_tick_cb(None, None)
                # [效能優化] 沒有新 tick 就不呼叫 on_tick_cb，避免每 50ms 觸發無意義的全量重算
            
            time.sleep(0.1)  # [效能優化] 從 50ms 調整為 100ms，降低 CPU 輪詢壓力
    except Exception as e:
        print(f"[Kafka Live] Thread exception: {e}")
    finally:
        print("[Kafka] Closing Reader...")
        kafka_reader.close()

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
    parser.add_argument('--smart-rollover', action='store_true', help="結算日自動切換為主圖顯示次月合約(R2)")
    parser.add_argument('--no-tse', action='store_true', help="不載入 TSE 對照線")
    parser.add_argument('--tfs', type=str, nargs='+', default=['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1d (comb)'], help="圖表週期選項")
    parser.add_argument('--max-bars', type=int, default=20000, help="最多載入的 K 棒數量")
    
    # Kafka Live 參數
    parser.add_argument('--live', action='store_true', help="啟用 Kafka 實時看盤系統")
    parser.add_argument('--kafka-broker', type=str, default='192.168.1.50:9092', help="Kafka Broker 地址")
    parser.add_argument('--kafka-topic', type=str, default='txf-tick', help="Kafka 訂閱主題")
    parser.add_argument('--simulate-cut-date', type=str, default=None, help="模擬 Parquet 只載入到此日期為止（例如 2026-06-11）")
    parser.add_argument('--progressive', action='store_true', help="在歷史模擬模式下開啟漸進式重播 (預設為瞬間載入)")
    args = parser.parse_args()

    if args.end_date is None: args.end_date = datetime.now().strftime('%Y-%m-%d')
    if args.date is None: args.date = "2000-01-01"
    
    print(f"[Task] {args.symbol} {args.tf} | {args.date} ~ {args.end_date} {'[Adjusted]' if args.adjust else '[Raw]'}")

    # ── 啟動時立即偵測今日是否為結算日（三層確認）──────────────────────────
    # 目的：讓快速背景路徑的 Smart Rollover 能在開盤就生效，
    # 不需等到完整計算路徑（get_data）載入歷史 Parquet 後才建立 GLOBAL_SETTLEMENT_DATES。
    if getattr(args, 'smart_rollover', False) and args.symbol == 'TXF':
        global GLOBAL_SETTLEMENT_DATES
        _today_str, _is_settlement_today = _detect_today_settlement_date()
        if _is_settlement_today:
            GLOBAL_SETTLEMENT_DATES = {_today_str}  # 先放今天；完整路徑跑完後會擴充歷史

    # 快取字典 (僅存最原始的 Historical Parquet DataFrame，不存合併後的結果)
    cache_raw = {}
    cache_tse_raw = {}
    cache_r2_raw = {}
    cache_proc = {}          # {orig_tf: pl.DataFrame} – 快取完整處理結果，供增量背景更新使用
    _ext_last_update = {}    # {orig_tf: float} – 上次完整外部資料 Join 的 Unix 時間戳
    actual_max_date = None

    # 初始化 LiveDeltaManager
    delta_manager_instance = None
    delta_manager_instance_r2 = None
    kafka_reader_instance = None
    kafka_reader_instance_r2 = None

    if args.live:
        from core.kafka_reader import KafkaTickReader
        import uuid
        try:
            unique_group = f"txf_chart_live_{uuid.uuid4().hex[:8]}"
            kafka_reader_instance = KafkaTickReader(broker_url=args.kafka_broker, topics=[args.kafka_topic, "txfr2-tick"], group_id=unique_group)
            delta_manager_instance = LiveDeltaManager()
            delta_manager_instance_r2 = LiveDeltaManager()
            
            # --- 尋找最近一根 K 棒的真實時間 ---
            # 如果是模擬模式，需要讀取歷史資料的最後一根。否則直接從實體路徑找最新檔案
            last_dt = None
            if args.simulate_cut_date:
                # 模擬模式下：從模擬日往前推 15 天找最後一根 K 棒
                sim_dt = datetime.strptime(args.simulate_cut_date, "%Y-%m-%d")
                recent_dt = (sim_dt - timedelta(days=15)).strftime('%Y-%m-%d')
                df_recent = load_historical_raw(args.symbol, '1m', recent_dt, args.end_date, False, max_bars=5000, simulate_cut_date=args.simulate_cut_date)
                if not df_recent.is_empty() and "ts" in df_recent.columns:
                    last_dt = df_recent["ts"][-1]
            else:
                # 真實 Live 模式下：直接掃描實體路徑找最新檔案 (Watermarking 免疫過年長假)
                from core.loader import DataLoader
                last_dt = DataLoader.get_latest_record_time(args.symbol, '1m')

            # 預設 fallback
            fallback_dt = datetime.strptime(args.simulate_cut_date, "%Y-%m-%d") if args.simulate_cut_date else datetime.now()
            base_ts_ms = int((fallback_dt - timedelta(days=1)).timestamp() * 1000)

            if last_dt is not None:
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

            simulate_flow = bool(args.simulate_cut_date) and args.progressive
            
            # --- 初始化兩個 Manager (透過單一 Reader 抓取所有 Gap Ticks) ---
            gap_history_dict = kafka_reader_instance.fetch_gap_ticks(base_ts_ms)
            
            main_topic = args.kafka_topic
            r2_topic = "txfr2-tick"
            
            delta_manager_instance.initialize(gap_history_dict.get(main_topic, pl.DataFrame()), simulate_flow)
            delta_manager_instance_r2.initialize(gap_history_dict.get(r2_topic, pl.DataFrame()), simulate_flow)
                            
        except Exception as e:
            print(f"[Kafka Warning] Could not connect or initialize: {e}")
            args.live = False

    # 核心資料獲取閉包 (Closure)
    def get_data(tf: str, background_update=False):
        nonlocal actual_max_date
        global GLOBAL_SETTLEMENT_DATES  # 允許閉包內修改模組層級全域快取
        
        orig_tf = tf
        is_combined = args.combined
        if tf.endswith(" (comb)"):
            is_combined = True
            tf = tf.replace(" (comb)", "").strip()

        # ==================================================================
        # [效能優化 A+B] 快速背景路徑：增量更新最後一根 K 棒
        # - O(num_periods) 遞推 MA，跳過每 0.5s 的全量 DataProcessor(1500 行)
        # - EMA3300 從完整歷史正確初始化後遞推，不受 tail(1500) 截斷（≈40% 誤差）影響
        # ==================================================================
        if background_update and orig_tf in cache_proc and not cache_proc[orig_tf].is_empty():
            cached = cache_proc[orig_tf]

            # 1d 或無 Live Feed → 直接回傳快取
            if delta_manager_instance is None or tf == '1d':
                return cached

            # 取得最新 K 棒狀態（perform_delta_merge 才能給出正確的 OHLCV + Volume）
            ticks_df = delta_manager_instance.get_ticks()
            if ticks_df.is_empty():
                return cached

            df_raw_live = perform_delta_merge(
                cache_raw.get(orig_tf, pl.DataFrame()), ticks_df, tf, args.symbol, is_combined
            )
            if df_raw_live.is_empty():
                return cached

            last_raw = df_raw_live.tail(1)
            new_close = float(last_raw["close"][0])
            new_ts    = last_raw["ts"][0]

            # ==================================================================
            # [Smart Rollover] 快速路徑的結算日換倉
            # 完整路徑（初始載入）已做 full join 換成 R2，但增量路徑的 new_close
            # 來自 TXF R1 Kafka ticks，若今天是結算日必須手動換成 R2。
            # ==================================================================
            if (getattr(args, 'smart_rollover', False)
                    and args.symbol == 'TXF'
                    and GLOBAL_SETTLEMENT_DATES
                    and delta_manager_instance_r2 is not None):
                # 以 ts + 9h 對齊期交所「交易日」（與完整路徑使用相同邏輯）
                import datetime as _dt
                _trading_dt = (new_ts + _dt.timedelta(hours=9)).date()
                _trading_date_str = _trading_dt.strftime("%Y-%m-%d")

                if _trading_date_str in GLOBAL_SETTLEMENT_DATES:
                    r2_ticks = delta_manager_instance_r2.get_ticks()
                    if not r2_ticks.is_empty() and "close" in r2_ticks.columns:
                        _r2_close = r2_ticks["close"][-1]
                        if _r2_close is not None:
                            # 用 R2 最新 close 取代 R1（high/low 精度由 full path 負責）
                            new_close = float(_r2_close)




            # ==================================================================
            # [B] 同根 K 棒狀態判斷：不能只看收盤價，必須兼顧高、低、量 (Order Flow 靈魂)
            # ==================================================================
            if "ts" in cached.columns and "close" in cached.columns and len(cached) > 0:
                _cc = cached["close"][-1]
                _hh = cached["high"][-1] if "high" in cached.columns else _cc
                _ll = cached["low"][-1] if "low" in cached.columns else _cc
                _vv = cached["volume"][-1] if "volume" in cached.columns else 0
                
                new_high = float(last_raw["high"][0])
                new_low = float(last_raw["low"][0])
                new_vol = float(last_raw["volume"][0])

                # 若 高、低、收、量 完全一致，才代表市場真的沒有新資訊，此時跳過才能達到真優化
                if (cached["ts"][-1] == new_ts
                        and _cc is not None
                        and abs(float(_cc) - new_close) < 0.001
                        and abs(float(_hh) - new_high) < 0.001
                        and abs(float(_ll) - new_low) < 0.001
                        and abs(float(_vv) - new_vol) < 0.001):
                    return cached

            # 判斷是否為新 K 棒
            is_new_bar = (
                "ts" not in cached.columns
                or len(cached) == 0
                or cached["ts"][-1] != new_ts
            )

            # 遞推計算 MA（使用完整快取歷史，精度等同全量計算）
            ma_vals = _incremental_ma_update(cached, new_close, tf, is_combined, is_new_bar)

            # 計算顏色
            new_open = float(last_raw["open"][0])
            session  = str(last_raw["session"][0]) if "session" in last_raw.columns else "Day"
            is_up    = new_close >= new_open
            from visualization.style_config import ColorScheme as _CS
            _color     = _CS.get_color(is_up, session)
            _vol_color = _CS.get_volume_color(is_up, session)

            # 以快取最後一列為模板（攜帶 TAIEX、TXFR2、vwap 等未明確更新的欄位）
            # 【關鍵修復】：使用 .cast(cached.schema["欄位"]) 動態對齊型別，避免 Int64 與 Float64 衝突
            new_row = cached.tail(1).with_columns([
                pl.lit(new_open).cast(cached.schema["open"]).alias("open"),
                pl.lit(float(last_raw["high"][0])).cast(cached.schema["high"]).alias("high"),
                pl.lit(float(last_raw["low"][0])).cast(cached.schema["low"]).alias("low"),
                pl.lit(new_close).cast(cached.schema["close"]).alias("close"),
                pl.lit(last_raw["volume"][0]).cast(cached.schema["volume"]).alias("volume"),
                pl.lit(_color).alias("color"),
                pl.lit(_color).alias("borderColor"),
                pl.lit(_color).alias("wickColor"),
                pl.lit(_vol_color).alias("vol_color"),
                pl.lit(is_up).alias("is_up"),
                pl.lit(session).alias("session"),
            ])

            # 更新 ts（確保型別一致）
            if "ts" in cached.columns:
                new_row = new_row.with_columns(
                    pl.Series("ts", [new_ts]).cast(cached.schema["ts"])
                )

            # 重新生成 time 欄位（str → Datetime("ns")，與 lightweight-charts 相容）
            if "ts" in new_row.columns and "time" in cached.columns:
                new_row = new_row.with_columns(
                    pl.col("ts").dt.strftime('%Y-%m-%d %H:%M:%S').alias("time")
                ).with_columns(
                    pl.col("time").str.to_datetime(format="%Y-%m-%d %H:%M:%S").cast(pl.Datetime("ns"))
                )

            # 更新 date 欄位
            if "date" in cached.columns and "ts" in new_row.columns:
                new_row = new_row.with_columns(pl.col("ts").dt.date().alias("date"))

            # 套用增量 MA 數值
            _ma_exprs = [
                pl.lit(float(v)).cast(cached.schema[c]).alias(c)
                for c, v in ma_vals.items()
                if c in cached.columns and v is not None
            ]
            if _ma_exprs:
                new_row = new_row.with_columns(_ma_exprs)

            # ==================================================================
            # [C] 同步更新外部指標 (TAIEX 大盤與 TXFR2 次月)
            # 解決圖表指標變成「水平死直線」的問題
            # ==================================================================
            
            # 從實時 Tick 池抓取最新的 TAIEX 與 TXFR2
            live_taiex = ticks_df["underlying_price"][-1] if "underlying_price" in ticks_df.columns else None
            
            live_txfr2 = None
            if delta_manager_instance_r2 is not None:
                r2_ticks = delta_manager_instance_r2.get_ticks()
                if not r2_ticks.is_empty() and "close" in r2_ticks.columns:
                    live_txfr2 = r2_ticks["close"][-1]

            # 1. 更新 TAIEX (大盤) 與正逆價差 (basis)
            if "TAIEX" in cached.columns:
                # 如果有收到新的實時大盤點位就用新的，否則沿用上一筆
                _taiex_val = live_taiex if live_taiex is not None else cached["TAIEX"][-1]
                if _taiex_val is not None:
                    new_row = new_row.with_columns([
                        pl.lit(float(_taiex_val)).cast(cached.schema["TAIEX"]).alias("TAIEX"),
                        pl.lit(new_close - float(_taiex_val)).alias("basis")
                    ])

            # 2. 保留原 R1 Close (供跨月轉倉計算使用)
            if "r1_close_original" in cached.columns:
                _r1_schema = cached.schema["r1_close_original"] if "r1_close_original" in cached.schema else pl.Float64
                new_row = new_row.with_columns(
                    pl.lit(new_close).cast(_r1_schema).alias("r1_close_original")
                )

            # 3. 更新 TXFR2 (次月) 與相關價差
            if "TXFR2" in cached.columns:
                _txfr2_val = live_txfr2 if live_txfr2 is not None else cached["TXFR2"][-1]
                if _txfr2_val is not None:
                    new_row = new_row.with_columns(
                        pl.lit(float(_txfr2_val)).cast(cached.schema["TXFR2"]).alias("TXFR2")
                    )
                    
                    # 更新跨月價差 (Calendar Spread)
                    if "calendar_spread" in cached.columns:
                        _r1_col = "r1_close_original"
                        _r1_val = (
                            float(cached[_r1_col][-1])
                            if _r1_col in cached.columns and cached[_r1_col][-1] is not None
                            else new_close
                        )
                        new_row = new_row.with_columns(
                            pl.lit(float(_txfr2_val) - _r1_val).alias("calendar_spread")
                        )
                    
                    # 更新次月逆價差 (r2_basis)
                    if "r2_basis" in cached.columns and "TAIEX" in new_row.columns:
                        _current_taiex = new_row["TAIEX"][0]
                        if _current_taiex is not None:
                            new_row = new_row.with_columns(
                                pl.lit(float(_txfr2_val) - float(_current_taiex)).alias("r2_basis")
                            )

            # 拼接：新 K 棒 → append；更新當根 → 替換最後一列
            if is_new_bar:
                updated = cached.vstack(new_row)
            else:
                updated = cached.head(len(cached) - 1).vstack(new_row)

            if len(updated) > args.max_bars:
                updated = updated.tail(args.max_bars)

            cache_proc[orig_tf] = updated
            return updated

        # ==================================================================
        # 完整計算路徑（初始載入 / 切換週期 / 快取尚未建立）
        # ==================================================================

        # 1. 取得歷史資料 (Parquet)
        # [優化] 使用 ThreadPoolExecutor 並行讀取三個商品歷史資料，大幅縮短啟動時間
        import concurrent.futures
        
        needs_main = orig_tf not in cache_raw
        needs_tse = orig_tf not in cache_tse_raw
        needs_r2 = orig_tf not in cache_r2_raw
        
        if needs_main or needs_tse or needs_r2:
            if not background_update: 
                print(f"[Process] Parallel loading historical Parquet data for {orig_tf}...")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_main = executor.submit(load_historical_raw, args.symbol, tf, args.date, args.end_date, is_combined, max_bars=args.max_bars, actual_max_date=actual_max_date, simulate_cut_date=args.simulate_cut_date) if needs_main else None
                future_tse = executor.submit(load_historical_raw, 'TSE', tf, args.date, args.end_date, is_combined, max_bars=args.max_bars, simulate_cut_date=args.simulate_cut_date) if needs_tse else None
                future_r2 = executor.submit(load_historical_raw, 'TXFR2', tf, args.date, args.end_date, is_combined, max_bars=args.max_bars, simulate_cut_date=args.simulate_cut_date) if needs_r2 else None
                
                if future_main: cache_raw[orig_tf] = future_main.result()
                if future_tse: cache_tse_raw[orig_tf] = future_tse.result()
                if future_r2: cache_r2_raw[orig_tf] = future_r2.result()
            
            if needs_main and actual_max_date is None and not cache_raw[orig_tf].is_empty():
                if "time" in cache_raw[orig_tf].columns:
                    actual_max_date = str(cache_raw[orig_tf]["time"][-1]).split(" ")[0]
                elif "date" in cache_raw[orig_tf].columns:
                    actual_max_date = str(cache_raw[orig_tf]["date"][-1]).split(" ")[0]

        df_hist = cache_raw[orig_tf]
        
        # 2. 動態拼接 Live Delta
        if delta_manager_instance is not None and tf != '1d':
            ticks_df = delta_manager_instance.get_ticks()
            df_raw = perform_delta_merge(df_hist, ticks_df, tf, args.symbol, is_combined)
        else:
            df_raw = df_hist
            
        if df_raw.is_empty():
            return pl.DataFrame()
            
        # [NEW] [效能優化] 如果是背景即時跳動，提早裁切 1500 根，避免後續繁重的轉倉拼接與 Full Join 處理 2 萬筆資料
        if background_update and len(df_raw) > 1500:
            df_raw = df_raw.tail(1500)

        # ---------------------------------------------------------
        # [NEW] 智慧轉倉 (Smart Rollover) - 將結算日當天的 K 棒強制換成 TXFR2
        # ---------------------------------------------------------
        if getattr(args, 'smart_rollover', False) and args.symbol == 'TXF' and orig_tf in cache_r2_raw and not df_raw.is_empty():
            import pandas as pd
            from datetime import date, timedelta

            # 若為初次載入 (background_update=False)，計算並快取全域結算日
            if GLOBAL_SETTLEMENT_DATES is None and not background_update:
                # 1. 統一使用 ts 平移 9 小時來完美對齊「期交所交易日 (Trading Date)」
                time_col = "ts" if "ts" in cache_raw[orig_tf].columns else "date"
                if time_col == "ts":
                    dates_expr = pl.col(time_col).dt.offset_by("9h").dt.date().cast(str)
                else:
                    dates_expr = pl.col(time_col).cast(str)
                    
                dates_s = cache_raw[orig_tf].select(dates_expr).to_series().unique().to_list()
                dates_s = [d for d in dates_s if d is not None]
                
                # 2. 演算法推算第三個禮拜三的精準結算日 (避開假日)
                year_months = set([d[:7] for d in dates_s])
                algorithmic_settlements = set()
                for ym in year_months:
                    y, m = int(ym[:4]), int(ym[5:7])
                    first_day = date(y, m, 1)
                    first_wed_offset = (2 - first_day.weekday()) % 7
                    third_wed = first_day + timedelta(days=first_wed_offset + 14)
                    third_wed_str = third_wed.strftime("%Y-%m-%d")
                    
                    # 歷史資料中 >= 第三個禮拜三 的第一個交易日
                    valid_dates = [d for d in dates_s if d >= third_wed_str]
                    if valid_dates:
                        algorithmic_settlements.add(sorted(valid_dates)[0])
                        
                # 3. 讀取 CSV 補足歷史特例 (提早/延後)
                try:
                    from config.settings import SETTLEMENT_CALENDAR_PATH
                    csv_dates = pd.read_csv(SETTLEMENT_CALENDAR_PATH)['date'].tolist()
                    algorithmic_settlements.update(csv_dates)
                except:
                    pass
                    
                GLOBAL_SETTLEMENT_DATES = algorithmic_settlements

            algorithmic_settlements = GLOBAL_SETTLEMENT_DATES if GLOBAL_SETTLEMENT_DATES is not None else set()
                
            # 4. 針對「結算日」進行偷天換日 (Data Splicing)
            # 將 TXF 的資料全部替換為 TXFR2
            ext_raw = cache_r2_raw[orig_tf]
            
            # 先確保 ext_raw 也有最新的 Live Ticks (如果是看實時盤)
            if delta_manager_instance_r2 is not None and tf != '1d':
                ext_raw = perform_delta_merge(ext_raw, delta_manager_instance_r2.get_ticks(), tf, 'TXFR2', is_combined)

            # [效能優化] ext_raw 也必須提早裁切，降低 full join 的負擔
            if background_update and not ext_raw.is_empty() and len(ext_raw) > 1500:
                ext_raw = ext_raw.tail(1500)
                
            if not ext_raw.is_empty():
                if tf == '1d':
                    join_cols = ["date"] if is_combined else ["date", "session"]
                else:
                    join_cols = ["ts"]
                
                # 保留 R1 原始 Close，供後續跨月轉倉計算使用
                df_raw = df_raw.with_columns(pl.col("close").alias("r1_close_original"))
                
                # 取得 ext_raw 的 OHLCV 與 true_pv_sum
                replace_cols = [c for c in ["open", "high", "low", "close", "volume", "true_pv_sum"] if c in ext_raw.columns]
                rename_map = {c: f"r2_{c}" for c in replace_cols}
                ext_join = ext_raw.select(join_cols + replace_cols).rename(rename_map).unique(subset=join_cols, keep="last")
                
                # 進行 Join (使用 full join 確保 R2 在 13:30 ~ 13:45 的 K 棒不會被拋棄)
                df_raw = df_raw.join(ext_join, on=join_cols, how="full", coalesce=True)
                
                # 若因為 full join 產生了新的一列，原本 df_raw 才有的欄位（如 symbol, underlying_close 等）會變成 null
                # 但這不影響，因為我們只在結算日當天的 13:30~13:45 產生 null，而這段時間大盤確實已經收盤了
                
                # 找出結算日的 rows (再次使用 ts 平移 9 小時完美對齊交易日)
                if "ts" in df_raw.columns:
                    trading_date_expr = pl.col("ts").dt.offset_by("9h").dt.date().cast(str)
                else:
                    trading_date_expr = pl.col("date").cast(str)
                    
                is_settlement = trading_date_expr.is_in(list(algorithmic_settlements))
                
                # 條件替換：如果是結算日且有 R2 資料，就替換為 R2 的資料，否則維持原樣
                replace_exprs = []
                for c in replace_cols:
                    r2_c = f"r2_{c}"
                    expr = pl.when(is_settlement & pl.col(r2_c).is_not_null()).then(pl.col(r2_c)).otherwise(pl.col(c)).alias(c)
                    replace_exprs.append(expr)
                    
                df_raw = df_raw.with_columns(replace_exprs)
                
                # 清除因為 full join 帶入但在非結算日無效的 R2 獨有列 (這些列的 close 會是 null)
                df_raw = df_raw.filter(pl.col("close").is_not_null())
                
                # [CRITICAL FIX] Polars 的 full join 會完全打亂資料的排序
                # 必須在此刻重新排序，否則後續 TAIEX 的 fill_null(forward) 會將 2020 年的數據填到 2026 年！
                sort_col = "ts" if "ts" in df_raw.columns else "date"
                df_raw = df_raw.sort(sort_col)
                
        # ---------------------------------------------------------
            
        # 3. 套用價格校正 —— **2026-07-21 退役**。
        # 理由:回溯調整(把換月價差累加進歷史價格)產生的是「不曾存在過的價格」,
        # 對「平舊倉/開新倉、價差計入成本」的實際轉倉是錯的模型;且此路徑依賴的
        # txf_adjustment_table_final.csv 已由 adjustments/roll_events.csv 取代
        # (只記事件、不改價格)。函式 apply_adjustment() 保留備查,旗標保留但不再有作用。
        if args.adjust:
            print("⚠️ --adjust 已退役(回溯調整不適用轉倉型策略);"
                  "換月價差請讀 D:/txf-data/adjustments/roll_events.csv。")
            
        # 4. 指標與顏色處理
        df_proc = DataProcessor.process_data(df_raw, tf, is_combined)
        
        # --- 封裝外部商品載入與合併邏輯 ---
        def _fetch_and_join_external(ext_symbol, cache_dict, target_col, dm_instance=None):
            nonlocal df_proc
            if orig_tf not in cache_dict:
                cache_dict[orig_tf] = load_historical_raw(ext_symbol, tf, args.date, args.end_date, is_combined, max_bars=args.max_bars, simulate_cut_date=args.simulate_cut_date)
            ext_hist = cache_dict[orig_tf]
            
            # 從指定的 delta_manager 取得即時大盤/次月指數
            if dm_instance is not None:
                ticks_df = dm_instance.get_ticks()
                if "underlying_price" in ticks_df.columns and ext_symbol == 'TSE':
                    ext_ticks = ticks_df.select([pl.col("ts"), pl.col("underlying_price").alias("close"), pl.col("volume")])
                    ext_raw = perform_delta_merge(ext_hist, ext_ticks, tf, ext_symbol, is_combined)
                elif "close" in ticks_df.columns:
                    ext_ticks = ticks_df.select([pl.col("ts"), pl.col("close"), pl.col("volume")])
                    ext_raw = perform_delta_merge(ext_hist, ext_ticks, tf, ext_symbol, is_combined)
                else:
                    ext_raw = ext_hist
            else:
                ext_raw = ext_hist
                
            if background_update and len(ext_raw) > 1500:
                ext_raw = ext_raw.tail(1500)
                
            ext_proc = DataProcessor.process_data(ext_raw, tf, is_combined)
            
            if not ext_proc.is_empty():
                if tf == '1d':
                    join_cols = ["date"] if is_combined else ["date", "session"]
                else:
                    join_cols = ["time"]
                    
                if all(c in ext_proc.columns for c in join_cols):
                    ext_join = ext_proc.select(join_cols + [pl.col("close").alias(target_col)]).unique(subset=join_cols, keep="last")
                    df_proc = df_proc.join(ext_join, on=join_cols, how="left")
                    
                if target_col in df_proc.columns:
                    df_proc = df_proc.with_columns(pl.col(target_col).fill_null(strategy="forward").fill_null(strategy="backward"))

        # 5. TSE 對照線處理
        if not args.no_tse and args.symbol == 'TXF':
            _fetch_and_join_external('TSE', cache_tse_raw, "TAIEX", dm_instance=delta_manager_instance)
            if "TAIEX" in df_proc.columns and "close" in df_proc.columns:
                df_proc = df_proc.with_columns((pl.col("close") - pl.col("TAIEX")).alias("basis"))
            
        # 6. TXFR2 (次月) 處理
        if args.symbol == 'TXF':
            _fetch_and_join_external('TXFR2', cache_r2_raw, "TXFR2", dm_instance=delta_manager_instance_r2)
            if "TXFR2" in df_proc.columns:
                if "TAIEX" in df_proc.columns:
                    df_proc = df_proc.with_columns((pl.col("TXFR2") - pl.col("TAIEX")).alias("r2_basis"))
                
                # 若開啟 smart_rollover，原 close 已變成 R2，需使用 r1_close_original 計算跨月價差
                target_r1_col = "r1_close_original" if "r1_close_original" in df_proc.columns else "close"
                if target_r1_col in df_proc.columns:
                    df_proc = df_proc.with_columns((pl.col("TXFR2") - pl.col(target_r1_col)).alias("calendar_spread"))
                    
        # Ensure strict chronological order for lightweight-charts
        if "ts" in df_proc.columns:
            df_proc = df_proc.unique(subset=["ts"], keep="last").sort("ts")
            df_proc = df_proc.with_columns(pl.col("ts").dt.strftime('%Y-%m-%d %H:%M:%S').alias("time"))
        elif "date" in df_proc.columns:
            df_proc = df_proc.unique(subset=["date"], keep="last").sort("date")
            if "time" not in df_proc.columns:
                df_proc = df_proc.with_columns(pl.col("date").alias("time"))
        elif "time" in df_proc.columns:
            df_proc = df_proc.unique(subset=["time"], keep="last").sort("time")
            
        # 完美解決 lightweight-charts 的時間軸 Bug：
        # 將字串轉回時間物件，並強制指定為奈秒 ("ns")。
        # 徹底繞過 lightweight-charts-python 的 datetime64[us] 轉換 Bug：
        # 強制所有時間欄位轉型為 Datetime("ns")，讓 Pandas astype('int64') 計算出正確秒數
        if "time" in df_proc.columns:
            if tf == '1d' and is_combined:
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

        # [效能優化] 快取完整處理結果（含正確收斂的 EMA3300 等），供後續背景增量更新使用
        cache_proc[orig_tf] = df_proc
        _ext_last_update[orig_tf] = time.time()

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
        is_simulating_history = bool(args.simulate_cut_date)
        simulate_flow = is_simulating_history and args.progressive
        kafka_thread = threading.Thread(
            target=start_live_kafka_listener,
            args=(kafka_reader_instance, delta_manager_instance, viewer, args.symbol, args.combined, simulate_flow, is_simulating_history, delta_manager_instance_r2, args.kafka_topic, "txfr2-tick"),
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