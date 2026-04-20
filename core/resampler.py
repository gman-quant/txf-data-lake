# core/resampler.py
import polars as pl
from datetime import time, timedelta
from config.calendar_rules import get_session_expression, DAY_START

# Session 的 aligned 時間上限 (平移後):
#   日盤: 08:45 ~ 13:45 → aligned 後為 00:00 ~ 05:00:00，上限 = 5 * 3600 秒
#   夜盤: 15:00 ~ 05:00 → aligned 後為 00:00 ~ 14:00:00，上限 = 14 * 3600 秒
_DAY_SESSION_LIMIT_SEC   = 5  * 3600   # 5 小時 (秒)
_NIGHT_SESSION_LIMIT_SEC = 14 * 3600   # 14 小時 (秒)


def _timeframe_to_seconds(timeframe: str) -> int:
    """將 Polars duration 字串轉換為秒數，例如 '1h' -> 3600, '30m' -> 1800"""
    if timeframe.endswith('h'):
        return int(timeframe[:-1]) * 3600
    elif timeframe.endswith('m'):
        return int(timeframe[:-1]) * 60
    elif timeframe.endswith('s'):
        return int(timeframe[:-1])
    return 3600  # fallback: 1h


def _snap_aligned_ts_to_session(q: pl.LazyFrame, timeframe: str) -> pl.LazyFrame:
    """
    將 aligned_ts 中「稍微超過 session 收盤上限」的資料點，
    強制拉回至最後一個合法 bucket 的起點，避免產生殘餘迷你 K-bar。

    原理：
        aligned_ts 平移後，日盤 session 對應 aligned [00:00, 05:00)，
        夜盤對應 aligned [00:00, 14:00)。若某筆資料的 aligned_ts 時間部分
        >= session 上限 (如 05:00:03)，代表它只是收盤那根 K-bar 多出幾秒的尾巴，
        應強制被併入最後一個合法 bucket，而非開新的一根 K-bar。

        做法：
          1. 計算出最後一個合法 bucket 的秒偏移：last_bucket_sec = floor((session_limit_sec - 1) / tf_sec) * tf_sec
          2. 若超界，把 aligned_ts 替換為「當日日期 + last_bucket_sec 的 duration」
    """
    tf_sec = _timeframe_to_seconds(timeframe)

    # 計算最後合法 bucket 起點 (秒偏移)，使用 Python int 在 schema build 期計算，不依賴 Polars 大整數乘法
    day_last_bucket_sec   = (((_DAY_SESSION_LIMIT_SEC   - 1) // tf_sec)) * tf_sec
    night_last_bucket_sec = (((_NIGHT_SESSION_LIMIT_SEC - 1) // tf_sec)) * tf_sec

    # 判斷 aligned_ts 是否超過 session 上限
    # 注意：dt.hour() 回傳 Int8/Int16，乘以 3600 後最大 23*3600=82800，超過 Int16 上限，必須先 cast 到 Int32
    aligned_sec = (
        pl.col("aligned_ts").dt.hour().cast(pl.Int32) * 3600
        + pl.col("aligned_ts").dt.minute().cast(pl.Int32) * 60
        + pl.col("aligned_ts").dt.second().cast(pl.Int32)
    )

    # 取整天基準 (00:00:00 of that day in aligned space)
    day_base_ts   = pl.col("aligned_ts").dt.truncate("1d")
    night_base_ts = pl.col("aligned_ts").dt.truncate("1d")

    snapped = (
        pl.when(
            (pl.col("session") == "Day") & (aligned_sec >= _DAY_SESSION_LIMIT_SEC)
        )
        .then(
            day_base_ts + pl.duration(seconds=day_last_bucket_sec)
        )
        .when(
            (pl.col("session") == "Night") & (aligned_sec >= _NIGHT_SESSION_LIMIT_SEC)
        )
        .then(
            night_base_ts + pl.duration(seconds=night_last_bucket_sec)
        )
        .otherwise(pl.col("aligned_ts"))
        .alias("aligned_ts")
    )

    return q.with_columns(snapped)

def resample_to_kbars(tick_df: pl.DataFrame, timeframe: str):
    
    # 1. 抓取 Symbol (修復 Bug)
    # 我們先在最前面抓出 symbol 的值，因為後面轉 Lazy 後比較難抓
    symbol_val = None
    if "symbol" in tick_df.columns:
        # 直接讀取第一列
        symbol_val = tick_df["symbol"][0]

    # 2. 建立 "Trading Date" (交易日)
    # 邏輯：如果是 00:00 ~ 05:00 之間的資料，日期要減 1 天 (歸到昨晚)
    # 這樣如 12/06 03:00 的夜盤，就會被標記為 12/05 的 Night
    q = tick_df.lazy().with_columns([
        get_session_expression("ts"),
        
        pl.when(pl.col("ts").dt.time() < DAY_START) # 只要是早上8點前
          .then(pl.col("ts").dt.offset_by("-1d"))  # 日期退一天
          .otherwise(pl.col("ts"))                 # 其他維持原樣
          .dt.date()                               # 取出日期部分
          .alias("date")
    ])

    # 3. 定義基礎數據聚合 (不含 ts)
    aggs = [
        pl.col("close").first().alias("open"),
        pl.col("close").max().alias("high"),
        pl.col("close").min().alias("low"),
        pl.col("close").last().alias("close"),
        pl.col("volume").sum().alias("volume")
    ]
    
    # TXF 特殊欄位
    if "underlying_price" in tick_df.columns:
        aggs.append(pl.col("underlying_price").last().alias("underlying_close"))

    # 4. 分流處理
    if timeframe == '1d':
        # [日線] 依據 (date, session) 分組
        # 補回 ts (取該時段第一筆)
        daily_aggs = [pl.col("ts").first().alias("ts")] + aggs
        
        q = (
            q.sort("ts")
            .group_by(["date", "session"]) 
            .agg(daily_aggs)
            .sort("ts")
        )
    else:
        # [分時線] 依據 ts 分組
        # 將時間平移，使得開盤時間對齊 00:00 (Day: 08:45, Night: 15:00) 以利 dynamic group_by 切齊
        q = q.with_columns(
            pl.when(pl.col("session") == "Day")
            .then(pl.col("ts").dt.offset_by("-8h45m"))
            .otherwise(pl.col("ts").dt.offset_by("-15h"))
            .alias("aligned_ts")
        )

        # 🔒 收盤 Snap：將稍微超出 session 收盤時間的資料點歸入最後一個合法 bucket
        q = _snap_aligned_ts_to_session(q, timeframe)

        q = (
            q.sort("aligned_ts")
            .group_by_dynamic(
                "aligned_ts", 
                every=timeframe, 
                closed="left", 
                label="left",
                group_by=["date", "session"]
            )
            .agg(aggs)
        )
        
        # 平移還原為原始時間
        q = q.with_columns(
            pl.when(pl.col("session") == "Day")
            .then(pl.col("aligned_ts").dt.offset_by("8h45m"))
            .otherwise(pl.col("aligned_ts").dt.offset_by("15h"))
            .alias("ts")
        ).drop("aligned_ts")

    # 5. 通用過濾
    q = q.filter(pl.col("volume") > 0)
    
    # 6. 補回 Symbol (使用我們在第1步抓到的值)
    if symbol_val is not None:
        q = q.with_columns(pl.lit(symbol_val).alias("symbol"))

    # 7. 最終欄位排序
    desired_order = [
        "symbol", "date", "ts", "session",
        "open", "high", "low", "close", "volume"
    ]
    
    current_cols = q.collect_schema().names()
    
    head_cols = [c for c in desired_order if c in current_cols]
    tail_cols = [c for c in current_cols if c not in head_cols]
    
    q = q.select(head_cols + tail_cols)
    
    return q.collect()


def resample_kbars(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
    """
    將低級別 K 棒 (例如 1m 或 1h) 加上動態重取樣為指定的目標週期 (例如 4h)
    """
    if df.is_empty():
        return df

    # 抓取 Symbol
    symbol_val = None
    if "symbol" in df.columns:
        symbol_val = df["symbol"][0]

    q = df.lazy()

    aggs = [
        pl.col("open").first().alias("open"),
        pl.col("high").max().alias("high"),
        pl.col("low").min().alias("low"),
        pl.col("close").last().alias("close"),
        pl.col("volume").sum().alias("volume")
    ]
    
    if "underlying_close" in df.columns:
        aggs.append(pl.col("underlying_close").last().alias("underlying_close"))

    # 將時間平移，使得開盤時間對齊 00:00 (以利 dynamic group_by 對準整點起始)
    q = q.with_columns(
        pl.when(pl.col("session") == "Day")
        .then(pl.col("ts").dt.offset_by("-8h45m"))
        .otherwise(pl.col("ts").dt.offset_by("-15h"))
        .alias("aligned_ts")
    )

    # 🔒 收盤 Snap：將稍微超出 session 收盤時間的資料點歸入最後一個合法 bucket
    q = _snap_aligned_ts_to_session(q, timeframe)

    # 動態聚合 (並使用 date, session 分組，不需再重新計算 date)
    q = (
        q.sort("aligned_ts")
        .group_by_dynamic(
            "aligned_ts", 
            every=timeframe, 
            closed="left", 
            label="left",
            group_by=["date", "session"]
        )
        .agg(aggs)
    )

    # 時間平移還原
    q = q.with_columns(
        pl.when(pl.col("session") == "Day")
        .then(pl.col("aligned_ts").dt.offset_by("8h45m"))
        .otherwise(pl.col("aligned_ts").dt.offset_by("15h"))
        .alias("ts")
    ).drop("aligned_ts")

    # 補回 Symbol
    if symbol_val is not None:
        q = q.with_columns(pl.lit(symbol_val).alias("symbol"))

    # 通用過濾與整理欄位
    q = q.filter(pl.col("volume") > 0)
    
    desired_order = [
        "symbol", "date", "ts", "session",
        "open", "high", "low", "close", "volume", "underlying_close"
    ]
    
    current_cols = q.collect_schema().names()
    head_cols = [c for c in desired_order if c in current_cols]
    tail_cols = [c for c in current_cols if c not in head_cols]
    
    q = q.select(head_cols + tail_cols)
    
    return q.collect()