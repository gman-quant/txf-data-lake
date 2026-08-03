# core/resampler.py
#
# 本檔只做一件事:**逐筆 tick → K 棒**(`resample_to_kbars`),供 `main_etl.py`(每日 ETL)
# 與 `fix_kbars.py`(從 raw 重建)使用。湖裡六個 TF 各自從逐筆獨立產出,不是層層聚上去的。
#
# ⚠️ 這裡**刻意沒有** K棒→K棒 的 `resample_kbars`。它是 2026-07-21 那次精簡(`8301fe4`,
#    舊看盤搬去 platform、`core/loader.py` 被刪)漏掉的殘渣 —— 唯一的呼叫端隨 loader 一起
#    消失,函式本身留了下來,於 2026-08-03 補刪。
#    要把細 TF 聚成粗 TF(例如加新 TF 時回填),兩條正規路徑:
#      ① `fix_kbars.py` —— 從 raw ticks 重算,慢但與每日 ETL 同一條碼。
#      ② 去 txf-quant-platform 跑 —— 那邊有完整的 TF 註冊表(TF_DEFS)與 3seg/週月線分支,
#         而且讀寫的是同一個湖。
#    **不要在這個 repo 裡重新長出一個 K棒→K棒 的函式**:2026-07-31 就是這樣壞掉的
#    (詳見 platform wiki `Time-Semantics` ⑥)。
import polars as pl
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
            day_base_ts + pl.duration(seconds=day_last_bucket_sec + tf_sec) - pl.duration(microseconds=1)
        )
        .when(
            (pl.col("session") == "Night") & (aligned_sec >= _NIGHT_SESSION_LIMIT_SEC)
        )
        .then(
            night_base_ts + pl.duration(seconds=night_last_bucket_sec + tf_sec) - pl.duration(microseconds=1)
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
    q = tick_df.lazy().sort("ts").with_columns([
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
        pl.col("volume").sum().alias("volume"),
        (pl.col("close") * pl.col("volume")).sum().alias("true_pv_sum")
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

    # 5b. 交易日永不為週末 —— 丟掉 date=週六/週日 的列。
    #     蓋掉三類髒資料:TAIFEX 假日測試盤(date=週六、量個位數)、週日幻影 tick、週一凌晨異常列。
    #     ⚠️ 必須用 date(交易日)判斷,不可用 ts:週五夜盤尾的 ts 落在「週六凌晨 00:00–05:00」,
    #        但其 date=週五(已 -1d 歸檔)→ 用 date 判斷會正確保留;用 ts 判斷會誤砍真夜盤。
    #     polars weekday: 週一=1 … 週五=5, 週六=6, 週日=7 → < 6 即保留週一~週五。
    q = q.filter(pl.col("date").dt.weekday() < 6)

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
