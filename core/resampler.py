# core/resampler.py
import polars as pl
from datetime import time
from config.calendar_rules import get_session_expression, DAY_START

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
        q = (
            q.sort("ts")
            .group_by_dynamic(
                "ts", 
                every=timeframe, 
                closed="left", 
                label="left"
            )
            .agg(aggs)
        )

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
        pl.col("volume").sum().alias("volume"),
        pl.col("session").first().alias("session")
    ]
    
    # 動態聚合
    q = (
        q.sort("ts")
        .group_by_dynamic(
            "ts", 
            every=timeframe, 
            closed="left", 
            label="left"
        )
        .agg(aggs)
    )

    # 重新計算 Date (Session 已經在 agg 中取得第一根基底 K 棒的狀態)
    q = q.with_columns([
        pl.when(pl.col("ts").dt.time() < DAY_START)
          .then(pl.col("ts").dt.offset_by("-1d"))
          .otherwise(pl.col("ts"))
          .dt.date()
          .alias("date")
    ])

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