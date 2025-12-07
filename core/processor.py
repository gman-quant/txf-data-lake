# core/processor.py
import polars as pl
from visualization.style_config import ColorScheme

class DataProcessor:
    """
    負責資料清洗、聚合運算與指標計算
    """
    @staticmethod
    def process_data(df: pl.DataFrame, timeframe: str, combine_sessions: bool) -> pl.DataFrame:
        if df.is_empty(): return df

        # 1. 日夜盤聚合 (日線專用)
        # 注意：聚合後會遺失 'session' 欄位，稍後需補回
        if timeframe == '1d' and combine_sessions:
            df = DataProcessor._aggregate_sessions(df)

        # 2. 準備基本顏色
        k_up_d, k_dn_d = ColorScheme.get_color(True, 'Day'), ColorScheme.get_color(False, 'Day')
        k_up_n, k_dn_n = ColorScheme.get_color(True, 'Night'), ColorScheme.get_color(False, 'Night')
        v_up_d, v_dn_d = ColorScheme.get_volume_color(True, 'Day'), ColorScheme.get_volume_color(False, 'Day')
        v_up_n, v_dn_n = ColorScheme.get_volume_color(True, 'Night'), ColorScheme.get_volume_color(False, 'Night')

        # 3. 定義 Polars 表達式
        time_fmt = "%Y-%m-%d" if (timeframe == '1d' and combine_sessions) else "%Y-%m-%d %H:%M:%S"
        
        # K棒與成交量顏色 (這時候 session 可能還不存在，所以這只是定義邏輯，稍後執行)
        kbar_expr = (
            pl.when(pl.col("is_up"))
            .then(pl.when(pl.col("session") == "Night").then(pl.lit(k_up_n)).otherwise(pl.lit(k_up_d)))
            .otherwise(pl.when(pl.col("session") == "Night").then(pl.lit(k_dn_n)).otherwise(pl.lit(k_dn_d)))
        )
        vol_expr = (
            pl.when(pl.col("is_up"))
            .then(pl.when(pl.col("session") == "Night").then(pl.lit(v_up_n)).otherwise(pl.lit(v_up_d)))
            .otherwise(pl.when(pl.col("session") == "Night").then(pl.lit(v_dn_n)).otherwise(pl.lit(v_dn_d)))
        )

        # 指標運算
        ma_multiplier = 2 if (timeframe == '1d' and not combine_sessions) else 1
        ma_days = [5, 10, 20, 60, 120, 240]
        ma_exprs = [pl.col("close").rolling_mean(d * ma_multiplier).alias(f"ma{d}") for d in ma_days]

        if timeframe == '1d':
            vwap_expr = pl.lit(None).alias("vwap")
        else:
            tp = (pl.col("high") + pl.col("low") + pl.col("close")) / 3
            pv = tp * pl.col("volume")
            
            # 🟢 [修正關鍵] 
            # 原本: .over("date") -> 導致日夜盤混在一起算
            # 修正: .over(["date", "session"]) -> 確保 08:45 和 15:00 換盤時，VWAP 會歸零重算
            
            vwap_expr = (
                (pv.cum_sum().over(["date", "session"])) / 
                (pl.col("volume").cum_sum().over(["date", "session"]))
            ).alias("vwap")

        # 4. 執行向量運算 (分段執行，確保欄位安全)

        # 🟢 [修正 1] 處理 session 缺失
        # 如果因為 combine_sessions 聚合導致 session 消失，補回 'Day'
        if "session" not in df.columns:
            df = df.with_columns(pl.lit("Day").alias("session"))

        # Step A: 產生基礎時間與狀態
        df = df.with_columns([
            pl.col("ts").dt.date().alias("date_temp"),
            pl.col("date" if timeframe == '1d' and combine_sessions else "ts").dt.strftime(time_fmt).alias("time"),
            pl.col("session").fill_null("Day"), # 這時候 session 一定存在了
            (pl.col("close") >= pl.col("open")).alias("is_up")
        ])

        # 🟢 [修正 2] 處理 date 缺失 (分時線)
        if "date" in df.columns:
            df = df.with_columns(pl.col("date").fill_null(pl.col("date_temp")))
        else:
            df = df.with_columns(pl.col("date_temp").alias("date"))

        # Step B: 計算顏色與指標 (這時候所有依賴欄位都齊全了)
        return (
            df.with_columns([
                kbar_expr.alias("color"),
                kbar_expr.alias("borderColor"),
                kbar_expr.alias("wickColor"),
                vol_expr.alias("vol_color"),
                vwap_expr,
                *ma_exprs
            ])
            .drop("date_temp")
        )

    @staticmethod
    def _aggregate_sessions(df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.with_columns(
                pl.when(pl.col("session") == "Night")
                .then(
                    pl.when(pl.col("date").dt.weekday() == 5).then(pl.col("date").dt.offset_by("3d"))
                    .otherwise(pl.col("date").dt.offset_by("1d"))
                )
                .otherwise(pl.col("date")).alias("trading_date")
            )
            .lazy().sort("ts").group_by("trading_date")
            .agg([
                pl.col("ts").first(), pl.col("open").first(), pl.col("high").max(),
                pl.col("low").min(), pl.col("close").last(), pl.col("volume").sum()
            ])
            .rename({"trading_date": "date"}).sort("date").collect()
        )