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
        if timeframe == '1d' and combine_sessions:
            df = DataProcessor._aggregate_sessions(df)

        # 2. 準備基本顏色
        k_up_d, k_dn_d = ColorScheme.get_color(True, 'Day'), ColorScheme.get_color(False, 'Day')
        k_up_n, k_dn_n = ColorScheme.get_color(True, 'Night'), ColorScheme.get_color(False, 'Night')
        v_up_d, v_dn_d = ColorScheme.get_volume_color(True, 'Day'), ColorScheme.get_volume_color(False, 'Day')
        v_up_n, v_dn_n = ColorScheme.get_volume_color(True, 'Night'), ColorScheme.get_volume_color(False, 'Night')

        # 3. 定義顏色與MA表達式
        time_fmt = "%Y-%m-%d" if (timeframe == '1d' and combine_sessions) else "%Y-%m-%d %H:%M:%S"
        
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

        ma_multiplier = 2 if (timeframe == '1d' and not combine_sessions) else 1
        ma_days = [5, 10, 20, 60, 120, 240]
        ma_exprs = [pl.col("close").rolling_mean(d * ma_multiplier).alias(f"ma{d}") for d in ma_days]

        # 4. 執行向量運算 (分段執行，確保欄位依序產生)

        # Step A: 防呆補強 (處理聚合後 session 遺失 & 分時線 date 遺失)
        if "session" not in df.columns:
            df = df.with_columns(pl.lit("Day").alias("session"))

        df = df.with_columns([
            pl.col("ts").dt.date().alias("date_temp"),
            pl.col("date" if timeframe == '1d' and combine_sessions else "ts").dt.strftime(time_fmt).alias("time"),
            pl.col("session").fill_null("Day"),
            (pl.col("close") >= pl.col("open")).alias("is_up")
        ])

        if "date" in df.columns:
            df = df.with_columns(pl.col("date").fill_null(pl.col("date_temp")))
        else:
            df = df.with_columns(pl.col("date_temp").alias("date"))

        # Step B: 🟢 [關鍵修正] 建立 VWAP 專用分組日期 (解決跨午夜斷裂)
        # 邏輯: 若是夜盤且時間 < 08:00 (代表過了午夜)，歸類到「前一天」的夜盤群組
        df = df.with_columns(
            pl.when((pl.col("session") == "Night") & (pl.col("ts").dt.hour() < 8))
            .then(pl.col("date").dt.offset_by("-1d"))
            .otherwise(pl.col("date"))
            .alias("vwap_group_date")
        )

        # Step C: 定義 VWAP 表達式 (使用 vwap_group_date)
        if timeframe == '1d':
            vwap_expr = pl.lit(None).alias("vwap")
        else:
            tp = (pl.col("high") + pl.col("low") + pl.col("close")) / 3
            pv = tp * pl.col("volume")
            
            # 使用 vwap_group_date 進行分組
            vwap_expr = (
                (pv.cum_sum().over(["vwap_group_date", "session"])) / 
                (pl.col("volume").cum_sum().over(["vwap_group_date", "session"]))
            ).alias("vwap")

        # Step D: 最終寫入與清理
        return (
            df.with_columns([
                kbar_expr.alias("color"),
                kbar_expr.alias("borderColor"),
                kbar_expr.alias("wickColor"),
                vol_expr.alias("vol_color"),
                vwap_expr,
                *ma_exprs
            ])
            .drop(["date_temp", "vwap_group_date"]) # 清理暫存欄位
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