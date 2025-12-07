# core/processor.py
import polars as pl
from visualization.style_config import ColorScheme

class DataProcessor:
    """
    負責資料清洗、聚合運算與視覺化前的顏色計算
    """
    @staticmethod
    def process_data(df: pl.DataFrame, timeframe: str, combine_sessions: bool) -> pl.DataFrame:
        if df.is_empty(): return df

        # 1. 日夜盤聚合邏輯 (TXF Special)
        # 這裡的邏輯比較複雜，未來甚至可以考慮整併進 calendar_rules.py
        if timeframe == '1d' and combine_sessions:
            df = DataProcessor._aggregate_sessions(df)

        # 2. 準備顏色參數 (從 Style Config 拿)
        # K棒 (實心)
        k_up_d, k_dn_d = ColorScheme.get_color(True, 'Day'), ColorScheme.get_color(False, 'Day')
        k_up_n, k_dn_n = ColorScheme.get_color(True, 'Night'), ColorScheme.get_color(False, 'Night')
        # 成交量 (半透明/增亮)
        v_up_d, v_dn_d = ColorScheme.get_volume_color(True, 'Day'), ColorScheme.get_volume_color(False, 'Day')
        v_up_n, v_dn_n = ColorScheme.get_volume_color(True, 'Night'), ColorScheme.get_volume_color(False, 'Night')

        # 3. 定義 Polars 表達式
        time_fmt = "%Y-%m-%d" if (timeframe == '1d' and combine_sessions) else "%Y-%m-%d %H:%M:%S"
        
        # 表達式: K棒顏色
        kbar_expr = (
            pl.when(pl.col("is_up"))
            .then(pl.when(pl.col("session") == "Night").then(pl.lit(k_up_n)).otherwise(pl.lit(k_up_d)))
            .otherwise(pl.when(pl.col("session") == "Night").then(pl.lit(k_dn_n)).otherwise(pl.lit(k_dn_d)))
        )
        # 表達式: 成交量顏色
        vol_expr = (
            pl.when(pl.col("is_up"))
            .then(pl.when(pl.col("session") == "Night").then(pl.lit(v_up_n)).otherwise(pl.lit(v_up_d)))
            .otherwise(pl.when(pl.col("session") == "Night").then(pl.lit(v_dn_n)).otherwise(pl.lit(v_dn_d)))
        )

        # 4. 執行向量運算
        return (
            df.with_columns([
                pl.col("date" if timeframe == '1d' and combine_sessions else "ts").dt.strftime(time_fmt).alias("time"),
                pl.col("session").fill_null("Day"),
                (pl.col("close") >= pl.col("open")).alias("is_up")
            ])
            .with_columns([
                kbar_expr.alias("color"),
                kbar_expr.alias("borderColor"),
                kbar_expr.alias("wickColor"),
                vol_expr.alias("vol_color")
            ])
        )

    @staticmethod
    def _aggregate_sessions(df: pl.DataFrame) -> pl.DataFrame:
        """[內部方法] 處理 TXF 全日盤聚合 (包含週五夜盤併入週一)"""
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