# config/calendar_rules.py
from datetime import time
import polars as pl

# 定義日盤時段：08:45 ~ 13:45
DAY_START = time(8, 30)
DAY_END = time(13, 45, 5)

def get_session_expression(col_name="ts"):
    """
    回傳一個 Polars Expression，用於標記 Session。
    邏輯：在 08:45:00 ~ 13:45:00 (含) 之間為 'Day'，其餘為 'Night'
    """
    # 提取時間部分
    times = pl.col(col_name).dt.time()
    
    return (
        pl.when((times >= DAY_START) & (times < DAY_END))
        .then(pl.lit("Day"))
        .otherwise(pl.lit("Night"))
        .alias("session")
    )