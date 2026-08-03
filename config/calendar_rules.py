# config/calendar_rules.py
from datetime import date, time, timedelta

# ⚠️ 這兩個值與 txf-quant-platform 的同名常數**必須一致** —— 湖裡六年 parquet 的
#    `session` 欄就是用它們寫進去的,任一邊漂掉就會讓新舊資料在同一欄位有兩種語意
#    (而且不會報錯)。由 platform 的 `tools/check_time_constants.py` 每日檢查
#    (掛在 daily_sync 第一步),詳見 platform wiki `Time-Semantics`。
# ⚠️ 註:DAY_START=08:30 **不是真實開盤**(08:45),它是「夜盤跨日→日盤」的日期切換樞紐;
#    DAY_END 多的那 5 秒是為了讓收盤集合競價(成交時戳 13:45:00.0xx)仍歸日盤。
DAY_START = time(8, 30)
DAY_END = time(13, 45, 5)


# ── 月份/星期幾的日期算術(2026-08-03 收斂)────────────────────────────────
# 先前 repo 內有**四份**同義實作:`settlement_registry.third_wednesday`、
# `taifex_calendar.third_wednesday`(與前者逐字相同)、`txo_gex_daily.nth_wed`
# 與 `txo_gex_daily.nth_weekday`(**同一個檔案裡的兩份**)。
# 合併前已證明四式等價:1990–2100 共 1332 個月份逐月比對全同;
# 反向對照(故意寫成「第二個週三」)在 2020–2030 有 132 個月不同 ⇒ 該比法驗得出差異。
#
# ⚠️ 這裡**刻意不 import polars** —— `taifex_calendar.py` 原本零重相依,
#    讓它為了一個日期公式而拖進 polars 是退步。故 polars 只在需要它的函式內 import。
def nth_weekday(year: int, month: int, n: int, weekday: int) -> date:
    """該月第 `n` 個星期 `weekday`(Mon=0 … Sun=6)。"""
    first = date(year, month, 1).weekday()
    return date(year, month, 1 + (weekday - first) % 7 + 7 * (n - 1))


def third_wednesday(year: int, month: int) -> date:
    """該月第三個星期三 = 台指期月合約的**名目**結算日(順延另計)。"""
    return nth_weekday(year, month, 3, 2)


def get_session_expression(col_name="ts"):
    """
    回傳一個 Polars Expression，用於標記 Session。
    邏輯：`[DAY_START, DAY_END)` 之間為 'Day'，其餘為 'Night'
    """
    import polars as pl              # 見上:保持本模組在純日期用途下零重相依
    # 提取時間部分
    times = pl.col(col_name).dt.time()

    return (
        pl.when((times >= DAY_START) & (times < DAY_END))
        .then(pl.lit("Day"))
        .otherwise(pl.lit("Night"))
        .alias("session")
    )