# config/calendar_rules.py
from datetime import date, time, timedelta

# ⚠️ 這兩個值與 txf-quant-platform 的同名常數**必須一致** —— 湖裡六年 parquet 的
#    `session` 欄就是用它們寫進去的,任一邊漂掉就會讓新舊資料在同一欄位有兩種語意
#    (而且不會報錯)。由 platform 的 `tools/check_time_constants.py` 每日檢查
#    (掛在 daily_sync 第一步),詳見 platform wiki `Time-Semantics`。
# ⚠️ 註:DAY_START=08:30 **不是真實開盤**(08:45),它是「夜盤跨日→日盤」的日期切換樞紐;
#    DAY_END 多的那 5 秒是為了讓收盤集合競價(成交時戳 13:45:00.0xx)仍歸日盤。
# ⚠️ **這兩個仍在使用中,不要以為它們是死的。**(我 P4 第一版的註解寫「已沒有程式在用」,
#    那是錯的 —— 2026-08-04 稽核抓到還有三處在用。現在的實況:)
#      ① `validate_lake.py` ③ 的 session↔ts 一致性檢查 —— **刻意保留自己的字面值**:
#         驗證器若改成 import `session_model`,錯的模型就會自己驗自己過。
#         獨立的第二意見有價值,所以那一處**不遷移**。
#      ② `tools/check_time_constants.py`(platform)用**正則讀原始碼**比對三個 repo,
#         靠這兩個字面值當錨點(它刻意不 import,才能在沒有本 repo venv 的情況下跑)。
#      ③ 向後相容:外部若還有 `from config.calendar_rules import DAY_START` 不會壞。
#    已遷移的:`get_session_expression`(改吃 vendored 值)、`core/resampler.py` 的
#    歸檔日期樞紐(改吃 `ARCHIVE_DATE_PIVOT`);`main_etl.py` 那個未使用的 import 已移除。
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
    """回傳一個 Polars Expression,用於標記 `session` 欄。

    邏輯:`[DAY_PREOPEN_START, DAY_CLOSE + 餘裕)` 之間為 'Day',其餘為 'Night'。

    ## P4(2026-08-04):邊界改吃 vendored 的 `session_model`

    **這支決定新寫進湖的 `session` 欄**,所以它是整個盤段模型工程裡風險最高的一處。
    改法刻意最小:區間端點從本檔的 `DAY_START` / `DAY_END` 換成 `session_model` 的
    `DAY_PREOPEN_START` / `DAY_CLOSE_WITH_GRACE` —— **值完全相同**(08:30 / 13:45:05),
    區間開閉也完全相同,所以是零行為改動。

    🔒 驗收依 wiki §6 P4 的明文要求:**對既有 parquet 重算一遍 session 欄、逐列比對零差異**。
      實測:真實 raw tick 逐列比對零差異;反向對照(故意把 `<` 寫成 `<=`)抓得到差異。

    📌 為什麼值一定相同:`session_model.py` 四份副本由 SHA256 釘死逐位元一致
      (`txf-quant-platform/tools/check_time_constants.py`,掛在 daily_sync 第一步),
      而那份檔的 `DAY_CLOSE + CLOSE_GRACE_SEC` 就是本檔 `DAY_END` 的來源分解。
    """
    import polars as pl              # 見上:保持本模組在純日期用途下零重相依
    from config.session_model import DAY_CLOSE_WITH_GRACE, DAY_PREOPEN_START

    times = pl.col(col_name).dt.time()
    return (
        pl.when((times >= DAY_PREOPEN_START) & (times < DAY_CLOSE_WITH_GRACE))
        .then(pl.lit("Day"))
        .otherwise(pl.lit("Night"))
        .alias("session")
    )