"""結算日曆 + 換月價差的單一產生器(2026-07-21 建立)。

**設計要點(為什麼長這樣)**

1. **拆兩張表**:結算「日期」與換月「價差」性質不同 ——
   日期是**日曆事實、未來也需要**(smart-rollover / SVWAP 錨 / 副圖虛線 / 前結前夜都要「今天是不是結算日」);
   價差是**市場事實、只有過去**(結算後才存在)。舊設計把兩者混在 monthly_settlements.csv,
   導致「未來的結算日永遠不在檔案裡」→ 當月結算日畫不出虛線。

2. **四層來源,各有不可取代的角色**(優先序由高而低):
     ① TAIFEX OpenAPI 最後結算價 —— **真值**,但只給最近一次(無歷史查詢)→ `settled`
     ② 期交所年度行事曆 PDF —— 唯一能**提前拿到整年**(每年 11 月中公布)→ `taifex_calendar`
     ③ Shioaji 合約主檔 `delivery_date` —— **複驗層不是來源層**(見 verify_against_shioaji)
     ④ 演算法(第三個週三)—— 唯一**無上限**,但**春節必錯**故 1、2 月不產出(ALGO_BLIND_MONTHS)
   `AUTHORITATIVE` 內的來源永不被演算法覆蓋;不一致時**大聲 log**(消滅靜默失效)。

3. **日曆絕不依賴行情資料完整度**:舊演算法用「該日有沒有 parquet」判斷休市,
   於是**湖缺資料就污染結算日曆**(2024-03 實例:湖缺 13 天 → 結算日算成 04-01,實際 03-20)。
   假日資訊改由 ② 提供。**唯一用到湖的地方是 `verify_settlement_traded()`,而它只報警不寫入。**

4. **日期一律 ISO `YYYY-MM-DD`**:字典序=時間序,`date.fromisoformat` 直接吃,無格式參數。
   (舊調整表的 `YYYY/MM/DD` 隨 apply_adjustment 一併退役。)

輸出:
  {DATA_ROOT}/adjustments/settlement_calendar.csv
      date,contract,status,source,algo_date        status ∈ {settled, scheduled}
  {DATA_ROOT}/adjustments/roll_events.csv
      date,r1_contract,r1_settle,r2_contract,r2_settle,delta
      (**無 cum_delta**:那欄是為了把 delta 加進歷史價格而存在,而回溯調整對「網格/轉倉」型
       策略是錯的工具 —— 真實世界是平舊倉開新倉、價差計入成本,不是價格連續。)
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import os
import urllib.request
from typing import Dict, List, Optional, Tuple

from config.settings import DATA_ROOT
from config.lake_paths import kbar_paths as _kbar_paths

ADJ_DIR = os.path.join(DATA_ROOT, "adjustments")
CALENDAR_CSV = os.path.join(ADJ_DIR, "settlement_calendar.csv")
ROLL_EVENTS_CSV = os.path.join(ADJ_DIR, "roll_events.csv")

TAIFEX_FINAL_SETTLE = "https://openapi.taifex.com.tw/v1/FinalSettlementPriceIndexFutures"
TAIFEX_DAILY_FUT = "https://openapi.taifex.com.tw/v1/DailyMarketReportFut"

CAL_FIELDS = ["date", "contract", "status", "source", "algo_date"]
ROLL_FIELDS = ["date", "r1_contract", "r1_settle", "r2_contract", "r2_settle", "delta"]

FUTURE_MONTHS = 18          # 往未來排幾個月的 scheduled

# 權威來源(演算法絕不覆蓋):TAIFEX 最後結算價 API、期交所年度行事曆 PDF。
# 排在這裡面之外的(algorithm / migrated)才允許被更好的來源取代。
AUTHORITATIVE = {"taifex_api", "taifex_calendar"}


# ── 基礎:第三個週三 + 假日順延 ────────────────────────────────────────────
# 2026-08-03:公式收斂到 `config/calendar_rules`(repo 內原本有**四份**同義實作)。
# 此處保留 re-export,因為它是本模組的公開名稱、外部有引用。
from config.calendar_rules import third_wednesday        # noqa: E402  (re-export)


def next_contract(contract: str) -> str:
    """'202607' → '202608'(TXF 為連續月份合約)。"""
    y, m = int(contract[:4]), int(contract[4:])
    return f"{y + 1}01" if m == 12 else f"{y}{m + 1:02d}"


# 演算法**證實猜不中的月份**:兩個歷史錯誤(2023-01、2026-02)全是春節,而春節由農曆 +
# 行政院公告的補假決定,歷史結算日裡不含這兩個資訊源。留一法實測 77 個月:75/77。
# 選項 b(2026-07-21 用戶決定):**對已知必錯的月份不產出猜測** —— 寧可 unknown,
# 也不要在表裡放一列「長得像資料、實際已知會錯」的東西。1、2 月改由年度行事曆 PDF 提供。
ALGO_BLIND_MONTHS = (1, 2)


def resolve_scheduled(year: int, month: int) -> Optional[dt.date]:
    """演算法推定結算日 = 第三個週三,逢週末順延。1、2 月回 None(見 ALGO_BLIND_MONTHS)。

    **不再嘗試「從歷史學假日」** —— 實測那對預測零貢獻(帶不帶假日集合都是 75/77),
    純屬複雜度。真正的假日資訊來自年度行事曆 PDF,不是從過去的結算日回推。
    """
    if month in ALGO_BLIND_MONTHS:
        return None
    d = third_wednesday(year, month)
    while d.weekday() >= 5:
        d += dt.timedelta(days=1)
    return d


# ── TAIFEX OpenAPI ───────────────────────────────────────────────────────
# (_get_json 已於 2026-08-24 移除 —— 兩個呼叫端都改走 `_fetch_rows_any` 的嗅探,
#  留著它就是下一個「零呼叫端的死碼」。utf-8-sig 的理由搬進 _fetch_rows_any。)


def _fetch_rows_any(url: str, timeout: int = 20) -> List[dict]:
    """**嗅探格式**,不賭哪一種 —— 這個端點的格式已震盪三次:
        原生 JSON → CSV + 中文表頭(2026-07-16,舊 JSON 解析靜默失效 9 天)
                  → JSON + 英文鍵(2026-08 發現;CSV 解析換成 0 筆有效列)
    每次都讓當時「賭對格式」的解析器靜默歸零。首字元 [ / { = JSON,否則 CSV。
    欄名差異由呼叫端用 `_col()` 的雙語名單吸收。"""
    with urllib.request.urlopen(url, timeout=timeout) as r:
        text = r.read().decode("utf-8-sig")
    s = text.lstrip()
    if s.startswith("[") or s.startswith("{"):
        rows = json.loads(s)
        return rows if isinstance(rows, list) else [rows]
    return list(csv.DictReader(text.splitlines()))


def _col(row: dict, *names) -> str:
    """依序試多個欄名(中文 CSV 版 / 英文 JSON 版),都沒有回空字串。"""
    for n in names:
        if n in row:
            return str(row[n] or "").strip()
    return ""


def fetch_final_settlement() -> Optional[Tuple[dt.date, str, float]]:
    """最近一次「最後結算價」→ (結算日, 到期月份, 價)。取 TX(臺股期貨)**月契約**那筆。

    ⚠ 2026-07-27 發現:API 首列開始出現週契約(如 202607W4 @ 7/22)。本 registry
    只管月結算 —— 週契約一旦寫進 cal,「結算日」的消費端(SVWAP 錨、幻影守衛等)
    會把週三誤當結算日 → 只認 6 位數純數字月份,其餘跳過。失敗印警告,不再無聲。
    註:此端點**只回單一最新結算**,週契約上榜後,月契約僅在「月結算日→下個週三」
    的 ~1 週窗口內可見 → 平常日回 None 是**正常**(update ① 會跳過,靠排程日曆),
    月結算會在窗口內被每日排程撈到(與既有「API 偶爾失敗」同級的容錯)。
    """
    try:
        for row in _fetch_rows_any(TAIFEX_FINAL_SETTLE):
            if "TX" not in str(row.get("Contract", "")).split("/"):
                continue
            m = str(row.get("ContractDeliveryMonth", "")).strip()
            if not (len(m) == 6 and m.isdigit()):        # 週契約(202607W4)跳過
                continue
            d = dt.datetime.strptime(str(row["TheFinalSettlementDay"]), "%Y%m%d").date()
            return d, m, float(row["TheFinalSettlementPrice"])
    except Exception as e:
        print(f"  ⚠️ fetch_final_settlement 失敗({e});端點格式可能又變了")
        return None
    return None


def fetch_daily_settlements() -> Tuple[Optional[dt.date], Dict[str, float]]:
    """最新交易日的 TX 各月份**每日結算價** → (該日, {到期月份: 結算價})。只取一般交易時段。

    ⚠ 2026-07-27 修:端點已改回 CSV → 以中文表頭取欄。
    ⚠ 2026-08-24 再修:端點**又**翻回 JSON(英文鍵,`TradingSession` 值仍中文
      「一般」)⇒ CSV 解析拿 0 筆有效列。改嗅探格式 + 雙語欄名,兩種都吃。
    """
    out: Dict[str, float] = {}
    day: Optional[dt.date] = None
    try:
        for row in _fetch_rows_any(TAIFEX_DAILY_FUT):
            if _col(row, "契約代號", "Contract") != "TX":
                continue
            if _col(row, "交易時段", "TradingSession") != "一般":
                continue
            m = _col(row, "到期月份(週別)", "ContractMonth(Week)")
            sp = _col(row, "結算價", "SettlementPrice")
            if "/" in m or sp in ("", "-", "NULL"):      # 價差組合單 / 無值
                continue
            day = dt.datetime.strptime(_col(row, "日期", "Date"), "%Y%m%d").date()
            out[m] = float(sp.replace(",", ""))
    except Exception as e:
        print(f"  ⚠️ fetch_daily_settlements 失敗({e});端點格式可能又變了")
        return None, {}
    if day is None:
        print("  ⚠️ fetch_daily_settlements:0 筆有效列(表頭/格式又變了?)")
    return day, out


# ── 讀寫 ─────────────────────────────────────────────────────────────────
def _read(path: str, key: str) -> Dict[str, dict]:
    if not os.path.exists(path):
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {r[key]: r for r in csv.DictReader(f)}


def _write(path: str, fields: List[str], rows: Dict[str, dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for k in sorted(rows):
            w.writerow({c: rows[k].get(c, "") for c in fields})



# ── 複驗層(只報警,不寫入)────────────────────────────────────────────────
def verify_against_shioaji(cal: Dict[str, dict], log) -> dict:
    """用 Shioaji **合約主檔的 `delivery_date`** 複驗日曆。只比對、不寫入。

    定位是**複驗層不是來源層**:論涵蓋範圍它幾乎全冗(年度行事曆 PDF 已給整年,
    Shioaji 只多給跨年的季月)。它真正補的洞是**臨時變更** —— 結算週遇颱風而順延時,
    去年 11 月印的 PDF 不知道、演算法不知道、TAIFEX 最後結算價 API 要等結算完才有,
    而合約主檔理論上當天就會更新(`update_date` 實測為當日)。

    ⚠️ **「颱風日 delivery_date 會即時改」是未經驗證的假設** —— 歷史上颱風從未撞上結算日
    (2020-02 至今 5 次颱風休市全落在非結算日),Shioaji 也不提供歷史合約狀態,無從回測。
    所以它只是「多一雙眼睛」,真正的保險是下面那支 `verify_settlement_traded()`。
    """
    stats = {"checked": 0, "mismatch": 0}
    try:
        import shioaji as sj
        from config.settings import API_KEY, SECRET_KEY
    except Exception as e:
        log(f"  ⏭️ Shioaji 複驗略過(模組/金鑰不可用:{type(e).__name__})")
        return stats
    api = None
    try:
        api = sj.Shioaji(simulation=True)          # 模擬環境:唯讀合約主檔,不碰生產下單
        api.login(API_KEY, SECRET_KEY)
        by_contract = {v.get("contract"): k for k, v in cal.items()}
        # 2026-08-03:舊寫法 `api.Contracts.Futures.TXF` 會噴 DeprecationWarning
        # (「backed by contracts v2; use api.contracts instead」)。
        # ⚠️ v2 **不是單純改小寫**:`api.contracts.futures` 是**函式**要傳 root,
        #   直接寫 `api.contracts.Futures.TXF` 會 AttributeError。
        # 等價性以實際登入驗過(shioaji 1.7.0):8 個合約的
        # (code, delivery_date, update_date) 逐項相同。
        for c in api.contracts.futures("TXF"):
            m = str(getattr(c, "delivery_month", "") or "")
            d = str(getattr(c, "delivery_date", "") or "").replace("/", "-")
            if len(m) != 6 or not d:
                continue
            stats["checked"] += 1
            ours = by_contract.get(m)
            if ours is None:
                log(f"  ⚠️ Shioaji 有 {m} 交割日 {d},我們日曆沒有這個月 → 請確認")
                stats["mismatch"] += 1
            elif ours != d:
                log(f"  ❗ **不一致**:{m} Shioaji 說 {d},日曆說 {ours} "
                    f"→ 可能是臨時順延(颱風/公告),請人工確認後手動更正")
                stats["mismatch"] += 1
        log(f"  ✅ Shioaji 複驗 {stats['checked']} 個掛牌合約,不一致 {stats['mismatch']} 個")
    except Exception as e:
        log(f"  ⏭️ Shioaji 複驗失敗({type(e).__name__}: {e})—— 不影響其他步驟")
    finally:
        try:
            if api is not None:
                api.logout()
        except Exception:
            pass
    return stats


def verify_settlement_traded(cal: Dict[str, dict], log, lookback_days: int = 10) -> dict:
    """**最強的一道守衛,而且完全由我們自己的資料決定**:
    日曆說某天是結算日,但湖裡那天**日盤 0 根** → 市場休市 → 結算必然順延。

    颱風休市的資料特徵已實證(2023-08-03 / 2024-07-24 / 2024-10-02 / 2024-10-31 / 2026-07-10):
    **TXF 有夜盤、日盤 0 根、TSE 連檔案都沒有**。此處只看日盤根數,不依賴任何外部服務,
    也不依賴 Shioaji 是否即時更新。只回報,不自動改日曆(順延到哪天要等官方,別自己猜)。
    """
    import glob

    stats = {"checked": 0, "suspect": 0}
    today = (dt.datetime.now() + dt.timedelta(hours=9)).date()
    lo = today - dt.timedelta(days=lookback_days)
    try:
        import polars as pl
    except Exception:
        return stats
    for iso, row in cal.items():
        d = dt.date.fromisoformat(iso)
        if not (lo <= d <= today):
            continue
        stats["checked"] += 1
        # 2026-08-24:改走存取層(舊 glob 是佈局的又一份手拼,翻 LAYOUT 即失明)。
        hits = _kbar_paths("1m", "TXF", d, d)
        if not hits:
            log(f"  ⏳ 結算日 {iso} 尚無 1m 檔(今日 ETL 未跑?)—— 待下次確認")
            continue
        try:
            n_day = pl.read_parquet(hits[0], columns=["session"]).filter(
                pl.col("session").str.to_lowercase() == "day").height
        except Exception:
            continue
        if n_day == 0:
            log(f"  🚨 **{iso} 日曆標為結算日,但當天日盤 0 根 = 市場休市** "
                f"→ 結算已順延至次一營業日!日曆此列需更正(等 TAIFEX 公布官方結算日)")
            stats["suspect"] += 1
    return stats


# ── 主流程 ───────────────────────────────────────────────────────────────
def migrate_history() -> int:
    """一次性:把既有 monthly_settlements.csv(r1) + txf_adjustment_table_final.csv(r2/delta)
    的 77 筆歷史,無損搬進兩張新表。舊檔不動(保留當備份)。"""
    import pandas as pd
    old_ms = os.path.join(ADJ_DIR, "monthly_settlements.csv")
    old_adj = os.path.join(ADJ_DIR, "txf_adjustment_table_final.csv")
    cal = _read(CALENDAR_CSV, "date")
    roll = _read(ROLL_EVENTS_CSV, "date")

    if not os.path.exists(old_ms):
        raise FileNotFoundError(
            f"{old_ms} 不存在 —— 舊表已於 2026-07-21 遷移完成後刪除。"
            f"本函式是**一次性遷移**的歷史紀錄,不應再被呼叫;"
            f"歷史結算日現已在 settlement_calendar.csv 內(status=settled)。")
    ms = pd.read_csv(old_ms)
    adj = pd.read_csv(old_adj) if os.path.exists(old_adj) else pd.DataFrame()
    adj_by_date = {}
    if len(adj):
        for _, r in adj.iterrows():
            iso = dt.datetime.strptime(str(r["date"]), "%Y/%m/%d").date().isoformat()
            adj_by_date[iso] = r

    n = 0
    for _, r in ms.iterrows():
        iso = pd.to_datetime(r["date"]).date().isoformat()
        contract = str(r["contract"]).strip()
        cal[iso] = {"date": iso, "contract": contract, "status": "settled",
                    "source": "migrated", "algo_date": ""}
        a = adj_by_date.get(iso)
        if a is not None:
            roll[iso] = {"date": iso, "r1_contract": contract, "r1_settle": int(r["r1_settle"]),
                         "r2_contract": next_contract(contract), "r2_settle": int(a["r2_settle"]),
                         "delta": int(a["delta"])}
        n += 1
    _write(CALENDAR_CSV, CAL_FIELDS, cal)
    _write(ROLL_EVENTS_CSV, ROLL_FIELDS, roll)
    return n


def update(verbose: bool = True, skip_shioaji: bool = False) -> dict:
    """每日呼叫,冪等:①API 權威覆寫已結算 ②演算法排未來 ③補 roll_event
    ④**複驗**(湖的日盤根數 + Shioaji 合約主檔)—— 複驗只報警,絕不自動改日曆。"""
    def log(m):
        if verbose:
            print(m)

    cal = _read(CALENDAR_CSV, "date")
    roll = _read(ROLL_EVENTS_CSV, "date")
    stats = {"corrected": 0, "settled_new": 0, "scheduled": 0, "roll_new": 0,
             "postpone_suspect": 0, "shioaji_mismatch": 0, "blind_removed": 0}

    # ① API:最近一次最後結算價 → 權威確認/修正
    fs = fetch_final_settlement()
    if fs:
        sd, contract, r1 = fs
        iso = sd.isoformat()
        algo = resolve_scheduled(sd.year, sd.month)
        prev = cal.get(iso)
        # 若同月有「演算法排的別的日期」→ 那是猜錯,移除並記錄
        for k in [k for k, v in cal.items()
                  if v.get("contract") == contract and k != iso and v.get("status") != "settled"]:
            log(f"  ⚠️ 演算法猜的 {k} 與官方 {iso} 不符 → 以官方為準(移除猜測列)")
            cal.pop(k); stats["corrected"] += 1
        if not prev or prev.get("status") != "settled":
            stats["settled_new"] += 1
        cal[iso] = {"date": iso, "contract": contract, "status": "settled",
                    "source": "taifex_api", "algo_date": algo.isoformat() if algo else ""}
        if algo and algo != sd:
            log(f"  ⚠️ 演算法 {algo} ≠ 官方 {iso}(已保留 algo_date 供稽核)")
        log(f"  ✅ 最後結算價 {contract} @ {iso} = {r1:.0f}")

        # ③ 該結算日的 R2 每日結算價 → roll_event
        if iso not in roll:
            day, settles = fetch_daily_settlements()
            r2c = next_contract(contract)
            if day == sd and r2c in settles:
                roll[iso] = {"date": iso, "r1_contract": contract, "r1_settle": int(r1),
                             "r2_contract": r2c, "r2_settle": int(settles[r2c]),
                             "delta": int(settles[r2c] - r1)}
                stats["roll_new"] += 1
                log(f"  ✅ roll_event {iso}: R2 {r2c}={settles[r2c]:.0f} delta={settles[r2c]-r1:+.0f}")
            else:
                log(f"  ⏳ roll_event {iso} 待補(每日行情為 {day},非結算日資料)")

    # ②0 先清掉演算法在「盲月」(春節)留下的舊猜測 —— ALGO_BLIND_MONTHS 是後來才加的,
    # 檔案裡可能還躺著早期版本寫進去的 1、2 月猜測列。權威來源的列不動。
    for k in [k for k, v in cal.items()
              if v.get("source") == "algorithm" and v.get("status") != "settled"
              and dt.date.fromisoformat(k).month in ALGO_BLIND_MONTHS]:
        log(f"  🧹 移除春節盲月的演算法猜測 {k}(改由年度行事曆 PDF 提供)")
        cal.pop(k)
        stats["blind_removed"] += 1

    # ② 演算法排未來 —— **只填權威來源沒蓋到的月份**。
    # 權威 = 已結算(TAIFEX API)或期交所年度行事曆 PDF;演算法是最後退路,絕不覆蓋這兩者。
    today = (dt.datetime.now() + dt.timedelta(hours=9)).date()
    y, m = today.year, today.month
    for _ in range(FUTURE_MONTHS):
        contract = f"{y}{m:02d}"
        covered = any(v.get("contract") == contract
                      and (v.get("status") == "settled" or v.get("source") in AUTHORITATIVE)
                      for v in cal.values())
        guess = resolve_scheduled(y, m)
        if not covered and guess:
            d = guess.isoformat()
            prev = cal.get(d, {})
            if prev.get("status") != "settled" and prev.get("source") not in AUTHORITATIVE:
                cal[d] = {"date": d, "contract": contract, "status": "scheduled",
                          "source": "algorithm", "algo_date": d}
                stats["scheduled"] += 1
        m += 1
        if m > 12:
            y, m = y + 1, 1

    _write(CALENDAR_CSV, CAL_FIELDS, cal)
    _write(ROLL_EVENTS_CSV, ROLL_FIELDS, roll)

    # ②c 盲月的年度行事曆**自動**攝取(2026-08-24,產品碼稽核)。
    #
    # 病:演算法刻意不猜 1、2 月(ALGO_BLIND_MONTHS,春節必錯),那兩個月**只能**
    # 來自期交所年度行事曆 PDF —— 而攝取入口 `import_calendars()` 先前只有手動的
    # `--import-calendars` 旗標走得到,daily_sync 第 ⑥ 步跑的 `-m settlement_registry`
    # 根本不會呼叫它。⇒ 2027-01 / 02 在 2026-08 當下就是缺的;拖到 2027-01 的結算日,
    # `is_settlement()` 對真結算日回 False ⇒ SVWAP / sB / 前結參考線**不重錨**,
    # 而 CSV 存在且非空,不會有任何例外。
    #
    # 修:盲月進入 90 天視野而無權威覆蓋 → 每日 sync 自動嘗試攝取該年 PDF。
    #   ‧ 90 天閘:PDF 約 11 月中公布,10 月初起探測留足裕度;**不用 FUTURE_MONTHS
    #     當視野**(它是 18 個月,會讓七月就開始天天打一份不存在的 PDF)。
    #   ‧ 冪等:攝取成功 → 盲月有 AUTHORITATIVE 覆蓋 → 之後每天這段直接跳過。
    #   ‧ 失敗無害:download() 以 %PDF magic 驗身,拿到錯誤頁回 None、整年跳過並
    #     大聲說明(import_calendars 既有的語意閘,寧可少一年不可寫入解錯的日期)。
    #   ‧ 放在 update 自己的 _write **之後**:import_calendars 內部自己讀寫 CSV,
    #     放前面會被本函式手上的舊 `cal` 蓋回去。寫完重讀,讓 ④ 複驗層驗到新狀態。
    _blind_pending = []
    _yy, _mm = today.year, today.month
    for _ in range(FUTURE_MONTHS):
        if _mm in ALGO_BLIND_MONTHS:
            _c = f"{_yy}{_mm:02d}"
            _covered = any(v.get("contract") == _c
                           and (v.get("status") == "settled"
                                or v.get("source") in AUTHORITATIVE)
                           for v in cal.values())
            _eta = (dt.date(_yy, _mm, 1) - today).days
            if not _covered and 0 <= _eta <= 90:
                _blind_pending.append((_yy, _mm))
        _mm += 1
        if _mm > 12:
            _yy, _mm = _yy + 1, 1
    if _blind_pending:
        _years = sorted({y_ for y_, _m2 in _blind_pending})
        log(f"  ⏳ 盲月 {_blind_pending} 90 天內到期且無權威來源 → 嘗試攝取年度行事曆 {_years}")
        stats["calendar_import"] = import_calendars(_years, verbose=verbose)
        cal = _read(CALENDAR_CSV, "date")

    # ④ 複驗層(只報警不寫入):我們自己的資料先驗,再用 Shioaji 合約主檔多看一眼
    stats["postpone_suspect"] = verify_settlement_traded(cal, log)["suspect"]
    if not skip_shioaji:
        stats["shioaji_mismatch"] = verify_against_shioaji(cal, log)["mismatch"]
    return stats


# ⚠️ 2026-08-23:這裡曾有 `load_settlement_dates()`,自稱「給消費端用」——
#    但唯一的消費端(txf-quant-platform)從第一天起就自己讀 CSV
#    (`DataEngine._load_settlement_dates` / `feeds/historical._settlement_days`),
#    而 platform wiki 把**那份**釘成單一來源。這支從沒有過消費端。
#    ⚠ 兩邊的失敗語意刻意不同:這支「讀不到回空集合」,platform 那側「讀不到就 raise」。
#      要重建跨 repo 的共用入口,得先談那個差異,不是把這支接回去。
#    `import csv` 留著 —— `_read` / `_write` 在用。


def import_calendars(years, verbose: bool = True) -> dict:
    """把期交所年度行事曆 PDF 的月結算日匯入日曆(source=taifex_calendar)。

    每一年都必須通過 taifex_calendar.verify()(語意閘 + 對已知官方結算日的回歸)才寫入;
    不過閘就整年跳過並大聲說明 —— 寧可少一年,不可寫入解錯的日期。
    """
    import taifex_calendar as TC

    def log(m):
        if verbose:
            print(m)

    cal = _read(CALENDAR_CSV, "date")
    known = {dt.date.fromisoformat(r["date"]) for r in cal.values() if r.get("status") == "settled"}
    today = (dt.datetime.now() + dt.timedelta(hours=9)).date()
    cur_year = today.year
    stats = {"years_ok": 0, "years_failed": 0, "added": 0, "upgraded": 0}

    for y in years:
        p = TC.local_path(DATA_ROOT, y)
        if not os.path.exists(p):
            p = TC.download(DATA_ROOT, y, cur_year)
            if not p:
                log(f"  {y}: ⏭️ 期交所無此年度行事曆")
                continue
        ok, msg = TC.verify(p, y, known)
        if not ok:
            log(f"  {y}: ❌ 不採用 —— {msg}")
            stats["years_failed"] += 1
            continue
        _, dates = TC.monthly_settlements(p, y)
        for d in sorted(dates):
            iso = d.isoformat()
            contract = f"{d.year}{d.month:02d}"
            prev = cal.get(iso)
            if prev and prev.get("status") == "settled":
                continue                                   # 已結算的官方值最權威,不動
            # 同月若有演算法猜的別的日期 → 移除(PDF 是官方公布值)
            for k in [k for k, v in cal.items()
                      if v.get("contract") == contract and k != iso
                      and v.get("source") not in AUTHORITATIVE and v.get("status") != "settled"]:
                cal.pop(k)
                log(f"     ↺ {y}-{d.month:02d} 演算法猜的 {k} → 官方 {iso}")
            stats["upgraded" if prev else "added"] += 1
            # status 是**時間狀態**(已成定局 / 尚未到來),不是「有沒有結算價」——
            # 結算價住在 roll_events.csv。2019 那種「日期已過、但我們沒有當時價格」的列
            # 標成 scheduled 會讀起來像「排定中」,是錯的。
            cal[iso] = {"date": iso, "contract": contract,
                        "status": "settled" if d < today else "scheduled",
                        "source": "taifex_calendar", "algo_date": TC.third_wednesday(y, d.month).isoformat()}
        log(f"  {y}: ✅ {msg}")
        stats["years_ok"] += 1

    _write(CALENDAR_CSV, CAL_FIELDS, cal)
    return stats


if __name__ == "__main__":
    import sys
    if "--migrate" in sys.argv:
        print(f"遷移歷史… {migrate_history()} 筆")
    if "--import-calendars" in sys.argv:
        i = sys.argv.index("--import-calendars")
        # 上界由今年推導(2026-08-24)—— 寫死 "2027" 意味著 2027-11 之後手動攝取
        # 2028 的行事曆會被靜靜截掉(而且不報錯)。
        _default_rng = f"2019-{dt.datetime.now().year + 1}"
        rng = sys.argv[i + 1] if len(sys.argv) > i + 1 and "-" in sys.argv[i + 1] else _default_rng
        a, b = (int(x) for x in rng.split("-"))
        print(f"匯入期交所年度行事曆 {a}~{b}…")
        print(" ", import_calendars(range(a, b + 1)))
    print("更新中…")
    print(" ", update(skip_shioaji="--no-shioaji" in sys.argv))
