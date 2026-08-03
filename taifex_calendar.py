"""期交所「年度行事曆 PDF」→ 月結算日(2026-07-21 建立)。

**這支為什麼存在**:期交所每年 **11 月中旬**公布次年行事曆 PDF,裡面**直接用色塊標出
12 個月結算日**(含春節順延),是唯一能「提前整整一年拿到全年結算日」的權威來源。
交易所的 OpenAPI 都沒有行事曆端點(TAIFEX 135 / TWSE 143 個端點逐項查過);
Shioaji 合約主檔的 delivery_date 雖是乾淨 API,但只涵蓋掛牌合約(連續 3 月 + 3 季月,有洞)。

**檔案位置**:
  當年度  https://www.taifex.com.tw/file/taifex/CHINESE/4/{year}Calendar.pdf
  往年    https://www.taifex.com.tw/file/taifex/CHINESE/4/{year}Calendar_cv1.pdf
  (往年檔**沒有任何網頁連結指向**,只能靠這個命名規則;不存在的年份伺服器回 200 但 0 bytes)
  本機留存於 {DATA_ROOT}/adjustments/calendars/taifex_calendar_{year}.pdf

⚠️ **解析靠色塊,不是文字** —— 版面若改版會**解錯而非報錯**。所以 `monthly_settlements()`
帶語意閘(每月至多一天 / 落在第三個週三 +0~20 天 / 至少 10 個月),`verify()` 另外用
已知的官方結算日做回歸。**任一閘不過就回 None,呼叫端必須退回演算法,絕不寫入。**

驗證實績(2026-07-21):2020–2026 七年、色 (0.573,0.816,0.314) 恆定、對已知結算日 **77/77**,
含演算法猜錯的兩個春節(2023-01-30、2026-02-23)。
"""
from __future__ import annotations

import collections
import datetime as dt
import os
from typing import Dict, Optional, Set, Tuple

CALENDAR_URL_CURRENT = "https://www.taifex.com.tw/file/taifex/CHINESE/4/{year}Calendar.pdf"
CALENDAR_URL_PAST = "https://www.taifex.com.tw/file/taifex/CHINESE/4/{year}Calendar_cv1.pdf"

MIN_MONTHS = 10          # 少於這個數就不信(PDF 版面可能變了)
MAX_ROLLOVER_DAYS = 20   # 結算日相對第三個週三最多順延幾天(春節 2023 實測 12 天)


# 2026-08-03:公式收斂到 `config/calendar_rules`(repo 內原本有四份同義實作)。
# ⚠️ 那個模組的 polars import 已移進函式內,所以本檔仍維持零重相依。
from config.calendar_rules import third_wednesday        # noqa: E402  (re-export)


def _bands(vals, gap):
    vals = sorted(vals)
    out = [[vals[0]]]
    for v in vals[1:]:
        (out.append([v]) if v - out[-1][-1] > gap else out[-1].append(v))
    return out


def _parse_colors(path: str, year: int) -> Dict[tuple, Set[dt.date]]:
    """回傳 {填色: {日期}}。版面 = 3 欄 × 4 列月曆,列用座標間隙分群、欄用等寬三等分。"""
    import fitz  # 延遲匯入:沒裝 pymupdf 時,本模組其餘功能仍可用

    page = fitz.open(path)[0]
    draws = page.get_drawings()

    def color_at(x0, y0, x1, y1):
        cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
        best = None
        for d in draws:
            if not d.get("fill"):
                continue
            r = d["rect"]
            if r.x0 <= cx <= r.x1 and r.y0 <= cy <= r.y1:
                a = (r.x1 - r.x0) * (r.y1 - r.y0)
                if best is None or a < best[1]:          # 取最小的色塊 = 該格自己的底色
                    best = (tuple(round(c, 3) for c in d["fill"]), a)
        return best[0] if best else None

    words = [w for w in page.get_text("words") if w[4].isdigit() and 1 <= int(w[4]) <= 31]
    if not words:
        return {}
    rows = _bands([w[1] for w in words], 40)
    x0 = min(w[0] for w in words)
    w3 = (max(w[0] for w in words) - x0) / 3

    def rowidx(y):
        for i, b in enumerate(rows):
            if b[0] - 5 <= y <= b[-1] + 5:
                return i
        return min(range(len(rows)), key=lambda i: abs(y - rows[i][0]))

    out = collections.defaultdict(set)
    for w in words:
        m = rowidx(w[1]) * 3 + min(2, int((w[0] - x0) / w3)) + 1
        if not 1 <= m <= 12:
            continue
        try:
            d = dt.date(year, m, int(w[4]))
        except ValueError:
            continue
        c = color_at(*w[:4])
        if c:
            out[c].add(d)
    return out


def monthly_settlements(path: str, year: int) -> Optional[Tuple[tuple, Set[dt.date]]]:
    """挑出語意上唯一合理的色群 = 月結算日。任一閘不過回 None(呼叫端必須退回演算法)。"""
    best = None
    for c, ds in _parse_colors(path, year).items():
        mc = collections.Counter(d.month for d in ds)
        if len(ds) < MIN_MONTHS or max(mc.values()) > 1:
            continue                                        # 閘1:每月至多一天、月份夠多
        if not all(third_wednesday(year, d.month) <= d
                   <= third_wednesday(year, d.month) + dt.timedelta(days=MAX_ROLLOVER_DAYS)
                   for d in ds):
            continue                                        # 閘2:必須落在第三個週三之後的合理窗
        if best is None or len(ds) > len(best[1]):
            best = (c, ds)
    return best


def verify(path: str, year: int, known: Set[dt.date]) -> Tuple[bool, str]:
    """閘3(回歸):與該年已知的官方結算日比對,有一個不符就判失敗。"""
    res = monthly_settlements(path, year)
    if not res:
        return False, "無符合語意閘的色群(PDF 版面可能已改版)"
    _, ds = res
    same_year = {d for d in known if d.year == year}
    bad = sorted(same_year - ds)
    if bad:
        return False, f"與已知官方結算日不符:{[str(b) for b in bad]}"
    return True, f"解出 {len(ds)} 天,對已知 {len(same_year)} 天全符"


def local_path(data_root: str, year: int) -> str:
    return os.path.join(data_root, "adjustments", "calendars", f"taifex_calendar_{year}.pdf")


def download(data_root: str, year: int, current_year: int) -> Optional[str]:
    """下載該年度 PDF 到本機。不存在時伺服器回 200 但 0 bytes → 視為沒有。"""
    import urllib.request

    url = (CALENDAR_URL_CURRENT if year >= current_year else CALENDAR_URL_PAST).format(year=year)
    try:
        data = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=60
        ).read()
    except Exception:
        return None
    if not data.startswith(b"%PDF"):
        return None
    p = local_path(data_root, year)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.write(data)
    return p


def is_published(year: int, current_year: int) -> bool:
    """次年行事曆是否已公布(每年約 11 月中旬)。給 daily_sync 每天做的極廉價探測用。"""
    import urllib.request

    url = (CALENDAR_URL_CURRENT if year >= current_year else CALENDAR_URL_PAST).format(year=year)
    try:
        r = urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}, method="HEAD"),
            timeout=25,
        )
        return int(r.headers.get("Content-Length", 0)) > 0
    except Exception:
        return False
