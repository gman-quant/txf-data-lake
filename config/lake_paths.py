"""資料湖路徑的**單一真相**(2026-08-17)。

## 為什麼要這個模組

四個 repo 各有一份 `config/settings.py`,各自宣告 `DATA_ROOT`,而且**寫法三種**:

    txf-data-lake        os.environ.get("DATA_ROOT", r"D:\\txf-data")   ← 可覆寫
    txf-quant-platform   DATA_ROOT = r"D:\\txf-data"                    ← 不可覆寫
    txf-gale-engine      DATA_ROOT = "D:/txf-data"                      ← 不可覆寫,而且它要開 public

更難堪的是:**有 12 個檔 `import config.settings` 之後還是直接寫死 `D:/txf-data`**
(含 `daily_sync.py` 與 `feeds/historical.py` 這個 viewer 主讀取器)。

診斷不是「缺模組」——模組有,大家也 import 了——是**模組的抽象層級太低**:
`settings.DATA_ROOT` 只回傳一個字串,拿到之後你還是得自己
`os.path.join(DATA_ROOT, "kbars", tf, sym, year, f"{date}_{sym}_{tf}.parquet")`。
省 12 個字元的模組沒有人會用。對照組:`session_model.py` 封裝的是**行為**,沒有人繞過。

所以這個模組**同時**是兩件事的唯一入口:
  ① **根目錄**在哪(部署組態,因機器而異)
  ② **路徑怎麼拼**(佈局規則,四個 repo 必須一致)

要組出一條路徑,非知道兩者不可 —— 所以它們本來就該在同一個地方。

## 兩個根,兩套政策

    ARCHIVE_ROOT   來源真值(ticks / bidask / md_raw / adjustments)。補不回來。
                   per-day,寫一次就不再動。備份的唯一對象。
    CACHE_ROOT     kbars。**建置產物**,是 ARCHIVE 的純函數,可整個刪掉重建。

預設 `CACHE_ROOT = ARCHIVE_ROOT/kbars` ⇒ **不設任何環境變數的話,行為與 2026-08-17
之前完全相同**。要把 cache 搬上 SSD 只需多設一個環境變數,不必改任何一行程式。

## 🔒 vendored 規則(同 `session_model.py`)

本檔為正典,**逐位元複製**到:

    txf-quant-platform/config/lake_paths.py
    txf-data-lake/config/lake_paths.py
    txf-gale-engine/config/lake_paths.py

由 `txf-quant-platform/tools/check_time_constants.py` 每日比對 SHA256
(掛在 daily_sync 第一步)。**改它要四份一起改。**

為什麼不做成 pip 套件:四個 repo 生命週期不同(platform 走 promote、data-lake 隨時改、
gale 半退役、streaming 在 Ubuntu),共用程式碼會把最嚴的紀律傳染給最鬆的那個
—— 理由與 `session_model.py` 相同,見 `tests/test_cross_repo_time_constants.py`。

## 純 stdlib

不得 import pandas / polars / 任何 repo 內的模組 —— 它要能被四個 repo 在任何時機 import。
"""
import datetime as _dt
import os

__all__ = [
    "LakePathError",
    "ARCHIVE_ROOT", "CACHE_ROOT",
    "DATA_ROOT", "DATA_LAKE_KBAR_DIR",
    "LAYOUT", "DEFAULT_LAYOUT", "LAYOUTS", "layout_of",
    "require_roots", "kbar_dir", "kbar_paths", "kbar_paths_for_days",
    "list_kbar_files", "latest_kbar_file",
    "tick_dir", "tick_path", "list_tick_files",
]


class LakePathError(RuntimeError):
    """根目錄解析失敗 / 不存在。

    刻意用獨立的例外型別:呼叫端可以 grep 得到,而且**不會被誤當成「這天沒資料」**。
    """


# --------------------------------------------------------------------------
# 根目錄解析
# --------------------------------------------------------------------------
#: 出廠預設。⚠️ 它是**這台機器**的值,不是語意的一部分 —— 換機器請設環境變數,
#: 不要改這一行(改了會讓四份 vendored 的 SHA256 不一致)。
_DEFAULT_ARCHIVE_ROOT = "D:/txf-data"


def _resolve(env_names, default):
    """依序試環境變數,都沒有就用 default。**不碰檔案系統**(見 require_roots)。

    為什麼 import 時不做存在檢查:這個模組會被測試、CI、以及沒有掛湖的機器 import。
    import 就炸會讓「跑測試」變成「必須先有一個 3 GB 的湖」。存在檢查改在**用的時候**做
    —— 那正是錯誤該出現的位置。
    """
    for name in env_names:
        v = os.environ.get(name)
        if v:
            return os.path.abspath(os.path.expanduser(v))
    return os.path.abspath(default)


#: 來源真值的根。`DATA_ROOT` 是 txf-data-lake 沿用已久的名字,保留為**過渡期別名**。
ARCHIVE_ROOT = _resolve(("TXF_ARCHIVE_ROOT", "DATA_ROOT"), _DEFAULT_ARCHIVE_ROOT)

#: 建置產物(kbars)的根。預設在 ARCHIVE_ROOT 底下 ⇒ 不設變數即維持現狀。
CACHE_ROOT = _resolve(("TXF_CACHE_ROOT",), os.path.join(ARCHIVE_ROOT, "kbars"))

# --- 向下相容:舊名保留,但**改成推導** ------------------------------------
# ⚠️ 這正是 platform 原本的缺陷:`DATA_LAKE_KBAR_DIR` 是**第二個獨立字面量**,
#    不從 DATA_ROOT 推導 ⇒ 改 DATA_ROOT 不會動到它,而它才是 viewer 主讀取器用的那個。
DATA_ROOT = ARCHIVE_ROOT
DATA_LAKE_KBAR_DIR = CACHE_ROOT

_checked = set()


def require_roots(*roots):
    """確認根目錄真的存在,否則**大聲失敗**。第一次檢查後記住結果(不重複 stat)。

    為什麼一定要有這道:根目錄設錯的話,`historical.py` 會逐日 `os.path.exists` 全部
    跳過,最後 `return pl.DataFrame()` ⇒ **圖是空的,而且沒有任何錯誤訊息**。
    同一個原則已經寫在 `settings.SETTLEMENT_CALENDAR_PATH` 的註解裡:
    「會給錯答案的退路比沒有退路更糟,故讀不到就大聲失敗」。
    """
    for r in (roots or (ARCHIVE_ROOT, CACHE_ROOT)):
        if r in _checked:
            continue
        if not os.path.isdir(r):
            raise LakePathError(
                f"資料湖根目錄不存在:{r}\n"
                f"  ARCHIVE_ROOT={ARCHIVE_ROOT}(env: TXF_ARCHIVE_ROOT / DATA_ROOT)\n"
                f"  CACHE_ROOT  ={CACHE_ROOT}(env: TXF_CACHE_ROOT)\n"
                f"拒絕以預設值繼續 —— 空資料會被誤讀成「這幾天沒盤」。"
            )
        _checked.add(r)


# --------------------------------------------------------------------------
# 佈局
# --------------------------------------------------------------------------
#: 每個 TF 的 kbar 檔案佈局。**這是佈局的唯一真相**(讀者與寫者共用)。
#:
#: ⚠️ 判準必須是**這張表**,不可以是 `if tf == "1d"` —— 用 TF 名字借代佈局是本工作區
#:    被咬過四次的那一族(`combine` 判準借用名字字串、`time_fmt` 三層推導…)。
#:
#: 2026-08-17 現況:1d 是年檔、其餘是日檔(六年演化下來的結果,不是設計)。
#:
#: 檔名規則(三種佈局的字典序都 = 時間序,`list_kbar_files` 的排序才成立):
#:   daily    `<tf>/<sym>/<YYYY>/<YYYY-MM-DD>_<SYM>_<TF>.parquet`
#:   monthly  `<tf>/<sym>/<SYM>_<TF>_<YYYY-MM>.parquet`
#:   yearly   `<tf>/<sym>/<SYM>_<TF>_<YYYY>.parquet`
#:
#: 🔒 **改這張表就是改佈局** —— 讀寫兩端同時跟著變,不必改任何呼叫端。
#:    但 `txf-quant-stable` 釘在 tag、跑的是舊碼 ⇒ **翻表之前必須先 promote**,
#:    否則看盤會讀不到檔而且只會安靜地變空圖。
LAYOUT = {
    "1d": "yearly",
}
DEFAULT_LAYOUT = "daily"

#: 合法值域。新增佈局要同時更新 `kbar_dir` / `kbar_paths` 的分支與這裡。
LAYOUTS = ("daily", "monthly", "yearly")


def layout_of(tf):
    """回傳 'daily' / 'monthly' / 'yearly'。"""
    v = LAYOUT.get(tf, DEFAULT_LAYOUT)
    if v not in LAYOUTS:
        raise LakePathError(f"未知的 kbar 佈局 {v!r}(tf={tf});合法值:{LAYOUTS}")
    return v


def _as_date(d):
    if isinstance(d, _dt.datetime):
        return d.date()
    if isinstance(d, _dt.date):
        return d
    return _dt.date.fromisoformat(str(d)[:10])


def kbar_dir(tf, symbol, year=None):
    """kbar 檔所在的目錄。

    daily 佈局多一層年份子目錄;monthly / yearly 是平的
    (一個 tf/symbol 底下最多 79 個月檔或 7 個年檔,不值得再分層)。
    """
    if layout_of(tf) in ("yearly", "monthly"):
        return os.path.join(CACHE_ROOT, tf, symbol)
    parts = [CACHE_ROOT, tf, symbol]
    if year is not None:
        parts.append(f"{int(year):04d}")
    return os.path.join(*parts)


def _month_starts(s, e):
    """s..e 之間每個月的 (year, month),含頭含尾。"""
    out, y, m = [], s.year, s.month
    while (y, m) <= (e.year, e.month):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def kbar_paths(tf, symbol, start, end, existing_only=True):
    """`start`..`end`(**含頭含尾的日曆日**)對應的 kbar 檔路徑,依時間排序。

    這是所有讀取端唯一該用的入口 —— 它同時知道根目錄與佈局,所以之後改佈局
    只要動這一個函式,不必再回頭改 6–8 處路徑拼接。

    existing_only=True(預設)只回傳實際存在的檔,語意與呼叫端原本的
    `if os.path.exists(path)` 完全相同。
    """
    require_roots(CACHE_ROOT)
    s, e = _as_date(start), _as_date(end)
    if e < s:
        return []

    paths = []
    lay = layout_of(tf)
    if lay == "yearly":
        for year in range(s.year, e.year + 1):
            paths.append(os.path.join(kbar_dir(tf, symbol),
                                      f"{symbol}_{tf}_{year:04d}.parquet"))
    elif lay == "monthly":
        for y, m in _month_starts(s, e):
            paths.append(os.path.join(kbar_dir(tf, symbol),
                                      f"{symbol}_{tf}_{y:04d}-{m:02d}.parquet"))
    else:
        d = s
        step = _dt.timedelta(days=1)
        while d <= e:
            paths.append(os.path.join(kbar_dir(tf, symbol, d.year),
                                      f"{d.isoformat()}_{symbol}_{tf}.parquet"))
            d += step

    if existing_only:
        paths = [p for p in paths if os.path.exists(p)]
    return paths


def kbar_paths_for_days(tf, symbol, days, existing_only=True):
    """指定的**一組日子**(可以不連續)對應的 kbar 檔,去重後依時間排序。

    為什麼要和 `kbar_paths` 分開:呼叫端有時手上是一份「缺哪幾天」的清單
    (例:`historical.py` 的 per-day 結算快取補洞)。用區間會多讀已經有的日子;
    而 per-month 佈局下「一天一檔」不再成立,**多個日子可能對到同一個檔** ——
    去重這件事必須由知道佈局的這一層做,不能留給呼叫端。
    """
    require_roots(CACHE_ROOT)
    seen, paths = set(), []
    for d in sorted(_as_date(x) for x in days):
        for p in kbar_paths(tf, symbol, d, d, existing_only=False):
            if p not in seen:
                seen.add(p)
                paths.append(p)
    if existing_only:
        paths = [p for p in paths if os.path.exists(p)]
    return paths


# --------------------------------------------------------------------------
# archive 側:逐筆成交(ticks)
# --------------------------------------------------------------------------
# 🔒 archive 的佈局規則**刻意與 cache 不同,而且不打算改**:一律日檔、寫一次就不再動。
#    理由:它補不回來 ⇒ 每個位元組只寫一次是最強的耐久性保證。cache 可以重建,
#    所以重寫免費;archive 不行,重寫就是風險。兩邊規則相反是設計,不是不一致。
#    ⚠️ 目錄比 kbars 多一層「月」(`<sym>/<年>/<月>/`)—— 那是既有 schema,別對齊。

def tick_dir(symbol, year=None, month=None):
    parts = [ARCHIVE_ROOT, "raw_ticks", symbol]
    if year is not None:
        parts.append(f"{int(year):04d}")
        if month is not None:
            parts.append(f"{int(month):02d}")
    return os.path.join(*parts)


def tick_path(symbol, d):
    d = _as_date(d)
    return os.path.join(tick_dir(symbol, d.year, d.month),
                        f"{d.isoformat()}_{symbol}_ticks.parquet")


def list_tick_files(symbol):
    """該商品所有現存的逐筆檔,依日期排序。

    ⚠️ 只認 `*_ticks.parquet`。**同一個目錄裡還躺著 `*_bidask.parquet`**
    (178 檔 / 949 MB,2025-12 起)—— 那是五檔簿,不是逐筆成交。
    用 `*.parquet` 一把抓會把它算進「這天有資料」而灌水。
    """
    require_roots(ARCHIVE_ROOT)
    root = os.path.join(ARCHIVE_ROOT, "raw_ticks", symbol)
    if not os.path.isdir(root):
        return []
    suffix = f"_{symbol}_ticks.parquet"
    found = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(suffix):
                found.append((fn, os.path.join(dirpath, fn)))
    found.sort(key=lambda x: x[0])
    return [p for _fn, p in found]


def list_kbar_files(tf, symbol):
    """該 tf/symbol **所有**現存的 kbar 檔,依時間排序(沒有則空 list)。

    ⚠️ 刻意用 `os.walk` 而不是「逐日組路徑再 exists」:全史用日期展開會是**上萬次
    stat**,在機械碟上要數十秒。走目錄一次就好。

    排序依據是**檔名**:兩種佈局的檔名都滿足「字典序 = 時間序」
    (daily 是 `YYYY-MM-DD_…`,yearly 是 `SYM_tf_YYYY`),所以同一套排序都適用。
    🔒 之後加 monthly(`SYM_tf_YYYY-MM`)也仍然成立 —— 新增佈局時要複驗這個前提。
    """
    require_roots(CACHE_ROOT)
    root = os.path.join(CACHE_ROOT, tf, symbol)
    if not os.path.isdir(root):
        return []
    found = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith(".parquet"):
                found.append((fn, os.path.join(dirpath, fn)))
    found.sort(key=lambda x: x[0])
    return [p for _fn, p in found]


def latest_kbar_file(tf, symbol):
    """該 tf/symbol **最新的** kbar 檔(沒有則 None)。

    用途:Gap Recovery 起點要知道湖裡最後一根 K 棒在哪。
    呼叫端原本自己 `os.listdir` 年份目錄再挑最後一個 —— 那是**佈局知識外洩**。
    """
    files = list_kbar_files(tf, symbol)
    return files[-1] if files else None
