#!/usr/bin/env python3
"""kbar 日檔 → 月檔轉換(Phase F 的搬運工)。

## 為什麼要它

`lake_paths.LAYOUT` 翻成 `monthly` 之後,**讀寫兩端立刻改看月檔**,但磁碟上還是日檔。
這支負責把既有的日檔併成月檔。

    檔數 24,228 → 約 1,422(6 TF × 3 商品 × 79 個月)

⚠️ **翻表與轉檔必須是同一個時機的兩個動作**,順序不能反:
先翻表 → 讀取端找不到月檔 → 空圖(而且不報錯,因為「這天沒資料」是合法狀態)。
所以正確流程是:**先轉檔(本工具)→ 驗 → 才翻表**。

## 🔒 安全設計

- **預設 `--dry-run`**:什麼都不寫,只印計畫。要真的寫必須明確加 `--write`。
- `--out-root` 可把結果寫到別的地方(例如 scratch),**生產完全不動** —— 這是
  promote 之前先驗整條鏈的方法。
- **不刪日檔。** 刪除是獨立的、需要人點頭的動作(`--prune` 只印指令,不執行)。
- 寫完**立刻讀回來與來源逐值比對**;不符就刪掉那個月檔並報錯,不留半成品。

## 為什麼 1d 也轉成月檔(明明年檔更少)

1d 從 7 個年檔變成 79 個月檔,**檔數是變多的**。仍然這樣做的理由是**消滅特例**:
`main_etl` 的 Case A/B、`historical.py` 的 `if base_tf == "1d"`、以及「1d 的身分鍵是
`(date, session)` 而其他 TF 是 `ts`」這三個分岔,全部來自 1d 的佈局與眾不同。
本工作區被「用 TF 名字借代行為」咬過四次 —— 用 200 個檔換掉三個特例是划算的。

## 用法

    python -m tools.compact_kbars --out-root <scratch>/cache --write   # 先在別處驗
    python -m tools.compact_kbars --write                              # 真的轉生產
    python -m tools.compact_kbars --symbols TXF --tfs 30m --write
"""
import argparse
import collections
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl  # noqa: E402

from config.lake_paths import CACHE_ROOT, list_kbar_files  # noqa: E402
from config.settings import TIMEFRAMES  # noqa: E402

for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8", errors="replace")

SYMBOLS = ["TXF", "TSE", "TXFR2"]


def _month_of(path, tf, symbol):
    """從**現有**檔名取出 YYYY-MM。

    daily  `2026-08-14_TXF_5m.parquet`      → 2026-08
    yearly `TXF_1d_2026.parquet`            → 需要看內容(一個年檔橫跨 12 個月)
    """
    b = os.path.basename(path)
    if b[:4].isdigit() and b[4] == "-":          # daily
        return b[:7]
    return None                                   # yearly:交給呼叫端讀內容分月


def plan(tf, symbol):
    """回傳 {YYYY-MM: [來源檔…]}(已排序)。"""
    groups = collections.OrderedDict()
    for p in list_kbar_files(tf, symbol):
        m = _month_of(p, tf, symbol)
        if m is None:                             # 年檔:依 date 欄拆月
            df = pl.read_parquet(p)
            for mm in sorted(df["date"].cast(pl.Utf8).str.slice(0, 7).unique().to_list()):
                groups.setdefault(mm, []).append((p, mm))
        else:
            groups.setdefault(m, []).append((p, None))
    return groups


def _load(src_list):
    """把一個月的來源讀成一張表。第二個元素非 None 表示要從年檔裡篩該月。"""
    frames = []
    for p, month_filter in src_list:
        df = pl.read_parquet(p)
        if month_filter is not None:
            df = df.filter(pl.col("date").cast(pl.Utf8).str.starts_with(month_filter))
        frames.append(df)
    if not frames:
        return pl.DataFrame()
    try:
        out = pl.concat(frames)
    except Exception:
        out = pl.concat(frames, how="diagonal")   # 舊檔可能缺欄(10 欄時代)
    return out.sort("ts")


def compact(tf, symbol, out_root, write, report):
    groups = plan(tf, symbol)
    dest_dir = os.path.join(out_root, tf, symbol)
    # 來源檔數要**全域去重**:一個年檔會出現在它涵蓋的每一個月裡,
    # 逐月累加會把 7 個年檔數成 83 個 —— 報告裡的誤導數字比沒有數字更糟。
    report["src_files"] += len({p for src in groups.values() for p, _ in src})
    for month, src in groups.items():
        df = _load(src)
        dest = os.path.join(dest_dir, f"{symbol}_{tf}_{month}.parquet")
        report["months"] += 1
        report["rows"] += df.height
        if not write:
            continue
        os.makedirs(dest_dir, exist_ok=True)
        tmp = f"{dest}.tmp{os.getpid()}"
        df.write_parquet(tmp)
        os.replace(tmp, dest)                     # 原子換檔(同 _atomic_write_parquet)
        # 立刻讀回來逐值比對 —— 不留沒驗過的半成品
        back = pl.read_parquet(dest)
        if not back.sort("ts").equals(df.sort("ts")):
            os.remove(dest)
            raise RuntimeError(f"寫回驗證失敗,已刪除:{dest}")
        report["written"] += 1


def main():
    ap = argparse.ArgumentParser(description="kbar 日檔 → 月檔(預設 dry-run)")
    ap.add_argument("--symbols", default=",".join(SYMBOLS))
    ap.add_argument("--tfs", default=",".join(TIMEFRAMES))
    ap.add_argument("--out-root", default=None,
                    help="輸出根目錄(預設 = CACHE_ROOT,即原地轉換)")
    ap.add_argument("--write", action="store_true",
                    help="真的寫入。**不加就只是 dry-run**")
    ap.add_argument("--prune", action="store_true",
                    help="只印出刪除日檔的指令,不執行")
    a = ap.parse_args()

    out_root = os.path.abspath(a.out_root) if a.out_root else CACHE_ROOT
    same = os.path.normcase(out_root) == os.path.normcase(CACHE_ROOT)
    print(f"來源 CACHE_ROOT = {CACHE_ROOT}")
    print(f"輸出 out-root   = {out_root}{'  (原地)' if same else '  (別處,生產不動)'}")
    print(f"模式           = {'✍️  WRITE' if a.write else '🔍 DRY-RUN(什麼都不寫)'}\n")

    report = collections.Counter()
    t0 = time.time()
    for tf in [t for t in a.tfs.split(",") if t]:
        for sym in [s for s in a.symbols.split(",") if s]:
            before = dict(report)
            compact(tf, sym, out_root, a.write, report)
            print(f"  {tf:5} {sym:6} {report['src_files']-before.get('src_files',0):>5} 個來源檔"
                  f" → {report['months']-before.get('months',0):>4} 個月檔"
                  f"  {report['rows']-before.get('rows',0):>9,} 列")

    el = time.time() - t0
    print(f"\n{'=' * 62}")
    print(f"來源 {report['src_files']:,} 檔 → 月檔 {report['months']:,} 個"
          f"(實寫 {report['written']:,});{report['rows']:,} 列;{el:.0f}s")
    if report["src_files"]:
        print(f"檔數 {report['src_files']:,} → {report['months']:,} "
              f"({report['src_files']/max(report['months'],1):.1f}× 減少)")
    if not a.write:
        print("\n🔍 這是 dry-run。要真的寫請加 --write。")
    if a.prune:
        print("\n⚠️ 刪除日檔的指令(**本工具不執行**,請人工確認後自己跑):")
        print(f"   1. 先確認月檔讀得到:python -m tools.verify_rebuild --sample 40")
        print(f"   2. 再刪:find \"{CACHE_ROOT}\" -name '????-??-??_*_*.parquet' -delete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
