#!/usr/bin/env python3
"""全史重建對帳 —— 驗證「kbars 是 archive 的純函數」這個前提。

## 這支工具在證明什麼

整個 archive/cache 分層設計的地基是一句話:**kbars 可以從 raw_ticks 完整重建**。
如果它成立,kbars 就是**建置產物** —— 可以不備份、可以整個刪掉、可以隨意改佈局、
schema 演進從「多週遷移專案」變成「改 recipe + rebuild」。

這句話目前只在 2026-08-17 的**還原演練子集**上驗過一次。本工具把它擴到全史。

## 為什麼重建的輸入就是 archive 裡那份 raw

`main_etl` 的順序是:fetch → **Phase 1.5/1.6 清洗** → Phase 2 寫 raw →
Phase 3 `resample_to_kbars(同一份已清洗的 df)`。

也就是說**存進 archive 的 raw 已經是清洗後的**,而 kbars 出自同一份記憶體物件。
所以 `resample_to_kbars(read_parquet(raw))` 應該逐值等於存檔的 kbar。

⚠️ 例外:清洗邏輯是 2026-07 之後才加的。更早歸檔的 raw 若含未清洗的列,重建就會不符
—— 那正是本工具該找出來的東西,不是誤報。

## 🔒 唯讀

只讀 `ARCHIVE_ROOT` 與 `CACHE_ROOT`,只寫 `--out` 指定的報告檔。
啟動時會斷言輸出路徑不在湖裡。

## 用法

    python -m tools.verify_rebuild --sample 40            # 先抽樣驗工具本身
    python -m tools.verify_rebuild --full --out r.json    # 全史
    python -m tools.verify_rebuild --from 2025-01-01 --to 2025-12-31
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import polars as pl  # noqa: E402

from config.lake_paths import (ARCHIVE_ROOT, CACHE_ROOT, kbar_paths,  # noqa: E402
                               list_tick_files, tick_path)
from config.settings import TIMEFRAMES  # noqa: E402
from core.resampler import resample_to_kbars  # noqa: E402

# 本 repo 的慣例(同 validate_lake.py):在碼裡強制 utf-8,不靠 shell 繼承。
# 排程/非 TTY 下印 emoji 在 cp950 會直接崩,不是亂碼。
for _s in (sys.stdout, sys.stderr):
    if hasattr(_s, "reconfigure"):
        _s.reconfigure(encoding="utf-8", errors="replace")

SYMBOLS = ["TXF", "TSE", "TXFR2"]
#: 1d 是年檔累積出來的,比對方式不同(見 compare_yearly)。
INTRADAY_TFS = [tf for tf in TIMEFRAMES if tf != "1d"]


def _days_for(symbol, d_from, d_to):
    out = []
    for p in list_tick_files(symbol):
        day = os.path.basename(p)[:10]
        if (d_from is None or day >= d_from) and (d_to is None or day <= d_to):
            out.append(day)
    return out


def _cmp(built, stored):
    """回傳 None 表示相同,否則回傳一句話說明第一個差異。

    刻意**不用** `df.equals()` 一句帶過:那樣只會得到 True/False,
    而我們需要知道**哪一欄、差多少** —— 不然報告只能說「不對」,無法據以判斷。
    """
    if built.height != stored.height:
        return f"列數 {built.height} vs {stored.height}"
    bc, sc = set(built.columns), set(stored.columns)
    if bc != sc:
        return f"欄位不同:重建多 {sorted(bc - sc)} / 存檔多 {sorted(sc - bc)}"
    cols = list(stored.columns)
    b = built.select(cols).sort("ts")
    s = stored.select(cols).sort("ts")
    for c in cols:
        bs, ss = b[c], s[c]
        if bs.dtype != ss.dtype:
            return f"欄 {c} dtype {bs.dtype} vs {ss.dtype}"
        try:
            same = bs.equals(ss)
        except Exception:
            same = bs.to_list() == ss.to_list()
        if not same:
            bl, sl = bs.to_list(), ss.to_list()
            for i, (x, y) in enumerate(zip(bl, sl)):
                if x != y and not (x is None and y is None):
                    return f"欄 {c} 第 {i} 列 {x!r} vs {y!r}"
            return f"欄 {c} 不符(逐列比對找不到位置,可能是 null 語意)"
    return None


def verify_day(symbol, day, tfs):
    """回傳 {tf: None|說明},以及讀檔耗時。"""
    raw = tick_path(symbol, day)
    if not os.path.exists(raw):
        return {tf: "raw 不存在" for tf in tfs}, 0.0
    t0 = time.time()
    ticks = pl.read_parquet(raw)
    dt_read = time.time() - t0

    res = {}
    for tf in tfs:
        stored_paths = kbar_paths(tf, symbol, day, day)
        try:
            built = resample_to_kbars(ticks, tf)
        except Exception as e:
            res[tf] = f"重建拋例外:{type(e).__name__}: {e}"
            continue
        if not stored_paths:
            res[tf] = None if built.is_empty() else f"存檔缺,但重建出 {built.height} 列"
            continue
        stored = pl.read_parquet(stored_paths[0])
        res[tf] = _cmp(built, stored)
    return res, dt_read


def self_test():
    """證明比較器**抓得到**不符 —— 沒有這一步,「全部通過」沒有意義。

    本 repo 零測試基礎建設(`tests/` 只有一個 x.txt),與其為了一個檔案發明一套
    測試框架,不如把反向對照做成工具自己的一個模式:**跑得起來、看得到、不會腐**。
    精神同 `tests/test_cross_repo_time_constants.py`
    (「『檢查通過』本身沒有意義 —— 必須先證明檢查器抓得到不一致」)。
    """
    p = kbar_paths("5m", "TXF", "2026-08-14", "2026-08-14")
    if not p:
        print("⚠️  self-test 需要 2026-08-14 的 5m 檔,略過")
        return 0
    s = pl.read_parquet(p[0])
    prev = kbar_paths("5m", "TXF", "2026-08-13", "2026-08-13")
    cases = [
        ("自己比自己(應相同)", s, None),
        ("少一列", s.head(s.height - 1), "列數"),
        ("改一個收盤價", s.with_columns(pl.col("close") + 1.0), "欄 close"),
        ("刪一欄", s.drop(s.columns[-1]), "欄位不同"),
        ("換 dtype", s.with_columns(pl.col("volume").cast(pl.Float64)), "dtype"),
    ]
    if prev:
        cases.append(("拿別天的檔來比", pl.read_parquet(prev[0]), "欄 "))

    ok = True
    for name, built, expect in cases:
        why = _cmp(built, s)
        if expect is None:
            good = why is None
        else:
            good = why is not None and expect in why
        ok &= good
        print(f"  {'✅' if good else '🔴'} {name:<22} → {why or '相同'}")
    print(f"\n{'✅ 比較器不是恆綠的' if ok else '🔴 比較器有漏 —— 對帳結果不可信'}")
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser(description="全史重建對帳(唯讀)")
    ap.add_argument("--self-test", action="store_true",
                    help="只跑反向對照:證明比較器抓得到不符")
    ap.add_argument("--symbols", default=",".join(SYMBOLS))
    ap.add_argument("--tfs", default=",".join(INTRADAY_TFS))
    ap.add_argument("--from", dest="d_from", default=None)
    ap.add_argument("--to", dest="d_to", default=None)
    ap.add_argument("--sample", type=int, default=0,
                    help="等距抽樣這麼多天(0=全部)")
    ap.add_argument("--full", action="store_true", help="全史(等同不給 from/to)")
    ap.add_argument("--out", default=None, help="報告 JSON 路徑")
    ap.add_argument("--stop-after", type=int, default=0,
                    help="累積這麼多個不符就停(0=不停)")
    a = ap.parse_args()

    if a.self_test:
        return self_test()

    if a.out:
        outabs = os.path.abspath(a.out)
        for root in (ARCHIVE_ROOT, CACHE_ROOT):
            assert not outabs.lower().startswith(os.path.abspath(root).lower()), \
                f"🔒 報告不可以寫進湖裡:{outabs}"

    symbols = [s for s in a.symbols.split(",") if s]
    tfs = [t for t in a.tfs.split(",") if t]

    print(f"ARCHIVE_ROOT = {ARCHIVE_ROOT}")
    print(f"CACHE_ROOT   = {CACHE_ROOT}")
    print(f"商品 {symbols} / TF {tfs}")

    plan = {}
    for sym in symbols:
        days = _days_for(sym, a.d_from, a.d_to)
        if a.sample and len(days) > a.sample:
            step = len(days) / a.sample
            days = [days[int(i * step)] for i in range(a.sample)]
        plan[sym] = days
        print(f"  {sym}: {len(days)} 天  ({days[0] if days else '-'} .. "
              f"{days[-1] if days else '-'})")

    total = sum(len(v) for v in plan.values())
    print(f"\n共 {total} 個 (商品,日) 組合 × {len(tfs)} 個 TF = {total * len(tfs)} 次比對\n")

    mismatches, errors = [], []
    done = 0
    t_start = time.time()
    for sym, days in plan.items():
        for day in days:
            try:
                res, _ = verify_day(sym, day, tfs)
            except Exception as e:
                errors.append({"symbol": sym, "date": day,
                               "error": f"{type(e).__name__}: {e}",
                               "trace": traceback.format_exc()[-500:]})
                done += 1
                continue
            for tf, why in res.items():
                if why is not None:
                    mismatches.append({"symbol": sym, "date": day, "tf": tf, "why": why})
            done += 1
            if done % 100 == 0 or done == total:
                el = time.time() - t_start
                rate = done / el if el else 0
                eta = (total - done) / rate if rate else 0
                print(f"  {done:>5}/{total}  不符 {len(mismatches):>4}  錯誤 {len(errors):>3}  "
                      f"{el:6.0f}s  ETA {eta:5.0f}s", flush=True)
            if a.stop_after and len(mismatches) >= a.stop_after:
                print(f"\n⛔ 已累積 {len(mismatches)} 個不符,提前停止(--stop-after)")
                break
        else:
            continue
        break

    el = time.time() - t_start
    print(f"\n{'=' * 62}")
    print(f"比對 {done}/{total} 組合,耗時 {el:.0f}s")
    print(f"不符:{len(mismatches)}    錯誤:{len(errors)}")
    if mismatches:
        by_tf = {}
        for m in mismatches:
            by_tf.setdefault(m["tf"], []).append(m)
        print("\n不符分佈:")
        for tf, items in sorted(by_tf.items()):
            print(f"  {tf:6} {len(items):>5} 天   例:{items[0]['symbol']} "
                  f"{items[0]['date']} → {items[0]['why']}")
    verdict = "PASS" if not mismatches and not errors else "FAIL"
    print(f"\n判定:{'✅ PASS —— kbars 確實是 archive 的純函數' if verdict == 'PASS' else '🔴 FAIL —— 見上方清單'}")

    if a.out:
        with open(a.out, "w", encoding="utf-8") as fh:
            json.dump({"verdict": verdict, "checked": done, "total": total,
                       "elapsed_s": round(el, 1), "symbols": symbols, "tfs": tfs,
                       "mismatches": mismatches, "errors": errors,
                       "archive_root": ARCHIVE_ROOT, "cache_root": CACHE_ROOT,
                       "generated": dt.datetime.now().isoformat(timespec="seconds")},
                      fh, ensure_ascii=False, indent=2)
        print(f"報告:{a.out}")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
