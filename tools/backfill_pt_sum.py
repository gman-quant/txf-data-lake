# -*- coding: utf-8 -*-
"""true_pt_sum / dur_s 全史回填(2026-08-16;wiki/MA-Semantics §6 落地)。

從 raw_ticks 重跑(新版)resample_to_kbars,把 true_pt_sum / dur_s **嫁接**到
既有 kbars 檔上(原子 os.replace)。v2(同日第一輪跑完後改):

  ‧ **舊 10 欄一律原封保留**(連位元都不動)——寫回的檔 = 舊檔 + 兩新欄。
  ‧ 嫁接條件 = 重算與舊檔對齊:ts/OHLCV 逐位元同;true_pv_sum 容忍相對 1e-9
    (第一輪實證:TSE 30m 全史 1614 檔只差最後一個 ULP —— 7/31 的 30m 回填走
    「5m 求和」、tick 直算是一段求和,浮點加法不可結合;TXF 價格是整數所以豁免)。
  ‧ 對不齊 → 該檔(1d 為該**列**)兩新欄填 **null** ⇒ 平台自動退化 close×τ,
    且全湖 schema 一致(混 10/12 欄會把 historical 讀取打進 diagonal concat 慢路徑)。
    已知對不齊的成因(2026-08-15 定性,詳見失敗清單檔):
      B 類 13 個零星日:raw 事後被重抓,同秒內 tick 順序不同(open/close 對調)
        或整段缺漏(TXFR2 2025-01-06 raw 丟了週五夜盤 —— 舊 kbars 反而比 raw 全);
      C 類:TXF 1d 2024-03-11 兩列存檔量只有真值 2%(既有壞列)。
    這些是「既有的 raw↔kbars 不一致」,修不修由使用者裁決,本腳本不動值。
  ‧ 可中斷續跑(已 12 欄的檔跳過);--dry-run 只驗不寫。

用法:
    python -m tools.backfill_pt_sum --dry-run          # 只驗證,不寫任何檔
    python -m tools.backfill_pt_sum                    # 全量(先自動全量備份)
    python -m tools.backfill_pt_sum --symbol TXF       # 單商品

備份:首次執行把整個 kbars/ 複製到 kbars_backup_pre_ptsum/(501MB;已存在則跳過)。
1d 年檔:以「(date, session) 身分、處理順序 keep-last」重演 ETL 的 append 語意。
"""
import argparse
import glob
import os
import math
import shutil
import sys
import time

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATA_ROOT, TIMEFRAMES          # noqa: E402
from core.resampler import resample_to_kbars               # noqa: E402

OLD_COLS = ["symbol", "date", "ts", "session",
            "open", "high", "low", "close", "volume", "true_pv_sum"]
NEW_COLS = OLD_COLS + ["true_pt_sum", "dur_s"]
SYMBOLS = ["TXF", "TXFR2", "TSE"]


def _atomic_write(df: pl.DataFrame, path: str) -> None:
    tmp = f"{path}.tmp{os.getpid()}"
    df.write_parquet(tmp)
    os.replace(tmp, path)


def _col_match(c: str, a: list, b: list) -> bool:
    """欄位相等判準:true_pv_sum 容相對 1e-9(求和順序 ULP;見檔頭),其餘逐位元。"""
    if c != "true_pv_sum":
        return a == b
    return all((x == y) or (x is not None and y is not None
                            and math.isclose(x, y, rel_tol=1e-9))
               for x, y in zip(a, b))


def _verify_old_cols(new: pl.DataFrame, old: pl.DataFrame, label: str,
                     failures: list) -> bool:
    """舊 10 欄與重算對齊(含列序;pv 容差見 _col_match)。不齊 → 記失敗,回 False。"""
    if new.height != old.height:
        failures.append(f"{label}: 根數 {old.height}→{new.height}")
        return False
    for c in OLD_COLS:
        if c not in old.columns:                     # 舊檔缺欄(不該發生)→ 跳過該欄
            continue
        if not _col_match(c, new[c].to_list(), old[c].to_list()):
            failures.append(f"{label}: 欄 {c} 不符")
            return False
    return True


def _graft(old: pl.DataFrame, pt: list, dur: list) -> pl.DataFrame:
    """舊檔 + 兩新欄(值或 null),舊欄位元不動。"""
    return old.with_columns(
        pl.Series("true_pt_sum", pt, dtype=pl.Float64),
        pl.Series("dur_s", dur, dtype=pl.Float64),
    )


def run(symbols, dry_run: bool) -> int:
    kbars_root = os.path.join(DATA_ROOT, "kbars")
    backup = os.path.join(DATA_ROOT, "kbars_backup_pre_ptsum")
    if not dry_run:
        if not os.path.exists(backup):
            print(f"[backup] kbars/ → {backup} …", flush=True)
            shutil.copytree(kbars_root, backup)
            print("[backup] 完成", flush=True)
        else:
            print(f"[backup] 已存在(續跑),跳過:{backup}", flush=True)

    failures: list = []
    t0 = time.time()
    n_written = n_skipped = n_days = n_nulled = 0

    for sym in symbols:
        raw_files = sorted(glob.glob(
            os.path.join(DATA_ROOT, "raw_ticks", sym, "*", "*", f"*_{sym}_ticks.parquet")))
        yearly_files = sorted(glob.glob(os.path.join(kbars_root, "1d", sym,
                                                     f"{sym}_1d_*.parquet")))
        # 1d 只要還有任何年檔缺新欄,就得對每一天重算(年檔身分靠全史累積)
        need_1d = any("true_pt_sum" not in pl.scan_parquet(y).collect_schema().names()
                      for y in yearly_files)
        print(f"[{sym}] raw 天數 {len(raw_files)},1d 待補={need_1d}", flush=True)
        oned_frames: list = []                       # 每天的 1d DataFrame(時序=ETL 處理序)
        for i, rf in enumerate(raw_files):
            date_str = os.path.basename(rf)[:10]
            year = date_str[:4]
            intraday_tfs = [tf for tf in TIMEFRAMES if tf != "1d"]
            targets = {tf: os.path.join(kbars_root, tf, sym, year,
                                        f"{date_str}_{sym}_{tf}.parquet")
                       for tf in intraday_tfs}
            need = [tf for tf in intraday_tfs
                    if os.path.exists(targets[tf])
                    and "true_pt_sum" not in pl.scan_parquet(targets[tf])
                    .collect_schema().names()]
            if not need and not need_1d:
                n_skipped += 1
                continue
            ticks = pl.read_parquet(rf)
            for tf in (need + (["1d"] if need_1d else [])):
                new = resample_to_kbars(ticks, tf)
                if tf == "1d":
                    oned_frames.append(new)
                    continue
                path = targets[tf]
                if not os.path.exists(path):         # 覆蓋完美 1:1,不該發生 → 記錄
                    failures.append(f"{sym}/{tf}/{date_str}: 湖檔不存在")
                    continue
                old = pl.read_parquet(path)
                if _verify_old_cols(new, old, f"{sym}/{tf}/{date_str}", failures):
                    out = _graft(old, new["true_pt_sum"].to_list(),
                                 new["dur_s"].to_list())
                else:
                    out = _graft(old, [None] * old.height, [None] * old.height)
                    n_nulled += 1
                if not dry_run:
                    _atomic_write(out, path)
                n_written += 1
            n_days += 1
            if i % 200 == 0:
                print(f"  [{sym}] {i}/{len(raw_files)} {date_str} "
                      f"({time.time()-t0:.0f}s)", flush=True)

        # ── 1d 年檔:重演 ETL append 語意(unique keep-last)後**逐列**嫁接 ──
        if oned_frames:
            byid = {}
            for fr in oned_frames:                    # 處理序 keep-last(= ETL append)
                for r in fr.to_dicts():
                    byid[(r["date"], r["session"])] = r
            for yf in yearly_files:
                old = pl.read_parquet(yf)
                if "true_pt_sum" in old.columns:
                    continue
                label = f"{sym}/1d/{os.path.basename(yf)}"
                pt, dur = [], []
                for r in old.to_dicts():
                    nr = byid.get((r["date"], r["session"]))
                    ok = nr is not None and all(
                        _col_match(c, [nr[c]], [r[c]])
                        for c in OLD_COLS
                        if c in r and c not in ("date", "session"))
                    if ok:
                        pt.append(nr["true_pt_sum"])
                        dur.append(nr["dur_s"])
                    else:
                        pt.append(None)
                        dur.append(None)
                        failures.append(f"{label}: 列 ({r['date']},{r['session']}) "
                                        f"{'無對應重算' if nr is None else '值不齊'} → null")
                        n_nulled += 1
                if not dry_run:
                    _atomic_write(_graft(old, pt, dur), yf)
                n_written += 1

    print(f"\n{'[DRY-RUN] ' if dry_run else ''}完成:{n_days} 天、寫入 {n_written} 檔、"
          f"跳過(已 12 欄){n_skipped} 天、null 嫁接 {n_nulled} 檔/列、"
          f"耗時 {time.time()-t0:.0f}s", flush=True)
    if failures:
        flog = os.path.join(DATA_ROOT, "kbars_backfill_pt_failures.txt")
        with open(flog, "w", encoding="utf-8") as fh:
            fh.write("\n".join(failures))
        print(f"⚠ {len(failures)} 個對不齊(舊值保留、pt=null;全清單 → {flog}):",
              flush=True)
        for f in failures[:60]:
            print("   ", f, flush=True)
        return 1
    print("✅ 全數對齊(pt 全填)", flush=True)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--symbol", choices=SYMBOLS)
    a = ap.parse_args()
    sys.exit(run([a.symbol] if a.symbol else SYMBOLS, a.dry_run))
