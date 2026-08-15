# -*- coding: utf-8 -*-
"""pt 回填例外的修復(2026-08-15 使用者三項裁決:①壞列重建 ②重抓 raw ③B 類重建)。

背景:`tools/backfill_pt_sum.py` 的驗證閘翻出 55 檔/列既有 raw↔kbars 不一致
(清單 D:/txf-data/kbars_backfill_pt_failures.txt),當時只標 null 不動值。
使用者裁決全部以「現行 raw 為準重建」,TXFR2 2025-01-06 先重抓 raw(現檔丟了週五夜盤)。

步驟(可分開跑):
    python -m tools.repair_pt_exceptions --refetch     # 只重抓 TXFR2 2025-01-06 raw(真 Shioaji 登入)
    python -m tools.repair_pt_exceptions --rebuild     # 只重建(要求 refetch 已成功或明示 --skip-0106)
    python -m tools.repair_pt_exceptions --refetch --rebuild   # 一氣呵成

安全設計:
    ‧ 動到的每個檔先備份到 D:/txf-data/repair_backup_20260815/(保相對路徑;已存在不覆蓋
      —— 保留「最初」狀態,重跑不會把備份洗成中間態)。
    ‧ 重抓的 raw 要通過三關才落地:非空、幻影守衛(末筆日期=請求日)、**涵蓋週五夜盤**
      (min(ts) ≤ 2025-01-03 16:00)。不過關 ⇒ 原檔不動、大聲報告。
    ‧ 重建 = resample_to_kbars(現行 12 欄版)整檔覆蓋 intraday + 1d 年檔逐列替換,
      每列/每檔印 old→new 差異摘要(審計軌跡)。
    ‧ 對「當初就通過驗證」的日子(2024-03-12 等)重建應位元同 —— 不同會印出來。
"""
import argparse
import glob
import os
import shutil
import sys
from datetime import datetime

import polars as pl

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import DATA_ROOT, TIMEFRAMES          # noqa: E402
from core.resampler import resample_to_kbars               # noqa: E402

BACKUP_ROOT = os.path.join(DATA_ROOT, "repair_backup_20260815")

# 修復清單(= 失敗清單的日子;2024-03-12 是為了 (2024-03-11, Night) 那列 —— D 日的
# Night 列住在 D+1 的檔裡;該日 intraday 當初全數通過,重建應位元同)。
REPAIR_DAYS = {
    "TXF": ["2021-09-29", "2021-09-30", "2024-03-11", "2024-03-12", "2026-03-24"],
    "TXFR2": ["2020-08-28", "2020-08-31", "2022-11-14", "2023-12-05", "2023-12-18",
              "2024-04-01", "2024-07-15", "2024-08-14", "2024-08-15", "2024-08-29",
              "2024-11-22", "2025-01-06"],
    "TSE": ["2020-03-23", "2020-03-24", "2024-06-20"],
}
REFETCH_DAY = "2025-01-06"
REFETCH_SYM = "TXFR2"
# 涵蓋判準:2025-01-06(一)的檔必須含 2025-01-03(五)夜盤 15:00 起的資料
NIGHT_COVER_BEFORE = datetime(2025, 1, 3, 16, 0)


def _raw_path(sym, d):
    return os.path.join(DATA_ROOT, "raw_ticks", sym, d[:4], d[5:7],
                        f"{d}_{sym}_ticks.parquet")


def _backup(path):
    """首次備份(已存在不覆蓋 —— 保留最初狀態)。"""
    rel = os.path.relpath(path, DATA_ROOT)
    dst = os.path.join(BACKUP_ROOT, rel)
    if not os.path.exists(dst):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(path, dst)


def _atomic_write(df, path):
    tmp = f"{path}.tmp{os.getpid()}"
    df.write_parquet(tmp)
    os.replace(tmp, path)


def do_refetch() -> bool:
    """重抓 TXFR2 2025-01-06 raw。回傳是否成功落地。"""
    from adapters.shioaji_source import ShioajiSource
    raw_path = _raw_path(REFETCH_SYM, REFETCH_DAY)
    old = pl.read_parquet(raw_path)
    print(f"[refetch] 現檔:{old.height} 筆,{old['ts'].min()} → {old['ts'].max()}")

    src = ShioajiSource()
    try:
        df = src.fetch_ticks(REFETCH_DAY, REFETCH_SYM)
    finally:
        try:
            src.report_usage()
            src.logout()
        except Exception:
            pass

    if df.is_empty():
        print("[refetch] ❌ 抓回空表 —— 原檔不動。")
        return False
    # 週日清洗(鏡射 main_etl Phase 1.5;平日請求只丟週日幻影列)
    from main_etl import _clean_sunday
    df = _clean_sunday(df, REFETCH_DAY)
    if df.is_empty():
        print("[refetch] ❌ 清洗後空 —— 原檔不動。")
        return False
    # 幻影守衛(鏡射 Phase 1.6)
    data_date = df.select(pl.col("ts").max().dt.date()).item()
    if str(data_date) != REFETCH_DAY:
        print(f"[refetch] ❌ 末筆日期 {data_date} ≠ 請求日 —— 幻影,原檔不動。")
        return False
    tmin, tmax = df["ts"].min(), df["ts"].max()
    print(f"[refetch] 新抓:{df.height} 筆,{tmin} → {tmax}")
    if tmin > NIGHT_COVER_BEFORE:
        print("[refetch] ❌ 仍缺週五夜盤(min(ts) 不在 2025-01-03 16:00 之前)—— 原檔不動。"
              " 舊 kbars 的夜盤棒維持保留(pt=null)。")
        return False
    if df.height < old.height:
        print(f"[refetch] ❌ 新檔筆數 {df.height} < 現檔 {old.height} —— 反而變少,原檔不動。")
        return False
    _backup(raw_path)
    _atomic_write(df, raw_path)
    print(f"[refetch] ✅ raw 已更新(備份於 {BACKUP_ROOT})")
    return True


def _summarize_bar_diff(label, old_df, new_df):
    """審計軌跡:根數與值差摘要(不擋,只印)。"""
    msg = [f"  {label}: {old_df.height} 根 → {new_df.height} 根"]
    if old_df.height == new_df.height and old_df.height:
        n_diff = {}
        for c in ("open", "high", "low", "close", "volume"):
            if c in old_df.columns and c in new_df.columns:
                a, b = old_df[c].to_list(), new_df[c].to_list()
                d = sum(1 for x, y in zip(a, b) if x != y)
                if d:
                    n_diff[c] = d
        msg.append(f"值變動 {n_diff if n_diff else '無(位元同)'}")
    print(" ".join(msg), flush=True)


def do_rebuild(skip_0106: bool) -> int:
    n_files = 0
    for sym, days in REPAIR_DAYS.items():
        oned_rows = {}                        # (date, session) → row(重算)
        for d in days:
            if skip_0106 and sym == REFETCH_SYM and d == REFETCH_DAY:
                print(f"[rebuild] ⏭ 跳過 {sym} {d}(--skip-0106)")
                continue
            rp = _raw_path(sym, d)
            if not os.path.exists(rp):
                print(f"[rebuild] ❌ raw 不存在:{rp}")
                return 1
            ticks = pl.read_parquet(rp)
            for tf in TIMEFRAMES:
                new = resample_to_kbars(ticks, tf)
                if tf == "1d":
                    for r in new.to_dicts():
                        oned_rows[(r["date"], r["session"])] = r
                    continue
                kp = os.path.join(DATA_ROOT, "kbars", tf, sym, d[:4],
                                  f"{d}_{sym}_{tf}.parquet")
                if not os.path.exists(kp):
                    print(f"[rebuild] ❌ 湖檔不存在:{kp}")
                    return 1
                old = pl.read_parquet(kp)
                _summarize_bar_diff(f"{sym}/{tf}/{d}", old, new)
                _backup(kp)
                _atomic_write(new, kp)
                n_files += 1

        # 1d 年檔:逐列替換(只動修復日產出的身分;其餘列原封不動)
        by_year = {}
        for (dt_, sess), r in oned_rows.items():
            by_year.setdefault(dt_.year, {})[(dt_, sess)] = r
        for yr, rows in sorted(by_year.items()):
            yf = os.path.join(DATA_ROOT, "kbars", "1d", sym, f"{sym}_1d_{yr}.parquet")
            if not os.path.exists(yf):
                print(f"[rebuild] ❌ 年檔不存在:{yf}")
                return 1
            old = pl.read_parquet(yf)
            olds = old.to_dicts()
            have = {(r["date"], r["session"]) for r in olds}
            out = []
            for r in olds:
                key = (r["date"], r["session"])
                if key in rows:
                    nr = rows.pop(key)
                    ch = {c: (r.get(c), nr.get(c)) for c in
                          ("open", "high", "low", "close", "volume")
                          if r.get(c) != nr.get(c)}
                    print(f"  {sym}/1d/{yr} 列 {key}: "
                          f"{'替換 ' + str(ch) if ch else '值同(補 pt)'}", flush=True)
                    out.append(nr)
                else:
                    out.append(r)
            for key, nr in rows.items():      # 年檔原本沒有的身分(如 2025-01-03 Night)
                if key[0].year != yr:
                    continue                  # 跨年列歸屬各自年檔(下輪處理)
                print(f"  {sym}/1d/{yr} 列 {key}: **新增**(原檔缺列)", flush=True)
                out.append(nr)
            new = pl.DataFrame(out).sort("ts")
            # dtype 對齊舊檔(ts ns 等;逐欄 cast 回舊 schema)
            for c, dt in zip(old.columns, old.dtypes):
                if c in new.columns:
                    new = new.with_columns(pl.col(c).cast(dt))
            new = new.select([c for c in old.columns if c in new.columns]
                             + [c for c in new.columns if c not in old.columns])
            _backup(yf)
            _atomic_write(new, yf)
            n_files += 1
    print(f"\n[rebuild] 完成,寫入 {n_files} 檔(備份於 {BACKUP_ROOT})", flush=True)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--refetch", action="store_true")
    ap.add_argument("--rebuild", action="store_true")
    ap.add_argument("--skip-0106", action="store_true",
                    help="refetch 失敗時仍重建其餘日子(2025-01-06 保持 null)")
    a = ap.parse_args()
    if not (a.refetch or a.rebuild):
        ap.error("至少要 --refetch 或 --rebuild")
    ok_0106 = None
    if a.refetch:
        ok_0106 = do_refetch()
        if not ok_0106 and not a.rebuild:
            sys.exit(1)
    if a.rebuild:
        skip = (ok_0106 is False) or a.skip_0106
        if ok_0106 is None and not a.skip_0106:
            # 沒跑 refetch 就 rebuild:檢查現檔是否已含夜盤,否則要求明示
            cur = pl.read_parquet(_raw_path(REFETCH_SYM, REFETCH_DAY))
            if cur["ts"].min() > NIGHT_COVER_BEFORE:
                print("[rebuild] 2025-01-06 raw 仍缺夜盤:先 --refetch,或 --skip-0106 明示跳過。")
                sys.exit(1)
        sys.exit(do_rebuild(skip))
