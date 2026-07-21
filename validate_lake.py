# validate_lake.py
"""收盤資料驗證器 —— 保證每天 sync 寫入的資料都符合既有格式與內容約束。

用途:
  - sync 跑完接著跑 `python validate_lake.py`(預設驗「今天」寫入的檔),
    通過印 ✅ PASS;有問題印 ❌ 並逐檔列出原因,當下就攔下髒資料,
    不會像「週末測試盤/週日幻影」那樣累積數年才發現。
  - 一次性全庫體檢:`python validate_lake.py --all`。

檢查項目(對每個 kbar 檔):
  ① schema:必含 [symbol, date, ts, session, open, high, low, close, volume]
  ② 交易日:date 欄不得為週末(週六/週日)—— 真實交易日永遠是週一~五
  ③ 盤別:session 標記需與 ts 時間一致(Day ⇔ 08:30 ≤ t < 13:45:05)
  ④ 量:volume 必須 > 0
  ⑤ 排序:ts 嚴格遞增、無重複
"""

import os
import sys
import glob
import argparse
from datetime import datetime, time

# Windows 主控台預設 cp950,確保 emoji / 中文輸出不爆 UnicodeEncodeError
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import polars as pl

from config.settings import DATA_ROOT, TIMEFRAMES
from config.calendar_rules import DAY_START, DAY_END

TARGET_SYMBOLS = ["TXF", "TSE", "TXFR2"]
REQUIRED_COLS = ["symbol", "date", "ts", "session", "open", "high", "low", "close", "volume"]


def validate_file(path: str) -> list[str]:
    """驗證單一 parquet 檔,回傳問題訊息清單(空 = 通過)。"""
    issues = []
    try:
        df = pl.read_parquet(path)
    except Exception as e:
        return [f"讀檔失敗:{e}"]

    if df.is_empty():
        return ["空檔(0 列)"]

    # ① schema
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        issues.append(f"缺欄位 {missing}")
        return issues  # 缺核心欄位,後續檢查無意義

    # ② 交易日不得為週末(polars weekday: 一=1 … 六=6, 日=7)
    if df["date"].dtype in (pl.Datetime, pl.Date):
        n_wknd = int(df["date"].dt.weekday().is_in([6, 7]).sum())
        if n_wknd:
            bad = df.filter(pl.col("date").dt.weekday().is_in([6, 7]))["date"].unique().to_list()
            issues.append(f"date=週末 {n_wknd} 列(日期:{[str(d)[:10] for d in bad]})")
    else:
        issues.append(f"date 欄型別非日期:{df['date'].dtype}")

    # ③ session 與 ts 時間一致性
    if df["ts"].dtype == pl.Datetime:
        t = pl.col("ts").dt.time()
        is_day_time = (t >= DAY_START) & (t < DAY_END)
        mism = df.filter(
            ((pl.col("session") == "Day") & ~is_day_time)
            | ((pl.col("session") == "Night") & is_day_time)
        )
        if len(mism):
            issues.append(f"session 與 ts 時間不一致 {len(mism)} 列")

    # ④ volume
    n_zero = int((df["volume"] <= 0).sum())
    if n_zero:
        issues.append(f"volume<=0 {n_zero} 列")

    # ⑤ ts 嚴格遞增、無重複
    n_dup = len(df) - df["ts"].n_unique()
    if n_dup:
        issues.append(f"ts 重複 {n_dup} 列")
    if not df["ts"].is_sorted():
        issues.append("ts 未排序")

    return issues


def _files_for_date(date_str: str) -> list[str]:
    """某交易日當天 sync 會寫入/更新的 kbar 檔(分時日檔 + 1d 年檔)。"""
    year = date_str[:4]
    paths = []
    for sym in TARGET_SYMBOLS:
        for tf in TIMEFRAMES:
            if tf == "1d":
                p = os.path.join(DATA_ROOT, "kbars", tf, sym, f"{sym}_{tf}_{year}.parquet")
            else:
                p = os.path.join(DATA_ROOT, "kbars", tf, sym, year, f"{date_str}_{sym}_{tf}.parquet")
            if os.path.exists(p):
                paths.append(p)
    return paths


def _all_kbar_files() -> list[str]:
    return [
        os.path.normpath(p)
        for p in glob.glob(os.path.join(DATA_ROOT, "kbars", "*", "*", "*", "*.parquet"))
        + glob.glob(os.path.join(DATA_ROOT, "kbars", "1d", "*", "*.parquet"))
        if "_sunday_backup" not in p and "_backup" not in p
    ]


def main():
    parser = argparse.ArgumentParser(description="TXF Data Lake 驗證器")
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"),
                        help="驗證此交易日寫入的檔(預設今天)。格式 YYYY-MM-DD")
    parser.add_argument("--all", action="store_true", help="改為全庫體檢(忽略 --date)")
    args = parser.parse_args()

    if args.all:
        files = _all_kbar_files()
        scope = f"全庫 {len(files)} 檔"
    else:
        files = _files_for_date(args.date)
        scope = f"{args.date} 寫入的 {len(files)} 檔"

    print(f"🔍 驗證範圍:{scope}")
    if not files:
        print("⚠️  找不到檔案(該日可能無盤/未 sync)。")
        return 0

    failed = 0
    for p in sorted(files):
        issues = validate_file(p)
        if issues:
            failed += 1
            rel = os.path.relpath(p, DATA_ROOT)
            print(f"❌ {rel}")
            for it in issues:
                print(f"      - {it}")

    print("-" * 60)
    if failed == 0:
        print(f"✅ PASS:{len(files)} 檔全數符合格式與內容約束。")
        return 0
    print(f"❌ FAIL:{failed}/{len(files)} 檔有問題(見上)。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
