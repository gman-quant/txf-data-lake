"""
期交所每日行情(TX)→ spread/1d 官方日線價差
==================================================================
來源:`D:/txf-data/adjustments/taifex_raw/{year}_fut.csv`(big5,期交所每日行情全表)
      —— 這些檔本來只是 settlement_registry 抓結算價的副產品,但它們其實含
      **六年份、每個合約、每日的官方結算價/收盤/最後最佳買賣價**,
      而且「到期月份」欄同時有單式(`202601`)與**價差組合商品**(`202601/202602`)。

⇒ 跨月價差的歷史,在日線層級是**官方完整**的(含真實買賣價差),不需要自己估。
   (只有**日內**的 bid/ask 才真的不可回溯。)

2025-12-31 交叉驗算(建立此檔的依據):
    單式買賣中價之差 = 29092.5 − 29017.0 = 75.5
    價差商品中價     = (74 + 77)/2       = 75.5   ✅ 完全一致
    價差商品買賣價差 = 3 點  vs 單式兩腿加總 4+7 = 11 點 → 組合簿遠緊於兩腿合成

⚠ 三種口徑差 6~9 點,**回答不同問題,不可互相取代**:
    cs_settle  結算價之差       → 官方估值。研究水位/季節性/多年漂移用這個
    cs_combo   價差商品中價     → 真實可成交。研究摩擦/可交易性用這個
    cs_mid     單式中價之差     → 我們自己算得出來的,當驗證錨(應 ≈ cs_combo)

用法:
    python parse_taifex_daily.py            # 全部年份
    python parse_taifex_daily.py --year 2025
"""

import argparse
import glob
import os
import re
import sys

import polars as pl

from config.settings import DATA_ROOT

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

RAW_DIR = os.path.join(DATA_ROOT, "adjustments", "taifex_raw")
OUT_DIR = os.path.join(DATA_ROOT, "spread", "1d")

# big5 解碼後的欄位順序(2020–2026 一致,已實查)
C_DATE, C_PROD, C_MONTH = 0, 1, 2
C_CLOSE, C_VOL, C_SETTLE, C_OI, C_BID, C_ASK, C_SESSION = 6, 9, 10, 11, 12, 13, 17

_MONTH_RE = re.compile(r"^\d{6}$")


def _num(col: str) -> pl.Expr:
    """期交所用 '-' 表示無值;逗號千分位也要清掉。"""
    return (pl.col(col).str.strip_chars().str.replace_all(",", "")
            .replace("-", None).cast(pl.Float64, strict=False))


def _settle(col: str) -> pl.Expr:
    """結算價專用:**結算日當天,已到期的近月結算價欄是 `0` 而不是 `-`**。
    價格 0 在期貨不可能,不擋掉會算出 `46066 − 0 = 46066` 這種垃圾價差
    (2026-07-15 實例)。→ 0 一律視為無值,該日 cs_settle 自然變 null,
    誠實反映「結算日的近月結算價不可與次月相比」。cs_close / cs_mid / cs_combo 不受影響。
    """
    return pl.when(_num(col) == 0).then(None).otherwise(_num(col))


def load_year(path: str) -> pl.DataFrame:
    d = pl.read_csv(path, encoding="big5", ignore_errors=True,
                    truncate_ragged_lines=True, infer_schema_length=0)
    c = d.columns
    d = d.filter(pl.col(c[C_PROD]).str.strip_chars() == "TX")
    return d.select([
        pl.col(c[C_DATE]).str.strip_chars().str.replace_all("/", "-").str.to_date("%Y-%m-%d").alias("date"),
        pl.col(c[C_MONTH]).str.strip_chars().alias("m"),
        pl.col(c[C_SESSION]).str.strip_chars().alias("ses"),
        _num(c[C_CLOSE]).alias("close"), _settle(c[C_SETTLE]).alias("settle"),
        _num(c[C_BID]).alias("bid"), _num(c[C_ASK]).alias("ask"),
        _num(c[C_VOL]).alias("vol"), _num(c[C_OI]).alias("oi"),
    ])


def build(df: pl.DataFrame) -> pl.DataFrame:
    single = df.filter(pl.col("m").str.contains(r"^\d{6}$"))
    combo = df.filter(pl.col("m").str.contains("/"))

    rows = []
    for (date, ses), g in single.group_by(["date", "ses"], maintain_order=True):
        months = sorted(g["m"].unique().to_list())
        if len(months) < 2:
            continue
        r1m, r2m = months[0], months[1]          # 近月 / 次月 = 到期月份最小的兩個
        a = g.filter(pl.col("m") == r1m).row(0, named=True)
        b = g.filter(pl.col("m") == r2m).row(0, named=True)

        cmb = combo.filter((pl.col("date") == date) & (pl.col("ses") == ses)
                           & (pl.col("m") == f"{r1m}/{r2m}"))
        cb = cmb.row(0, named=True) if cmb.height else {}

        def mid(x, y):
            return (x + y) / 2 if (x is not None and y is not None) else None

        m_out1, m_out2 = mid(a["bid"], a["ask"]), mid(b["bid"], b["ask"])
        rows.append({
            "date": date, "session": "Day" if ses == "一般" else "Night",
            "r1_contract": r1m, "r2_contract": r2m,
            "r1_settle": a["settle"], "r2_settle": b["settle"],
            "r1_close": a["close"], "r2_close": b["close"],
            "r1_bid": a["bid"], "r1_ask": a["ask"], "r1_vol": a["vol"], "r1_oi": a["oi"],
            "r2_bid": b["bid"], "r2_ask": b["ask"], "r2_vol": b["vol"], "r2_oi": b["oi"],
            # 三種口徑(見檔頭:回答不同問題,不可互相取代)
            "cs_settle": (b["settle"] - a["settle"]) if (a["settle"] is not None and b["settle"] is not None) else None,
            "cs_close": (b["close"] - a["close"]) if (a["close"] is not None and b["close"] is not None) else None,
            "cs_mid": (m_out2 - m_out1) if (m_out1 is not None and m_out2 is not None) else None,
            # 價差組合商品**自己的**行情 —— 我們自己怎麼算都算不出來的東西
            "combo_close": cb.get("close"), "combo_bid": cb.get("bid"),
            "combo_ask": cb.get("ask"), "combo_vol": cb.get("vol"),
        })

    out = pl.DataFrame(rows).sort(["date", "session"])
    return out.with_columns([
        ((pl.col("combo_bid") + pl.col("combo_ask")) / 2).alias("cs_combo"),
        (pl.col("combo_ask") - pl.col("combo_bid")).alias("combo_spread"),
        (pl.col("r1_ask") - pl.col("r1_bid")).alias("r1_spread"),
        (pl.col("r2_ask") - pl.col("r2_bid")).alias("r2_spread"),
    ]).with_columns(
        # 資料品質檢查欄:單式中價之差 應 ≈ 價差商品中價。差太多 = 口徑或解析出問題。
        (pl.col("cs_mid") - pl.col("cs_combo")).abs().alias("qc_mid_vs_combo")
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", help="只跑單一年份")
    args = ap.parse_args()

    files = sorted(glob.glob(os.path.join(RAW_DIR, "*_fut.csv")))
    by_year = {}
    for f in files:
        name = os.path.basename(f)
        y = name[:4]
        if args.year and y != args.year:
            continue
        by_year.setdefault(y, []).append(f)

    os.makedirs(OUT_DIR, exist_ok=True)
    for y, fs in sorted(by_year.items()):
        df = pl.concat([load_year(f) for f in fs], how="diagonal")
        out = build(df)
        p = os.path.join(OUT_DIR, f"{y}_spread_1d.parquet")
        out.write_parquet(p, compression="zstd")
        day = out.filter(pl.col("session") == "Day")
        def med(col):
            s2 = day[col].drop_nulls()
            return f"{s2.median():7.1f}" if s2.len() else "      -"
        qc = day["qc_mid_vs_combo"].drop_nulls()
        print(f"  {y}  {out.height:>5} 列(日盤 {day.height:>3})  {os.path.getsize(p)/1e3:>5.0f} KB"
              f"  | cs_settle {med('cs_settle')}  cs_combo {med('cs_combo')}"
              f"  combo價差 {med('combo_spread')}"
              f"  | QC 中位 {qc.median() if qc.len() else float('nan'):.2f}"
              f" p95 {qc.quantile(.95) if qc.len() else float('nan'):.2f}"
              f"  非null cs_settle {day['cs_settle'].drop_nulls().len()}/{day.height}")


if __name__ == "__main__":
    main()
