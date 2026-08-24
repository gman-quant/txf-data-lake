# -*- coding: utf-8 -*-
"""kbar 的**唯一寫入出口**(2026-08-24,產品碼稽核 blocker 的修法)。

## 為什麼需要這個模組

`lake_paths.LAYOUT` 自稱「佈局的唯一真相(讀者與寫者共用)」,但在本模組出現之前
**寫入端從來沒接上它**:`main_etl` 用自己的 `if tf == '1d'` 分流(佈局的第二份實作),
`fix_kbars` 再抄一份。翻 `LAYOUT` 那天(Phase F,`tools/compact_kbars.py` 是已寫好的
搬運工):讀取端立刻改看新佈局,寫入端照舊寫舊位置 ⇒ **六個 TF 的新棒同時靜止**,
而三層偵測(validate_lake 同款寫死 / `_check_completeness` 手拼 / daily_sync 的
`os.walk` 佈局盲)全部一起說 PASS。零例外、零警告。

## 兩件事,缺一不可

**① 目標路徑問 `kbar_paths(existing_only=False)`** —— 單日在任何佈局都恰好一個
容器路徑。本模組**不知道**佈局長什麼樣,只知道「把這一天交給哪個檔」。

**② 合併語意跟著容器走** —— 這是「只換路徑函式就收工」的陷阱(稽核的驗證者抓過
這個提案錯誤):daily 容器一檔一日,整份覆寫=正確;yearly / monthly 容器是
**累積型**,整份覆寫會讓一個月只剩最後一個交易日。多日容器一律 read-merge-write。

## 棒的身分鍵(`DEDUP_KEY`)

  1d    (date, session)   盤段棒:ts = 該盤**第一筆 tick** 時間,重抓會差幾毫秒
                          ⇒ 用 ts 當鍵會把同一根認成兩根(2026-07 的 1d 重複列 bug)
  其餘  ts                重採樣的桶邊界,穩定,就是身分

⚠ 這**不是**佈局借代:佈局歸 `LAYOUT` 管、身分歸這張表管,兩者獨立
  (1d 今天恰好也是唯一的 yearly,但那是巧合 —— Phase F 之後全部變 monthly,
   身分鍵不跟著變)。

## 重建工具(fix_kbars 之類)的注意事項

merge 是 `keep="last"`:同鍵新列蓋舊列,**但新資料裡不存在的舊列會留著**。
單日重寫在 daily 容器下等於全量替換;在多日容器下若重算後某根棒消失了
(例:resampler 修 bug 後少一根),殘影會留在容器裡。**全量重建照既有慣例:
刪掉 cache 重建(實測 6 分鐘),不要靠逐日 merge 收斂。**
"""
from __future__ import annotations

import os

import polars as pl

from config.lake_paths import kbar_paths, layout_of

#: 棒的身分鍵(去重用)。理由見檔頭 —— 這張表管「身分」,LAYOUT 管「佈局」,別合併。
DEDUP_KEY = {"1d": ["date", "session"]}


def atomic_write_parquet(df, path):
    """原子寫入:先寫同目錄的暫存檔,再 `os.replace` 換上去。

    為什麼(2026-07-21 加,原住 main_etl):`df.write_parquet(path)` 直接寫目標檔,
    行程若在寫到一半被中斷(斷電、被砍、磁碟滿),留下的是**毀損的半成品**。
    對多日容器(年檔/月檔)尤其致命 —— 下次執行讀不動它,整個容器的歷史都要救。
    同一檔案系統上的 rename 是原子的:要嘛看到舊檔、要嘛看到完整新檔。
    """
    tmp = f"{path}.tmp{os.getpid()}"
    try:
        df.write_parquet(tmp)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def save_kbars(symbol: str, tf: str, date_str, kbar_df, log=print):
    """把「一個交易日 × 一個 TF」的 kbar 寫進湖。回傳寫入路徑;
    容器讀取失敗回 `None`(**保住既有檔案**,呼叫端跳過這一筆繼續別的)。

    佈局由 `kbar_paths` 決定(本函式沒有任何 `if tf == ...` 的路徑分支);
    合併與否由 `layout_of` 決定(daily=整份覆寫、多日容器=read-merge-write)。
    """
    target = kbar_paths(tf, symbol, date_str, date_str, existing_only=False)[0]
    os.makedirs(os.path.dirname(target), exist_ok=True)

    final = kbar_df
    if layout_of(tf) != "daily" and os.path.exists(target):
        try:
            existing = pl.read_parquet(target)
        except Exception as e:
            # ⚠️ 2026-07-21 修正資料遺失鏈(原住 main_etl 的 Case A,語意原樣搬):
            #    這裡若改成「用新資料覆寫」,一次讀取失敗就賠掉整個容器的歷史。
            #    保住既有檔案、跳過本次更新、用 ❌ 大聲報(❌ 是 daily_sync Tee 的
            #    錯誤標記,會浮到 [SUMMARY])。不 raise:爆炸半徑要停在這一筆。
            log(f"❌ {tf} 容器讀取失敗,已跳過本次更新以保住既有資料")
            log(f"   檔案:{target}")
            log(f"   原因:{type(e).__name__}: {e}")
            log(f"   影響:{symbol} 的 {tf} 不更新(其他 TF 與其他商品不受影響);")
            log(f"        修好該檔前每天都會重複此錯誤 —— 這是刻意的,別忽略。")
            return None
        key = DEDUP_KEY.get(tf, ["ts"])
        final = (pl.concat([existing, kbar_df])
                   .unique(subset=key, keep="last")
                   .sort("ts"))

    atomic_write_parquet(final, target)
    return target
