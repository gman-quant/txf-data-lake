# main_etl.py

import os
import argparse
import sys
import time
from datetime import datetime
import polars as pl

# 引入我們寫好的模組
from config.settings import DATA_ROOT, TIMEFRAMES
from adapters.shioaji_source import ShioajiSource
from core.resampler import resample_to_kbars
from lake_writer import save_kbars, atomic_write_parquet as _atomic_write_parquet

# 定義目標商品清單
TARGET_SYMBOLS = ['TXF', 'TSE', 'TXFR2']

# 單商品失敗的有界重試(暫時性失誤幾秒內就好;真的還沒有的資料靠 daily_sync 隔日自癒)
SYMBOL_TRIES = 3
SYMBOL_RETRY_WAIT = 20               # 秒;線性退避 20s、40s


# (_atomic_write_parquet 已搬進 lake_writer —— kbar 與 raw ticks 共用同一支,
#  原子性的理由與原文照搬,見該檔。)
def _clean_sunday(tick_df, date_str):
    """根治「週日檔 / 週日幻影列」,且**不破壞既有歸檔慣例、零資料遺失**。

    背景(慣例):本專案是「交易日」歸檔——檔 D = 前一晚夜盤(15:00→05:00)+ D 日盤(08:45–13:45)。
    Bug 來源:batch_run 連週日都請求(freq='D'),Shioaji 對非交易日**不回空、改回前一盤資料**,
    偶爾夾帶帶週日時間戳的幻影 tick;ETL 又用請求日當檔名 → 生出週日檔 + 幻影列。

    安全修法(只動週日、不碰夜盤歸屬):
      (1) **請求日是週日 → 直接清空跳過**(台指無任何週日盤,本不該有檔)→ 不再生週日檔。
      (2) 其餘日:**只丟掉「日曆日為週日」的列**(幻影;台指日盤/夜盤都不可能落在週日)。
          夜盤尾最遠到週六 05:00(週五夜盤),不會是週日 → 不誤砍。前夜盤+日盤全數保留,慣例不變。

    註:平日的「假日請求拿到前一盤」會生出「錯日期(平日名)」檔——那是另一個較小的議題,需配合
    交易日曆才能根治,不在本修法範圍(本修法專解你回報的『週日』問題,且保證不丟資料)。
    """
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    if target.weekday() == 6:                 # (1) 週日請求 → 清空(回同 schema 空表)→ 上層跳過
        return tick_df.clear()
    if tick_df.is_empty() or "ts" not in tick_df.columns:
        return tick_df
    return tick_df.filter(pl.col("ts").dt.weekday() != 7)   # (2) 丟掉日曆週日的幻影列(polars 週日=7)


def run_pipeline(date_str, shared_source=None):
    print(f"🚀 Starting ETL Pipeline for {date_str}...")
    
    # 🟢 [修改 2] 決定使用哪個 Source
    if shared_source is None:
        # 如果外部沒給，就自己建立一個 (單日模式)
        source = ShioajiSource()
        is_local_session = True # 標記這是自己建的，等下要負責關掉
    else:
        # 如果外部有給，就用外部的 (批次模式)
        source = shared_source
        is_local_session = False # 這是別人借我的，我不能關掉它

    year = date_str[:4]
    month = date_str[5:7]

    failed_symbols = []                  # 本次跑完仍失敗的商品(摘要與 exit code 用)

    try:
        # 確保連線 (ShioajiSource 內部有 check，重複呼叫 connect 沒成本)
        source.connect()

        for symbol in TARGET_SYMBOLS:
            print(f"\n------ Processing {symbol} ------")
            # 2026-07-22 事故:原本整個 for 迴圈包在**單一** try/except 裡,
            #   TSE 拋 KeyError('TSE001') -> 迴圈直接中斷,**排在後面的 TXFR2 也沒跑到**
            #   (一個商品的暫時性失敗賠掉兩個)。改為**每商品各自 try**:單一商品失敗
            #   只影響自己,其餘照跑;仍失敗者記入 failed_symbols,由 daily_sync 的
            #   per-symbol 缺口掃描在後續每天自動重試(自癒),不在這裡無限等。
            for attempt in range(1, SYMBOL_TRIES + 1):
                try:
                    _process_symbol(symbol, date_str, year, month, source)
                    break
                except Exception as e:
                    if attempt < SYMBOL_TRIES:
                        wait = SYMBOL_RETRY_WAIT * attempt      # 線性退避:20s、40s
                        print(f'[warn] {symbol} 第 {attempt} 次失敗:{e!r} -> {wait}s 後重試')
                        time.sleep(wait)
                    else:
                        print(f'[FAIL] {symbol} 失敗(重試 {SYMBOL_TRIES} 次):{e!r}')
                        failed_symbols.append(symbol)

    except Exception as e:
        print(f'[FAIL] ETL Failed: {e}')
        failed_symbols.append('(pipeline)')
    finally:
        # 只有真正連線過才需要登出
        if is_local_session and source.is_connected:
            source.report_usage()
            source.logout()
            print('Shioaji Logout.')
        else:
            print('Keeping connection alive for next batch...')

    if failed_symbols:
        # 大聲失敗:daily_sync 記 rc、SUMMARY 才看得見(舊版吞掉例外後 rc 仍是 0)
        print(f'[FAIL] [{date_str}] 未取得:' + ', '.join(failed_symbols) +
              ' -- 後續排程會自動重試(daily_sync per-symbol 缺口掃描)')
    return failed_symbols


def _process_symbol(symbol, date_str, year, month, source):
    """單一商品的 E-T-L(原 for 迴圈本體;抽出來才能逐商品 try/重試)。"""
    if True:
        if True:

            # 0. 預先計算 Raw Data 路徑
            raw_dir = os.path.join(DATA_ROOT, "raw_ticks", symbol, year, month)
            raw_path = os.path.join(raw_dir, f"{date_str}_{symbol}_ticks.parquet")
            
            tick_df = None
            downloaded = False

            # 檢查本地是否已有檔案
            if os.path.exists(raw_path):
                print(f"📦 Found local raw data: {raw_path}")
                print("   ⏩ Skipping download, loading from disk...")
                try:
                    tick_df = pl.read_parquet(raw_path)
                except Exception as e:
                    print(f"⚠️ Local file corrupted ({e}), forcing re-download.")

            # 如果本地沒檔案 (tick_df 還是 None)，才去網路下載
            if tick_df is None:
                # --- Phase 1: Extract (下載) ---
                tick_df = source.fetch_ticks(date_str, symbol)

                if tick_df.is_empty():
                    print(f"⚠️  No data found for {symbol} on {date_str}. Skipping.")
                    return          # 原為 for 迴圈內的 continue(本體已抽成函式)
                downloaded = True

            # --- Phase 1.5: 根治週日檔/週日幻影列(週日請求→清空跳過;其餘日→丟週日幻影列)---
            before = len(tick_df)
            tick_df = _clean_sunday(tick_df, date_str)
            if tick_df.is_empty():
                print(f"⚠️  {symbol} {date_str}: 週日/無盤(清空),Skipping.")
                return          # 原為 for 迴圈內的 continue(本體已抽成函式)
            if before != len(tick_df):
                print(f"   🧹 丟掉 {before - len(tick_df)} 筆週日幻影列(保留 {len(tick_df)})")

            # --- Phase 1.6: 幻影守衛(平日假日/颱風假等「非交易日」)---
            # Shioaji 對非交易日**不回空、回「上一個交易時段」的資料(帶舊日期)**。
            #   例:請求清明 4/6(Mon)→ 回 4/2 夜盤,資料最後一筆日期是 4/3。
            # 既有三道守衛都只防「週末」(resampler `date<6`、_clean_sunday 週日列、validate_lake ②),
            # 抓不到這種——因為幻影的內容日期是**合法平日**(4/3 週五)。這裡比對
            #   「資料最後一筆的日期 == 請求日」,不符即整批跳過(raw/kbar 都不存),
            # 自動涵蓋所有排定假日 + 臨時休市(颱風),**免維護交易日曆**。
            # (正常交易日:日盤收在請求日 13:45 → 日期一定 == 請求日,故不會誤殺。)
            req_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            data_date = tick_df.select(pl.col("ts").max().dt.date()).item()
            if data_date != req_date:
                print(f"⚠️  {symbol} {date_str}: 抓到的資料日期為 {data_date}(≠請求日)= 非交易日幻影,跳過不存。")
                return          # 原為 for 迴圈內的 continue(本體已抽成函式)

            # --- Phase 2: Load Raw (存檔;只存下載來且已濾乾淨的) ---
            if downloaded:
                os.makedirs(raw_dir, exist_ok=True)
                _atomic_write_parquet(tick_df, raw_path)
                print(f"✅ Raw Ticks downloaded & saved: {raw_path}")

            # --- Phase 3: Transform & Load K-Bars ---
            for tf in TIMEFRAMES:
                kbar_df = resample_to_kbars(tick_df, tf)
                
                if kbar_df.is_empty():
                    return          # 原為 for 迴圈內的 continue(本體已抽成函式)

                # 儲存:**單一出口 `lake_writer.save_kbars`**(2026-08-24)。
                # 舊版在這裡用 `if tf == '1d'` 自己分流年檔/日檔 —— 那是佈局的
                # 第二份實作,翻 `lake_paths.LAYOUT` 那天讀取端跟著走、這裡不會,
                # 六個 TF 的新棒會同時靜止而且三層偵測都看不到(稽核 blocker)。
                # 現在佈局/合併語意/原子寫全在 lake_writer;這裡只剩「叫它、報告」。
                saved = save_kbars(symbol, tf, date_str, kbar_df)
                if saved is None:
                    continue    # ❌ 已由 save_kbars 大聲說明(保住既有檔案)。
                                # ⚠ 舊版這裡是 `return` —— 只因 1d 排在 TIMEFRAMES
                                #   最後才恰好等價於 continue(抽函式時的位置巧合);
                                #   多日容器不再只有最後一個 TF 之後,continue 才對。
                if tf == "1d":
                    # 保留舊格式的行(Total days = 年檔列數/2:日+夜各一根)
                    import polars as _pl
                    _n = _pl.read_parquet(saved).height
                    print(f"   -> {tf} Updated: {saved} (Total days: {_n//2})")
                else:
                    print(f"   -> {tf} Saved: {saved} ({len(kbar_df)} bars)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TXF Data Lake ETL")
    default_date = datetime.now().strftime('%Y-%m-%d')
    parser.add_argument('--date', type=str, default=default_date, help='Format: YYYY-MM-DD')
    
    args = parser.parse_args()

    failed = run_pipeline(args.date)
    # rc != 0 才能讓 daily_sync 的 sync_state.json / SUMMARY 反映「有商品沒拿到」
    sys.exit(1 if failed else 0)