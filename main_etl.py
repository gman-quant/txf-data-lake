# main_etl.py

import os
import argparse
from datetime import datetime
import polars as pl

# 引入我們寫好的模組
from config.settings import DATA_ROOT, TIMEFRAMES
from adapters.shioaji_source import ShioajiSource
from core.resampler import resample_to_kbars
from config.calendar_rules import DAY_START

# 定義目標商品清單
TARGET_SYMBOLS = ['TXF', 'TSE', 'TXFR2']


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

    try:
        # 確保連線 (ShioajiSource 內部有 check，重複呼叫 connect 沒成本)
        source.connect()

        for symbol in TARGET_SYMBOLS:
            print(f"\n------ Processing {symbol} ------")

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
                    continue
                downloaded = True

            # --- Phase 1.5: 根治週日檔/週日幻影列(週日請求→清空跳過;其餘日→丟週日幻影列)---
            before = len(tick_df)
            tick_df = _clean_sunday(tick_df, date_str)
            if tick_df.is_empty():
                print(f"⚠️  {symbol} {date_str}: 週日/無盤(清空),Skipping.")
                continue
            if before != len(tick_df):
                print(f"   🧹 丟掉 {before - len(tick_df)} 筆週日幻影列(保留 {len(tick_df)})")

            # --- Phase 2: Load Raw (存檔;只存下載來且已濾乾淨的) ---
            if downloaded:
                os.makedirs(raw_dir, exist_ok=True)
                tick_df.write_parquet(raw_path)
                print(f"✅ Raw Ticks downloaded & saved: {raw_path}")

            # --- Phase 3: Transform & Load K-Bars ---
            for tf in TIMEFRAMES:
                kbar_df = resample_to_kbars(tick_df, tf)
                
                if kbar_df.is_empty():
                    continue

                # [分流儲存策略] 根據週期決定儲存策略
                # Case A: 日線 (1d) -> 存成「年檔」，使用 Append 模式
                if tf == '1d':
                    kbar_dir = os.path.join(DATA_ROOT, "kbars", tf, symbol)
                    os.makedirs(kbar_dir, exist_ok=True)
                    
                    # 檔名: TXF_1d_2025.parquet
                    save_path = os.path.join(kbar_dir, f"{symbol}_{tf}_{year}.parquet")
                    
                    if os.path.exists(save_path):
                        # 讀取舊檔 -> 合併 -> 去重 -> 寫回
                        try:
                            existing_df = pl.read_parquet(save_path)
                            # 合併並以 ts 去重 (保留最新的)
                            final_df = pl.concat([existing_df, kbar_df]).unique(subset=["ts"], keep="last").sort("ts")
                        except Exception as e:
                            print(f"⚠️ Merge error, overwriting: {e}")
                            final_df = kbar_df
                    else:
                        final_df = kbar_df
                        
                    final_df.write_parquet(save_path)
                    print(f"   -> {tf} Updated: {save_path} (Total days: {len(final_df)//2})")

                # Case B: 分時/分秒 (1m, 5s...) -> 存成「日檔」，直接覆蓋
                else:
                    kbar_dir = os.path.join(DATA_ROOT, "kbars", tf, symbol, year)
                    os.makedirs(kbar_dir, exist_ok=True)
                    
                    save_path = os.path.join(kbar_dir, f"{date_str}_{symbol}_{tf}.parquet")
                    kbar_df.write_parquet(save_path)
                    print(f"   -> {tf} Saved: {save_path} ({len(kbar_df)} bars)")

    except Exception as e:
        print(f"❌ ETL Failed: {e}")
    finally:
        # 只有真正連線過才需要登出
        if is_local_session and source.is_connected:
            source.report_usage()
            source.logout()
            print("👋 Shioaji Logout.")
        else:
            print("🔄 Keeping connection alive for next batch...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TXF Data Lake ETL")
    default_date = datetime.now().strftime('%Y-%m-%d')
    parser.add_argument('--date', type=str, default=default_date, help='Format: YYYY-MM-DD')
    
    args = parser.parse_args()
    
    run_pipeline(args.date)