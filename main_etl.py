# main_etl.py

import os
import argparse
from datetime import datetime
import polars as pl

# 引入我們寫好的模組
from config.settings import DATA_ROOT, TIMEFRAMES
from adapters.shioaji_source import ShioajiSource
from core.resampler import resample_to_kbars

# 定義目標商品清單
TARGET_SYMBOLS = ['TXF', 'TSE', 'TXFR2']

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

                # --- Phase 2: Load Raw (存檔) ---
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