import os
import glob
import polars as pl
from config.settings import DATA_ROOT, TIMEFRAMES
from core.resampler import resample_to_kbars

def run_fix():
    print(f"🔄 Preparing to fix existing K-bars in: {DATA_ROOT}")
    
    # 決定要重算的週期 (排除 1d，因為 1d 不受動態群組平移影響)
    # 如果想連 1d 一起重算，可以把這行改成 targets = TIMEFRAMES
    targets = [tf for tf in TIMEFRAMES if tf != "1d"]
    print(f"🎯 Target timeframes for fix: {targets}")
    
    search_pattern = os.path.join(DATA_ROOT, "raw_ticks", "**", "*_ticks.parquet")
    raw_files = glob.glob(search_pattern, recursive=True)
    
    print(f"📦 Found {len(raw_files)} raw tick files. Starting process...\n")
    
    for count, raw_path in enumerate(raw_files, 1):
        filename = os.path.basename(raw_path)
        parts = filename.split('_')
        if len(parts) < 2: continue
        
        date_str = parts[0]
        symbol = parts[1]
        year = date_str[:4]
        
        print(f"[{count}/{len(raw_files)}] ⚙️ Processing {symbol} on {date_str}...")
        
        try:
            tick_df = pl.read_parquet(raw_path)
        except Exception as e:
            print(f"   ⚠️ Failed to read {raw_path}: {e}")
            continue
            
        for tf in targets:
            kbar_df = resample_to_kbars(tick_df, tf)
            if kbar_df.is_empty():
                continue
                
            # 分時線路徑邏輯 (同 main_etl.py)
            kbar_dir = os.path.join(DATA_ROOT, "kbars", tf, symbol, year)
            os.makedirs(kbar_dir, exist_ok=True)
            save_path = os.path.join(kbar_dir, f"{date_str}_{symbol}_{tf}.parquet")
            
            # 直接覆蓋舊有的檔案
            kbar_df.write_parquet(save_path)
            
    print("\n✅ All historical K-bars have been successfully fixed and overwritten.")

if __name__ == "__main__":
    run_fix()
