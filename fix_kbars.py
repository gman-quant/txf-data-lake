import os
import glob
import polars as pl
from config.settings import DATA_ROOT, TIMEFRAMES
from core.resampler import resample_to_kbars
from lake_writer import save_kbars

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
                
            # 2026-08-24:改走 lake_writer 單一出口(同 main_etl)。
            # 順帶把舊的**非原子** write_parquet 換成原子寫 —— 重建跑到一半被砍,
            # 留半成品在湖裡與 main_etl 當年的資料遺失鏈是同一族。
            # ⚠ 多日容器下本工具是逐日 merge,不適合全量重建(殘影問題,
            #   見 lake_writer 檔頭)—— 全量重建照慣例刪 cache 重建。
            save_kbars(symbol, tf, date_str, kbar_df)
            
    print("\n✅ All historical K-bars have been successfully fixed and overwritten.")

if __name__ == "__main__":
    run_fix()
