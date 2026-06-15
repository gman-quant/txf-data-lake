import os
import glob
import polars as pl
import sys

# 確保可以 import 專案內的模組
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATA_ROOT, TIMEFRAMES
from core.resampler import resample_to_kbars

RAW_TICKS_DIR = os.path.join(DATA_ROOT, "raw_ticks")
KBARS_DIR = os.path.join(DATA_ROOT, "kbars")

def main():
    print("=== True VWAP ETL Started ===")
    
    if not os.path.exists(RAW_TICKS_DIR):
        print(f"Error: {RAW_TICKS_DIR} 不存在。")
        return
        
    # 找尋所有的 symbol (例如 TXF, TXFR2)
    symbols = [d for d in os.listdir(RAW_TICKS_DIR) if os.path.isdir(os.path.join(RAW_TICKS_DIR, d))]
    print(f"發現 {len(symbols)} 個商品: {symbols}")
    
    for symbol in symbols:
        print(f"\n>> 正在處理商品: {symbol}")
        symbol_dir = os.path.join(RAW_TICKS_DIR, symbol)
        
        # 尋找該 symbol 下所有的 parquet 檔案
        files = glob.glob(os.path.join(symbol_dir, "**", "*.parquet"), recursive=True)
        if not files:
            print(f"  無 tick 檔案，跳過。")
            continue
            
        print(f"  找到 {len(files)} 個 Tick 檔案，準備展開全週期重鑄...")
        
        # 準備容器收集 1d 的日線資料 (因為 1d 是依年份存檔)
        annual_1d_dfs = {}
        
        for file_path in files:
            try:
                df_ticks = pl.read_parquet(file_path)
                if df_ticks.is_empty():
                    continue
                    
                # 從檔名解析出日期，如 2025-01-01
                filename = os.path.basename(file_path)
                date_str = filename.split("_")[0]
                year_str = date_str.split("-")[0]
                
                for tf in TIMEFRAMES:
                    # 呼叫系統共用的 resample_to_kbars (已內建 true_pv_sum 與 session 對齊邏輯)
                    df_kbar = resample_to_kbars(df_ticks, tf)
                    if df_kbar is None or df_kbar.is_empty():
                        continue
                        
                    # 決定儲存路徑
                    if tf == '1d':
                        # 收集 1d 資料，最後統一存成 {year} 檔
                        if year_str not in annual_1d_dfs:
                            annual_1d_dfs[year_str] = []
                        annual_1d_dfs[year_str].append(df_kbar)
                    else:
                        # 其它週期存為日檔: kbars/{tf}/{symbol}/{year}/{date}_{symbol}_{tf}.parquet
                        out_dir = os.path.join(KBARS_DIR, tf, symbol, year_str)
                        os.makedirs(out_dir, exist_ok=True)
                        out_file = os.path.join(out_dir, f"{date_str}_{symbol}_{tf}.parquet")
                        df_kbar.write_parquet(out_file)
                        
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
                
        # 最後將收集到的 1d 資料存成年度檔案
        if annual_1d_dfs:
            print(f"  正在寫入 1d 年檔...")
            for year_str, df_list in annual_1d_dfs.items():
                if df_list:
                    # 合併該年度所有的日線並依時間排序
                    df_year_1d = pl.concat(df_list).sort("ts")
                    out_dir = os.path.join(KBARS_DIR, '1d', symbol)
                    os.makedirs(out_dir, exist_ok=True)
                    out_file = os.path.join(out_dir, f"{symbol}_1d_{year_str}.parquet")
                    df_year_1d.write_parquet(out_file)
                    
    print("\n=== True VWAP ETL Completed! ===")

if __name__ == "__main__":
    main()
