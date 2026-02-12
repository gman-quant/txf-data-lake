# batch_run.py
import pandas as pd
from datetime import datetime, timedelta
from adapters.shioaji_source import ShioajiSource
from main_etl import run_pipeline

def run_batch_job(start_date, end_date):
    print(f"📆 Batch Job: {start_date} to {end_date}")
    
    # 1. 建立一次連線 (Singleton)
    source = ShioajiSource()
    source.connect() # 這裡登入一次
    
    # 2. 產生日期列表 (排除週末)
    # 改用 'D' (Daily)，包含週六週日
    # 雖然會多跑很多天 "No data found"，但能確保抓到 "週六補班日"
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    print(f"🎯 Total {len(dates)} trading days to process.")
    
    try:
        for dt in dates:
            date_str = dt.strftime('%Y-%m-%d')
            
            # 3. 呼叫 ETL，並把 source 傳進去
            # 這樣 main_etl 就不會執行 logout
            print(f"\n>>> Processing: {date_str}")
            run_pipeline(date_str, shared_source=source)
            
    except KeyboardInterrupt:
        print("\n🛑 Batch job interrupted by user.")
        
    finally:
        # 4. 全部跑完後，才執行最後一次登出
        print("\n🎉 Batch Job Completed. Logging out...")
        source.report_usage()
        source.logout()

if __name__ == "__main__":
    # 設定您要補資料的區間
    START = "2026-02-06"
    END   = "2026-02-06"
    
    run_batch_job(START, END)