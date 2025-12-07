# core/loader.py
import os
import pandas as pd
import polars as pl
from config.settings import DATA_ROOT
from datetime import datetime

class DataLoader:
    """
    負責從 Data Lake (Parquet) 讀取原始 K 棒資料
    """
    @staticmethod
    def load_kbars(symbol: str, timeframe: str, start_date: str, end_date: str) -> pl.DataFrame:
        target_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df_list = []
        
        # 1. 根據週期決定讀取策略 (年檔 vs 日檔)
        if timeframe == '1d':
            years = sorted(list(set([d.strftime('%Y') for d in target_dates])))
            for year in years:
                path = os.path.join(DATA_ROOT, "kbars", timeframe, symbol, f"{symbol}_{timeframe}_{year}.parquet")
                if os.path.exists(path): df_list.append(pl.read_parquet(path))
        else:
            for dt in target_dates:
                d_str, year = dt.strftime('%Y-%m-%d'), dt.strftime('%Y')
                path = os.path.join(DATA_ROOT, "kbars", timeframe, symbol, year, f"{d_str}_{symbol}_{timeframe}.parquet")
                if os.path.exists(path): df_list.append(pl.read_parquet(path))

        if not df_list:
            return pl.DataFrame()

        # 2. 合併與初步過濾 (處理跨日重複)
        df = pl.concat(df_list).unique(subset=["ts"], keep="last").sort("ts")
        
        # 日線需額外過濾日期區間 (因為年檔包含整年)
        if timeframe == '1d':
            s_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            e_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            df = df.filter((pl.col("date") >= s_dt) & (pl.col("date") <= e_dt))
            
        return df