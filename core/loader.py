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
        from config.settings import TIMEFRAMES
        from core.resampler import resample_kbars
        
        # 決定物理檔案讀取的週期：若要求的週期不在預設產生的列表中，尋找適合的底層資料
        base_tf = timeframe
        if timeframe not in TIMEFRAMES:
            if timeframe.endswith('h') and '1h' in TIMEFRAMES:
                base_tf = '1h'
            elif timeframe.endswith('m') and '1m' in TIMEFRAMES:
                base_tf = '1m'
            elif (timeframe.endswith('w') or timeframe.endswith('d')) and '1d' in TIMEFRAMES:
                base_tf = '1d'
            else:
                base_tf = '1m' # Fallback
        
        target_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        df_list = []
        
        # 1. 根據週期決定讀取策略 (年檔 vs 日檔) 使用 base_tf
        if base_tf == '1d':
            years = sorted(list(set([d.strftime('%Y') for d in target_dates])))
            for year in years:
                path = os.path.join(DATA_ROOT, "kbars", base_tf, symbol, f"{symbol}_{base_tf}_{year}.parquet")
                if os.path.exists(path): df_list.append(pl.read_parquet(path))
        else:
            for dt in target_dates:
                d_str, year = dt.strftime('%Y-%m-%d'), dt.strftime('%Y')
                path = os.path.join(DATA_ROOT, "kbars", base_tf, symbol, year, f"{d_str}_{symbol}_{base_tf}.parquet")
                if os.path.exists(path): df_list.append(pl.read_parquet(path))

        if not df_list:
            return pl.DataFrame()

        # 2. 合併與初步過濾 (處理跨日重複)
        df = pl.concat(df_list, how="diagonal").unique(subset=["ts"], keep="last").sort("ts")
        
        # 確保 session 欄位存在且無 null (解決 how="diagonal" 產生的 null)
        from config.calendar_rules import get_session_expression
        if "session" in df.columns:
            df = df.with_columns(pl.col("session").fill_null(get_session_expression("ts")))
        else:
            df = df.with_columns(get_session_expression("ts"))
        
        # 日線需額外過濾日期區間 (因為年檔包含整年)
        if base_tf == '1d':
            s_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
            e_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
            df = df.filter((pl.col("date") >= s_dt) & (pl.col("date") <= e_dt))
            
        # 3. 動態重取樣 (如果 requested timeframe 不等於 base_tf)
        if timeframe != base_tf:
            df = resample_kbars(df, timeframe)
            
        # 4. 歷史資料修復 Patch: 確保 08:xx 開頭的 K 棒強制判定為 Day (日盤)
        # 因為舊版 ETL 產生的歷史 parquet 可能會將 08:00 的 1h 標成 Night
        if "session" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("ts").dt.hour() == 8)
                  .then(pl.lit("Day"))
                  .otherwise(pl.col("session"))
                  .alias("session")
            )
            # 強制補強：00:00~05:00 & 15:00~23:00 必定是 Night
            df = df.with_columns(
                pl.when(pl.col("ts").dt.hour().is_in([15, 16, 17, 18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5]))
                  .then(pl.lit("Night"))
                  .otherwise(pl.col("session"))
                  .alias("session")
            )
            
        return df