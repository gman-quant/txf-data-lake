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
    def get_latest_record_time(symbol: str, timeframe: str = '1m') -> datetime:
        """
        掃描 Data Lake 找出該商品最新的 Parquet 檔案，並回傳最後一根 K 棒的時間戳。
        """
        base_dir = os.path.join(DATA_ROOT, "kbars", timeframe, symbol)
        if not os.path.exists(base_dir):
            return None
            
        if timeframe == '1d':
            # 1d 檔案結構：{symbol}_1d_{year}.parquet
            files = [f for f in os.listdir(base_dir) if f.endswith(".parquet")]
            if not files:
                return None
            latest_file = sorted(files)[-1]
            path = os.path.join(base_dir, latest_file)
        else:
            # 1m 檔案結構：{year}/{date}_{symbol}_{timeframe}.parquet
            years = [d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
            if not years:
                return None
            latest_year = sorted(years)[-1]
            year_dir = os.path.join(base_dir, latest_year)
            files = [f for f in os.listdir(year_dir) if f.endswith(".parquet")]
            if not files:
                return None
            latest_file = sorted(files)[-1]
            path = os.path.join(year_dir, latest_file)
            
        try:
            df = pl.read_parquet(path)
            if df.is_empty():
                return None
            if "ts" in df.columns:
                last_dt = df["ts"][-1]
                if isinstance(last_dt, str):
                    last_dt = datetime.fromisoformat(last_dt)
                return last_dt
            elif "date" in df.columns:
                last_dt = df["date"][-1]
                if isinstance(last_dt, str):
                    last_dt = datetime.strptime(last_dt, "%Y-%m-%d")
                return last_dt
        except Exception as e:
            print(f"[DataLoader] Error reading latest file {path}: {e}")
        return None

    @staticmethod
    def load_kbars(symbol: str, timeframe: str, start_date: str, end_date: str, combine_sessions: bool = False) -> pl.DataFrame:
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
            
            if combine_sessions:
                # 使用後向填充 (backward fill) 動態決定交易日進行篩選，確保前一日夜盤不會被提前過濾掉
                trading_date_expr = (
                    pl.when(pl.col("session") == "Day")
                    .then(pl.col("date"))
                    .otherwise(pl.lit(None))
                    .alias("trading_date")
                )
                
                # 計算 fallback 作為保險 (例如最後一筆是夜盤無後續日盤時)
                fallback_expr = (
                    pl.when(pl.col("date").dt.weekday() == 5)
                    .then(pl.col("date").dt.offset_by("3d"))
                    .otherwise(pl.col("date").dt.offset_by("1d"))
                )
                
                df_with_trading = (
                    df.sort("ts")
                    .with_columns(trading_date_expr)
                    .with_columns(pl.col("trading_date").fill_null(strategy="backward"))
                    .with_columns(pl.col("trading_date").fill_null(fallback_expr))
                )
                df = df_with_trading.filter((pl.col("trading_date") >= s_dt) & (pl.col("trading_date") <= e_dt)).drop("trading_date")
            else:
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