# adapters/shioaji_source.py
import shioaji as sj
import polars as pl
from datetime import datetime
from config.settings import API_KEY, SECRET_KEY

class ShioajiSource:
    def __init__(self):
        self.api = sj.Shioaji(simulation=True)
        self.is_connected = False

    def connect(self):
        if not self.is_connected:
            accounts = self.api.login(API_KEY, SECRET_KEY)
            print(f"✅ Shioaji Login: {accounts[0].person_id}")
            self.is_connected = True

    def get_contract(self, symbol_code):
        """
        工廠方法：根據代碼回傳對應的 Contract 物件
        """
        if symbol_code == 'TXF':
            # 抓期貨近月
            return self.api.Contracts.Futures.TXF.TXFR1
        elif symbol_code == 'TSE':
            # 抓加權指數
            return self.api.Contracts.Indexs.TSE.TSE001
        else:
            raise ValueError(f"Unknown symbol: {symbol_code}")

    def fetch_ticks(self, date_str: str, symbol_code: str):
        self.connect()
        
        # 1. 取得合約 (這裡示範抓 TXF 近月)
        contract = self.get_contract(symbol_code)
        
        print(f"📥 Fetching {symbol_code} ticks for {date_str}...")
        ticks = self.api.ticks(contract, date_str)

        # 🛡️ 防呆：如果沒抓到資料，直接回傳空的 DataFrame
        if not ticks or len(ticks.ts) == 0:
            print(f"⚠️ Warning: No ticks found for {symbol_code} on {date_str}")
            return pl.DataFrame() # 回傳空表，讓 main_etl.py 處理


        # 2. 動態建立 Data Dict (核心修改)
        # 基礎欄位：所有商品都有
        data_dict = {
            'ts': ticks.ts, # 時間：原始是 int64 (nanoseconds)
            'close': ticks.close,
            'volume': ticks.volume
        }
        if symbol_code == 'TXF':
            data_dict.update({
                'bid_price': ticks.bid_price,
                'bid_volume': ticks.bid_volume,
                'ask_price': ticks.ask_price,
                'ask_volume': ticks.ask_volume,
                'tick_type': ticks.tick_type # 內外盤 (1:外, 2:內, 0:未知)
            })

        # 3. 轉為 Polars DataFrame
        df = pl.DataFrame(data_dict)

        # 4. 型別轉換與標準化
        # 處理時間 (ns -> datetime)
        df = df.with_columns(
            pl.from_epoch(pl.col("ts"), time_unit="ns").alias("ts")
        )
        # 處理 Tick Type (如果有這個欄位)
        if 'tick_type' in df.columns:
            df = df.with_columns(pl.col("tick_type").cast(pl.Int8))

        # 🌟 5. 注入 Symbol 標籤 (為了未來的合併做準備)
        # 使用 pl.lit (Literal) 填充整列，Polars 會優化儲存，不佔空間
        df = df.with_columns(pl.lit(symbol_code).alias("symbol"))
        # 欄位排序：確保 ts → symbol 在最前面，其他欄位保持原順序
        keep_cols = [c for c in df.columns if c not in ("ts", "symbol")]
        df = df.select(["ts", "symbol", *keep_cols])
        
        return df
    
    def report_usage(self):
        print(self.api.usage())

    def logout(self):
        self.api.logout()