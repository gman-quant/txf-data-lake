# adapters/shioaji_source.py
import time

import shioaji as sj
import polars as pl
from datetime import datetime
from config.settings import API_KEY, SECRET_KEY

# 合約下載就緒的等待上限(登入後 Shioaji 於背景非同步下載合約集)
CONTRACT_READY_TRIES = 30
CONTRACT_READY_INTERVAL = 2          # 秒;30×2s = 最多等 60 秒

# 加權指數合約代碼(依序嘗試):新碼在前、舊碼保留當 fallback
TSE_INDEX_CODES = ("IX0001", "TSE001")


class ShioajiSource:
    def __init__(self):
        self.api = sj.Shioaji(simulation=True)
        self.is_connected = False

    def connect(self):
        if not self.is_connected:
            accounts = self.api.login(API_KEY, SECRET_KEY) # pyright: ignore[reportArgumentType]
            print(f"✅ Shioaji Login: {accounts[0].person_id}")
            self.is_connected = True
            self._wait_contracts_ready()

    def _wait_contracts_ready(self):
        """等合約集下載完成再放行(輪詢**實際能不能取到合約**)。

        ⚠️ 不要用 `api.Contracts.status` —— 那是已棄用的 Python 類別才有的屬性;
        實際的 `api.Contracts` 是編譯版物件(builtins.Contracts),沒有該屬性
        (2026-07-23 實測 AttributeError,寫了等於沒等)。改為直接探測目標合約。
        登入後合約是背景非同步下載的(期貨 FUT 先到、指數 IND 後到),沒等就取會 KeyError。
        """
        for i in range(1, CONTRACT_READY_TRIES + 1):
            try:
                if list(self.api.Contracts.Indexs.TSE) and                         self.api.Contracts.Futures.TXF.TXFR1 is not None:
                    if i > 1:
                        print(f"   ⏳ 合約集就緒(等了 {(i - 1) * CONTRACT_READY_INTERVAL}s)")
                    return
            except Exception:
                pass
            time.sleep(CONTRACT_READY_INTERVAL)
        print(f"⚠️  合約集在 {CONTRACT_READY_TRIES * CONTRACT_READY_INTERVAL}s 內仍不完整,"
              f"繼續(取合約時會重試)。")

    def get_contract(self, symbol_code, tries=3, interval=5):
        """
        工廠方法：根據代碼回傳對應的 Contract 物件。

        取不到就重試(合約集可能還在下載中)—— 第二道防線,配合 _wait_contracts_ready。
        """
        def _pick():
            if symbol_code == 'TXF':
                return self.api.Contracts.Futures.TXF.TXFR1      # 期貨近月
            elif symbol_code == 'TSE':
                # ⚠️ 2026-07-22 事故根因:Shioaji 的指數合約代碼**變了** ——
                #   舊碼 `TSE001` 已從合約表消失(2026-07-23 實測:TSE 指數 181 檔內
                #   查無 TSE001),加權指數現為 **IX0001「發行量加權股價指數」**。
                #   交叉驗證:IX0001 抓 2026-07-21 的 3,242 筆 tick 與湖內舊 TSE001
                #   存檔**逐筆對齊、close 最大差 0.0** = 同一標的,歷史資料可直接續接。
                #   保留舊碼 fallback:哪天官方改回去也不會再斷一次。
                for code in TSE_INDEX_CODES:
                    try:
                        c = self.api.Contracts.Indexs.TSE[code]
                        if c is not None:
                            return c
                    except Exception:
                        continue
                raise KeyError(f"加權指數合約不在合約表內(試過 {TSE_INDEX_CODES})")
            elif symbol_code == 'TXFR2':
                return self.api.Contracts.Futures.TXF.TXFR2      # 期貨次月
            else:
                raise ValueError(f"Unknown symbol: {symbol_code}")

        last = None
        for i in range(1, tries + 1):
            try:
                c = _pick()
                if c is not None:
                    return c
                last = KeyError(symbol_code)                     # 屬性存在但為 None 也算沒到
            except ValueError:
                raise                                            # 代碼打錯是程式 bug,別重試
            except Exception as e:
                last = e
            if i < tries:
                print(f"   ⏳ {symbol_code} 合約尚未就緒({last!r}),{interval}s 後重試 {i}/{tries - 1}…")
                time.sleep(interval)
        raise RuntimeError(f"{symbol_code} 合約取得失敗(重試 {tries} 次):{last!r}")

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
        if symbol_code in ('TXF', 'TXFR2'):
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