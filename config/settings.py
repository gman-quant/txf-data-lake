# config/settings.py
import os

from dotenv import load_dotenv

# Load Environment Variables 
load_dotenv()

API_KEY = os.environ.get("SHIOAJI_API_KEY")
SECRET_KEY = os.environ.get("SHIOAJI_SECRET_KEY")

# 資料儲存根目錄
# DATA_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'txf-data')
DATA_ROOT = os.environ.get("DATA_ROOT", r"D:\txf-data")

# 結算與調整資料目錄
ADJUSTMENTS_DIR = os.path.join(DATA_ROOT, "adjustments")
# 結算日曆(2026-07-21 起唯一來源;舊 monthly_settlements.csv 已退役刪除)。
# 由 settlement_registry.py 維護,涵蓋過去 + 未來,欄位 date,contract,status,source,algo_date。
SETTLEMENT_CALENDAR_PATH = os.path.join(ADJUSTMENTS_DIR, "settlement_calendar.csv")

# 定義要轉檔的週期列表
# 5s:極短線, 1m:原子K, 5m:波段, 1h:長線, 1d:日線
TIMEFRAMES = ['5s', '1m', '5m', '1h', '1d']