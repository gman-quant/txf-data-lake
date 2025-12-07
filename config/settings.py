# config/settings.py
import os

from dotenv import load_dotenv

# Load Environment Variables 
load_dotenv()

API_KEY = os.environ.get("SHIOAJI_API_KEY")
SECRET_KEY = os.environ.get("SHIOAJI_SECRET_KEY")

# 資料儲存根目錄
DATA_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

# 定義要轉檔的週期列表
# 5s:極短線, 1m:原子K, 5m:波段, 1h:長線, 1d:日線
TIMEFRAMES = ['5s', '1m', '5m', '1h', '1d']