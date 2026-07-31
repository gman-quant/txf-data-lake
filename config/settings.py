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
# 5s:極短線, 1m:原子K, 5m:波段, 30m:粗 TF 的聚合底層, 1h:長線, 1d:日線
#
# 30m(2026-07-31 加):**不是為了直接看 30m**,是給 platform 當「1h 以上 TF 的聚合底層」。
#   起因:platform 新增的 3seg(語意分段:早盤/午盤/晚盤,切點是美股開盤 21:30 或 22:30)
#   需要 **≤30m 粒度**才切得到那條邊界,而湖裡最粗的可用底層是 5m
#   → 聚 3000 根 3seg 要讀 ~34 萬根 5m,載入 16.4 秒。改吃 30m 只要 ~5.7 萬根。
#   順帶讓 platform 的 30m 從「5m 聚」變成原生讀取。
#
# ✅ 正確性已驗(2026-07-31,5 個交易日):
#   ① 30m 從 5m 聚 == 從 1m 聚(兩者來源都是逐筆、獨立產出)
#   ② **1h 從 30m 聚 == 湖裡原生 1h**(原生是逐筆產的 → ground truth)逐根完全相同
#   依據與 ADR P3「5s 是原子單位」同一個論證:粗 TF 的邊界是細 TF 邊界的子集,
#   而 OHLC 可結合、volume 與 true_pv_sum 可加 ⇒ 分層聚合無損。
#   歷史 4807 檔是**從既有 5m 回填**的(比重跑逐筆快得多,且由上述對照保證等價)。
TIMEFRAMES = ['5s', '1m', '5m', '30m', '1h', '1d']