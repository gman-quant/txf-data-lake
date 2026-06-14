# 🇹🇼 TXF Data Lake (Taiwan Index Futures)

這是一個針對 **台指期 (TXF)** 與 **加權指數 (TSE)** 的資料處理專案。
利用 Python 與 Shioaji API 進行 Tick 資料抓取，並透過 Polars 進行高速清洗與重取樣，最終輸出 Parquet 檔案。

內建互動看盤工具，使用 `lightweight-charts` 顯示日線圖，並支援圖例切換均線顯示。

-----

## 🚀 核心功能 (Features)

  * **Raw Tick 與 K-Bar 資料湖**：
      * 原始 Tick 資料以月為單位儲存。
      * K-bar 資料支援 `5s`, `1m`, `5m`, `1h`, `1d`。
  * **ETL 流程**：
      * 同時支援 `TXF` 與 `TSE` 兩種商品。
      * 日線資料存成年度 Parquet，分時資料存成每日 Parquet。
      * 增量更新機制，已有 Tick 檔案不重複下載。
  * **互動看盤**：
      * `view_chart.py` 預設顯示 `1d` 日線。
      * 均線預設隱藏，使用者可點圖例手動開啟。
      * 支援 `--combine` / `--combined` 日夜盤合併與 `--adjust` 校正價格。

-----

## 🛠️ 安裝與設定 (Installation)

### 1\. 環境需求

  * Python 3.10+
  * 永豐金 Shioaji 帳號 (API 使用權限)

### 2\. 安裝依賴套件

```bash
uv venv

. .venv/bin/activate # Linux / macOS
. .venv/Scripts/activate # Windows

uv pip install -r requirements.txt
```

### 3\. 設定帳號 (Configuration)

請確保 `adapters/shioaji_source.py` 或您的設定檔中已填入正確的 API Key 與 Secret。
*(建議使用環境變數或獨立的 secrets.py 檔案管理，勿上傳至 Git)*

-----

## 📂 專案結構 (Project Structure)

```text
txf-data-lake/
├── adapters/                # Shioaji 連線與 Tick 資料下載
│   └── shioaji_source.py
├── config/                  # 全域設定與時間週期定義
│   ├── settings.py
│   └── calendar_rules.py
├── core/                    # 核心資料處理邏輯
│   ├── loader.py
│   ├── processor.py
│   └── resampler.py
├── visualization/           # 圖表顯示與樣式設定
│   ├── chart_builder.py
│   └── style_config.py
├── notes/                   # 策略與設計記錄
├── AutoRun.md
├── batch_run.py
├── fix_kbars.py
├── main_etl.py
├── README.md
├── requirements.txt
└── view_chart.py
```

> 注意：實際資料儲存在 `DATA_ROOT` 指定的路徑，預設為 `D:\txf-data`。

-----

## 🕹️ 使用指南 (Usage)

### 1\. 每日更新 (Daily ETL)

每天收盤（下午 13:45 以後）執行，抓取當日資料。

```bash
# 預設抓取「今天」的資料
python main_etl.py

# 指定抓取特定日期的資料
python main_etl.py --date 2025-12-05
```

### 2\. 補歷史資料 (Batch Historical Data)

若需一次補齊整個月或整季的資料，請修改 `batch_run.py` 內的 `START` 與 `END` 變數。
*(注意：Shioaji 有每日/單次連線流量限制，建議一次抓 2\~3 個月)*

```bash
python batch_run.py
```

### 3\. 專業看盤 (View Chart)

這是本專案的核心視覺化工具，為 **TradingView 風格的動態互動圖表**。

#### 🟢 智慧看盤 (預設)
系統搭載智慧防爆回推與記憶體快取，免輸入日期即可載入最新資料：
```bash
python view_chart.py
```
- 左上角 **Timeframe 切換器** 預設提供：`1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`, `1d (comb)`。
- 自動從最新日期往回推算載入最多 20,000 根 K 棒，防爆且瞬間顯示。
- 切換週期會自動寫入快取，後續切換 0 毫秒延遲。

#### 🟢 絕對即時串流 (Live Streaming)
實作 **Batch (Parquet) + Stream (Kafka)** 架構，提供即時跳動的看盤體驗：
```bash
python view_chart.py --live
```
- 透過 Kafka 動態抓取 Tick 補齊 Parquet 缺口 (Gap Replay)，並以 0.5s 頻率更新即時行情。
- 切換至任何短週期，最後一根 K 棒與各項指標皆會即時跳動。

#### 🟢 驗證 Kafka 接收與銜接功能 (Gap Replay Test)
使用 `--simulate-cut-date` 指定 Parquet 讀取的截止日，**截止日之後的所有資料將強制由 Kafka 進行抓取與補齊**，藉此驗證 Kafka 的接收與無縫接軌能力。

提供兩種驗證模式：
1. **瞬間補齊 (預設)**：由 Kafka 一次性下載指定日期至今的所有遺漏 Tick，並瞬間呈現在畫面上。
```bash
python view_chart.py --live --simulate-cut-date 2026-06-10
```
2. **全速漸進重播**：將指定日期至今的資料缺口，以全速「一根一根」在眼前畫出，追上最新行情後將無縫轉為即時監聽模式。
```bash
python view_chart.py --live --simulate-cut-date 2026-06-10 --progressive
```

#### 🟢 其他進階參數
```bash
# 自訂區間、下拉選單，並覆寫 20,000 根防爆上限
python view_chart.py --date 2022-06-01 --end-date 2022-08-31 --tf 5m --max-bars 50000 --tfs 1m 3m 1d

# 將前一日夜盤 (15:00) 與當日日盤 (13:45) 合併為單一全天盤 1d K 棒
python view_chart.py --tf 1d --combine

# 校正價格顯示 (消除結算日跳空)
python view_chart.py --tf 1d --adjust

# 查看 TSE (加權指數)
python view_chart.py --symbol TSE --tf 1d
```
> `view_chart.py` 預設隱藏所有均線（除 TAIEX 外），可於右上角圖例手動點選開啟。

-----

## 🎨 風格設定 (Style Configuration)

若要修改圖表顏色（例如改回美股綠漲紅跌，或調整十字線樣式），請編輯 `visualization/style_config.py`：

```python
# visualization/style_config.py

class ColorScheme:
    # 切換台股模式 (True=紅漲綠跌 / False=綠漲紅跌)
    TAIWAN_STYLE = True 
    
    # 十字線顏色設定
    CROSSHAIR_COLOR = '#CCCCCC'
```

-----

## ⚠️ 注意事項 (Notes)

1.  **Shioaji Quota**: 若遇到 `UsageStatus` 額度不足，請等待隔日重置。通常一個月 TXF Tick 資料量約 50MB\~120MB 不等。
2.  **Mac 顯示問題**: 若圖表全黑或無顯示，請確認 `view_chart.py` 中是否已強制啟用圖例 (Legend) 與十字線 (Crosshair) 的可見度設定。
3.  **補班日**: `batch_run.py` 採用 `freq='D'` 掃描，確保不會漏掉週六補班日的交易資料。

-----

**Happy Trading\! 🚀**