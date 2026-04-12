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
      * 支援 `--combined` 日夜盤合併與 `--adjust` 校正價格。

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

這是本專案最強大的視覺化工具。

#### 🟢 基本單日看盤

```bash
# 預設：看今天的 TXF 1日K
python view_chart.py

# 指定日期與週期 (例如 12/05 的 1分K)
python view_chart.py --date 2025-12-05 --tf 1m
```

#### 🟢 查看一段時間 (自動拼接)

想看連續趨勢，例如 11月整個月的走勢：

```bash
python view_chart.py --date 2025-01-01 --end-date 2026-12-31 --tf 1h
```

#### 🟢 日線圖 (合併日夜盤)

```bash
# 不加 --combined 時，保留原始日夜盤分離顯示
python view_chart.py --date 2025-01-01 --end-date 2025-12-31 --tf 1d
```

```bash
# 加上 --combined，會將日盤與夜盤合併成單一 1d K 棒
python view_chart.py --date 2025-01-01 --end-date 2025-12-31 --tf 1d --combined
```

#### 🟢 校正價格顯示

```bash
python view_chart.py --date 2025-01-01 --end-date 2025-12-31 --tf 1d --adjust
```

#### 🟢 查看 TSE

```bash
python view_chart.py --symbol TSE --date 2025-01-01 --end-date 2025-12-31 --tf 1d
```

> `view_chart.py` 現在預設會隱藏均線，若要查看可以在圖例中點選對應線條。

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