# 🇹🇼 TXF Data Lake (Taiwan Index Futures)

這是一個針對 **台指期 (TXF)** 與 **加權指數 (TSE)** 打造的高效能數據湖 (Data Lake) 專案。
利用 Python 與 Shioaji API 進行高頻 Tick 資料抓取，並透過 Polars 進行高速清洗與重取樣 (Resampling)，最終儲存為標準化的 Parquet 檔案。

包含一個基於 `lightweight-charts` 的專業級看盤工具，支援日夜盤分離/合併顯示、台股配色習慣以及高效能回放。

-----

## 🚀 功能特色 (Features)

  * **無損資料架構**：
      * **Raw Ticks**：以「月」為單位存檔，保留最原始 Tick 資訊。
      * **K-Bars**：以「年」為單位存檔 (Polars 優化)，支援 `1d`, `1h`, `1m`, `5s` 等多種週期。
  * **智能 ETL**：
      * 自動處理跨日夜盤 (Overnight Session) 歸屬問題。
      * 自動識別週五夜盤跨週一的日期位移。
      * 增量更新機制 (Incremental Update)，不重複下載。
  * **專業視覺化**：
      * 支援 **台股配色 (紅漲綠跌)** 或美股配色切換。
      * **日夜盤分明**：透過亮度區分時段 (日盤亮、夜盤暗)。
      * **彈性看盤**：支援單日檢視、日期區間拼接、日線合併 (Combine) 等模式。

-----

## 🛠️ 安裝與設定 (Installation)

### 1\. 環境需求

  * Python 3.10+
  * 永豐金 Shioaji 帳號 (API 使用權限)

### 2\. 安裝依賴套件

```bash
pip install -r requirements.txt
```

### 3\. 設定帳號 (Configuration)

請確保 `adapters/shioaji_source.py` 或您的設定檔中已填入正確的 API Key 與 Secret。
*(建議使用環境變數或獨立的 secrets.py 檔案管理，勿上傳至 Git)*

-----

## 📂 專案結構 (Project Structure)

```text
txf-data-lake/
├── config/                  # [設定] 全域配置與規則
│   ├── settings.py          # 定義 DATA_ROOT 路徑與支援的 K 棒週期
│   └── calendar_rules.py    # 定義日夜盤 (Day/Night) 的切割邏輯
├── data/                    # [核心] 數據湖儲存區
│   ├── raw_ticks/           # 原始 Tick 資料 (YYYY/MM 分類)
│   └── kbars/               # K棒資料 (YYYY 分類，便於回測)
├── core/
│   └── resampler.py         # K棒重取樣、跨日處理與時間位移邏輯
├── visualization/
│   └── style_config.py      # 色票管理 (台股風格/十字線設定)
├── adapters/
│   └── shioaji_source.py    # Shioaji API 連線與資料抓取邏輯
├── main_etl.py              # [主程式] 單日 ETL 任務 (增量更新)
├── batch_run.py             # [工具] 歷史資料批次下載 (自動掃描區間)
└── view_chart.py            # [工具] 互動式看盤系統 (支援日線合併)
```

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
# 預設：看今天的 TXF 5分K
python view_chart.py

# 指定日期與週期 (例如 12/05 的 1分K)
python view_chart.py --date 2025-12-05 --tf 1m
```

#### 🟢 查看一段時間 (自動拼接)

想看連續趨勢，例如 11月整個月的走勢：

```bash
python view_chart.py --date 2025-11-01 --end-date 2025-11-30 --tf 1h
```

#### 🟢 日線圖 (合併日夜盤)

預設日夜盤分開：

```bash
# --combine 參數會自動執行聚合邏輯
python view_chart.py --date 2025-01-01 --end-date 2025-12-31 --tf 1d
```

將「日盤」與「夜盤」合併為單一根 K 棒，適合觀察大波段趨勢：

```bash
# --combine 參數會自動執行聚合邏輯
python view_chart.py --date 2025-01-01 --end-date 2025-12-31 --tf 1d --combine
```

#### 🟢 查看加權指數 (TSE)

```bash
# --sybbol choices: TSE, TXF(default)
python view_chart.py --symbol TSE --date 2025-01-01 --end-date 2025-12-31  --tf 1d
```

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