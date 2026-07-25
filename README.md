# txf-data-lake — 台指期歷史資料湖 ETL

從永豐 Shioaji **歷史 API** 取台指期 / 加權指數的 tick,用 Polars 重採樣成 K 棒,
寫成 Parquet 資料湖。**純 ETL,沒有看盤介面**(看盤在 `txf-quant-platform`)。

```
永豐 Shioaji API
   ├─（即時）→ txf-streaming-server ──Protobuf──→ Kafka ─────────┐
   └─（歷史）→ 【本專案】txf-data-lake ─Polars→ Parquet ─────────┤
                                          (D:\txf-data)        ├─→ txf-quant-platform（看盤/回測）
                                                               └─→ txf-gale-engine（戰情室看板）
```

**本專案不碰 Kafka**,只用 Shioaji 歷史 API。即時行情是 `txf-streaming-server` 的事。
下游兩個消費端(platform / gale)都會讀本專案產出的 Parquet。

| | |
|---|---|
| **商品** | `TXF`(近月)、`TXFR2`(次月)、`TSE`(加權指數) |
| **K 棒週期** | `5s` `1m` `5m` `1h` `1d` |
| **資料起點** | 2020-02-25 |
| **輸出** | Parquet,預設在 `D:\txf-data`(可用 `DATA_ROOT` 覆寫) |

---

## 1. 安裝

需要 **Python 3.13**、[`uv`](https://docs.astral.sh/uv/)、一組有 API 權限的永豐 Shioaji 帳號。

```bash
git clone <repo-url> && cd txf-data-lake
uv venv
uv pip install -r requirements.txt
```

> 🔴 **升級 polars 一律用 `uv pip install --upgrade -r requirements.txt`,別直接 `uv pip install --upgrade polars`。**
> `requirements.txt` 宣告的是 `polars[rtcompat]` —— 少了這個 extra,在**沒有 AVX2 的 CPU**
> (如 Ivy Bridge 世代)上會裝到需要 AVX2 的 runtime,執行時直接 **Illegal instruction 崩潰**。
> 三個套件 `polars` / `polars-runtime-32` / `polars-runtime-compat` 版本必須一致。

### 設定 `.env`

在 repo 根目錄建立 `.env`(已被 gitignore,**絕不可進版控**):

```bash
SHIOAJI_API_KEY=你的APIKey
SHIOAJI_SECRET_KEY=你的SecretKey
# 選填:覆寫資料湖位置(預設 D:\txf-data)
# DATA_ROOT=D:/txf-data
```

金鑰只能放這裡,不要寫進 `adapters/shioaji_source.py` 或任何會進 git 的檔案。

---

## 2. 資料湖結構

```text
D:\txf-data\
├── raw_ticks\                       原始 tick(以月為單位)
├── kbars\<tf>\<symbol>\<year>\      K 棒 Parquet(tf = 5s/1m/5m/1h/1d)
├── adjustments\                     結算日曆 settlement_calendar.csv 等
├── external\                        外部資料(如 Yahoo ^TWII 長期歷史)
└── txo\                             選擇權相關
```

> `1d` 每個交易日有**兩根**(日盤 08:45 / 夜盤 15:00)是正常設計,不是重複資料。

---

## 3. 日常使用

### 每日更新(收盤 13:45 之後)

```bash
python main_etl.py                    # 抓「今天」
python main_etl.py --date 2026-07-24  # 抓指定日
```

三個商品各自獨立處理:**單一商品失敗不會中斷其他商品**,失敗的會在後續每日同步中自動重試(自癒)。

### 驗證資料湖

```bash
python validate_lake.py --date 2026-07-24   # 檢查單日
python validate_lake.py --all               # 全庫體檢
```

### 補歷史資料

改 `batch_run.py` 裡的 `START` / `END` 再執行。Shioaji 有每日流量限制,**建議一次抓 2~3 個月**。

```bash
python batch_run.py
```

### 其他工具

| 指令 | 用途 |
|---|---|
| `python fix_kbars.py` | 重建有問題的 K 棒 |
| `python settlement_registry.py` | 更新結算日曆(向 TAIFEX API 自我校正) |
| `python taifex_calendar.py` | 交易日曆查詢 |

> ⚠️ 所有 Python 指令在 Windows 上請前綴 `PYTHONUTF8=1`(這些腳本會印 emoji,
> cp950 環境下不加會直接崩潰)。

---

## 4. 自動化(每日排程)

**排程不是由本專案管的。** workspace 根目錄的 `daily_sync.py` 是唯一的編排器,
由 Windows 工作排程「TXF Daily Sync」每個工作日 **13:50** 呼叫:

```
就緒輪詢(ping)→ 掃描缺哪幾天 → 逐日執行:
    main_etl.py → validate_lake.py → (gale) 匯出 bidask → 匯出 HTML
```

```bash
python daily_sync.py            # 補所有缺的交易日(排程跑這個)
python daily_sync.py --date D   # 只補單日
python daily_sync.py --dry-run  # 只看缺哪幾天,不執行(零風險)
```

缺口掃描是 **per-symbol** 的:某天只有部分商品成功,隔天會自動只補沒補到的那個。
假日不必特別處理 —— `main_etl.py` 的幻影資料守衛會自己跳過。

日誌:`logs/catchup-<日期>.log`;各步驟狀態:`logs/sync_state.json`。

---

## 5. 疑難排解

| 症狀 | 處理 |
|---|---|
| `Illegal instruction` 崩潰 | polars 沒帶 `[rtcompat]` extra,見上方安裝章節 |
| `UsageStatus` 額度不足 | Shioaji 每日額度用完,等隔日重置(TXF 一個月 tick 約 50–120 MB) |
| 印字時 `UnicodeEncodeError` | 加 `PYTHONUTF8=1` 前綴 |
| 某天資料缺 | `python daily_sync.py --dry-run` 看缺哪幾天,再 `--date` 補 |
| 週末/假日出現奇怪資料 | 正常會被守衛擋掉;若已寫入,用 `validate_lake.py --all` 找出來 |

---

## Disclaimer

本專案供程式交易研究與教育用途,不構成投資建議。使用者需自行承擔資料正確性與交易風險。
