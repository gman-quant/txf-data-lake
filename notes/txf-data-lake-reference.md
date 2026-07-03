# TXF Data Lake — 即時看盤系統完整技術參考文件

> **用途**：供 `txf-quant-platform` 重構時的完整參考。  
> **版本**：記錄截至 2026-06-16 的最終優化版本。  
> **原始碼**：`c:\Projects\TXF-Trading-Workspace\txf-data-lake\`

> ### 🔄 2026-06-16 全專案深度 Review（本次更新）
> 本文經一次「逐檔實讀 + 對抗式驗證（9 面向、18 個 agent、420 次工具呼叫）」的完整 review。
> - **保留**：原 §1–§10 的演算法說明大多仍正確，原樣保留。
> - **更新**：修正了多處過時敘述（最重要的是 §5.1 `LiveDeltaManager` 的記憶體資料契約完全寫錯，見下）。所有更新處標註 `⚠️ 已修正 2026-06-16`。
> - **新增**：§11–§16 補上原本完全缺漏的「資料攝取/ETL 管線、Data Lake 實體 Schema、還原價差邏輯、指標覆蓋表、資料正確性問題清單、依優先序的重構任務清單」。
>
> 🔴 **最高優先（安全）**：`.env` 以明文存放 API 金鑰（已 gitignore、未進 git 史）。建議定期輪換，見 §15「I-SEC」。

---

## 目錄

1. [專案架構總覽](#1-專案架構總覽)
2. [資料流程圖](#2-資料流程圖)
3. [核心模組詳解](#3-核心模組詳解)
   - 3.1 [config — 設定與規則](#31-config--設定與規則)
   - 3.2 [core/loader.py — 歷史資料讀取](#32-coreloaderpy--歷史資料讀取)
   - 3.3 [core/resampler.py — K 棒重採樣引擎](#33-coreresamplerpy--k-棒重採樣引擎)
   - 3.4 [core/processor.py — 指標計算引擎](#34-coreprocessorpy--指標計算引擎)
   - 3.5 [core/kafka_reader.py — Kafka 即時消費者](#35-corekafka_readerpy--kafka-即時消費者)
   - 3.6 [visualization/style_config.py — 視覺主題](#36-visualizationstyle_configpy--視覺主題)
   - 3.7 [visualization/chart_builder.py — 圖表渲染器](#37-visualizationchart_builderpy--圖表渲染器)
   - 3.8 [view_chart.py — 主程式（最複雜）](#38-view_chartpy--主程式最複雜)
4. [關鍵演算法與設計決策](#4-關鍵演算法與設計決策)
   - 4.1 [時間對齊：Period Alignment（最難坑）](#41-時間對齊period-alignment最難坑)
   - 4.2 [即時 Delta 合併](#42-即時-delta-合併)
   - 4.3 [Smart Rollover（結算日換倉）](#43-smart-rollover結算日換倉)
   - 4.4 [增量 MA 計算（最終版優化）](#44-增量-ma-計算最終版優化)
   - 4.5 [VWAP 計算的跨日對齊問題](#45-vwap-計算的跨日對齊問題)
   - 4.6 [EMA 在截斷資料上的誤差問題](#46-ema-在截斷資料上的誤差問題)
5. [即時系統運作細節](#5-即時系統運作細節)
   - 5.1 [LiveDeltaManager](#51-livedelta-manager)
   - 5.2 [Gap Replay（斷線重播）](#52-gap-replay斷線重播)
   - 5.3 [背景執行緒架構](#53-背景執行緒架構)
6. [資料 Schema 規格](#6-資料-schema-規格)
7. [已知問題與 Workarounds](#7-已知問題與-workarounds)
8. [效能分析](#8-效能分析)
9. [重構建議（給 txf-quant-platform）](#9-重構建議給-txf-quant-platform)
10. [CLI 使用方式](#10-cli-使用方式)
11. [資料攝取 / ETL 管線（Ingestion）— ⚠️ 新增](#11-資料攝取--etl-管線ingestion)
12. [Data Lake 實體結構與完整 Schema — ⚠️ 新增](#12-data-lake-實體結構與完整-schema)
13. [還原價差 / 除息調整邏輯 — ⚠️ 新增](#13-還原價差--除息調整邏輯)
14. [指標覆蓋表（現有 vs 缺漏）— ⚠️ 新增](#14-指標覆蓋表現有-vs-缺漏)
15. [資料正確性與完整性問題清單 — ⚠️ 新增](#15-資料正確性與完整性問題清單)
16. [重構任務清單（依優先序）— ⚠️ 新增](#16-重構任務清單依優先序)

---

## 1. 專案架構總覽

```
txf-data-lake/
├── config/
│   ├── settings.py          # 路徑、API Key、週期定義
│   └── calendar_rules.py    # 台指期交易時段規則（Day/Night session）
├── core/
│   ├── loader.py            # DataLoader: Parquet 讀取、日期範圍篩選
│   ├── resampler.py         # K 棒重採樣（tick → 任意週期 OHLCV）
│   ├── processor.py         # DataProcessor: MA/VWAP/顏色計算
│   └── kafka_reader.py      # KafkaTickReader: 即時/Gap Replay 消費者
├── visualization/
│   ├── style_config.py      # ColorScheme: MA 設定、顏色主題
│   └── chart_builder.py     # ChartBuilder: lightweight-charts 渲染器
├── adapters/                # 外部 API 介接（Shioaji 等）
├── view_chart.py            # 主程式：所有邏輯的 orchestrator
└── main_etl.py              # ETL 批次腳本（歷史 Parquet 生成）
```

**核心依賴**：
- `polars` — 主要資料框架（效能比 pandas 快 10-100x）
- `lightweight-charts-python` — 前端圖表渲染（基於 TradingView LWC）
- `confluent-kafka` — Kafka 消費者
- `pandas` — 只在圖表 API 介面層使用（chart_builder.py）

---

## 2. 資料流程圖

### 啟動流程

```
main()
  │
  ├─ [--live] Kafka 初始化
  │   ├─ KafkaTickReader 連線（TXF + TXFR2 topics）
  │   ├─ DataLoader.get_latest_record_time() → 找最後一根 Parquet 時間
  │   └─ KafkaTickReader.fetch_gap_ticks() → Gap Replay（補充 Parquet 結束到現在的 ticks）
  │
  ├─ get_data(args.tf)  ← 第一次全量計算
  │   ├─ load_historical_raw() × 3（TXF, TSE, TXFR2 並行）
  │   ├─ delta_manager.get_ticks() → LiveDeltaManager 提供 live ticks
  │   ├─ perform_delta_merge() → 拼接 Parquet + live ticks
  │   ├─ Smart Rollover（結算日換倉，可選）
  │   ├─ apply_adjustment()（前復權，可選）
  │   ├─ DataProcessor.process_data()（MA, VWAP, 顏色）
  │   ├─ _fetch_and_join_external(TSE) → 拼接 TAIEX，算 basis
  │   ├─ _fetch_and_join_external(TXFR2) → 拼接 TXFR2，算 calendar_spread
  │   └─ cache_proc[tf] = df_proc  ← 快取供增量更新使用
  │
  ├─ ChartBuilder 初始化
  │   ├─ lightweight-charts 主視窗
  │   ├─ basis 副圖
  │   └─ 讀取 settlement CSV → self._settle_date_set（快取）
  │
  ├─ viewer.plot(df_processed)
  │
  └─ [--live] 啟動背景 Kafka 執行緒
```

### 即時更新流程（每 100ms 一次 poll，有 tick 才觸發）

```
KafkaTickReader.poll_new_ticks()
  │
  ├─ [有 tick] → LiveDeltaManager.add_ticks()
  │               └─ 加入 live_ticks，超過 26h 自動截斷
  │
  └─ on_tick_cb()  ← 每 0.5s 最多執行一次
      │
      ├─ [快速路徑] get_data(background_update=True)
      │   ├─ cache_proc[tf] 已存在？ → 走增量路徑
      │   │   ├─ perform_delta_merge() → 取最新 K 棒 OHLCV
      │   │   ├─ [B] 同根且價格未變？ → return cached
      │   │   ├─ _incremental_ma_update() → O(6) 算術
      │   │   └─ 更新 cached 最後一列 → return updated
      │   └─ 快取未建立 → 走全量路徑（通常不會發生）
      │
      └─ viewer.update_live_bar(live_bar)  ← 推送給圖表
```

---

## 3. 核心模組詳解

### 3.1 config — 設定與規則

#### `settings.py`

```python
DATA_ROOT = os.environ.get("DATA_ROOT", r"D:\txf-data")  # Parquet 根目錄

# Parquet 子目錄結構
# 分時線: {DATA_ROOT}/kbars/{tf}/{symbol}/{year}/{date}_{symbol}_{tf}.parquet
# 日線:   {DATA_ROOT}/kbars/1d/{symbol}/{symbol}_1d_{year}.parquet
# 調整表: {DATA_ROOT}/adjustments/txf_adjustment_table_final.csv
# 結算日: {DATA_ROOT}/adjustments/monthly_settlements.csv

TIMEFRAMES = ['5s', '1m', '5m', '1h', '1d']  # ETL 生成的週期
```

#### `calendar_rules.py`

台指期交易時段定義：
- **日盤 (Day)**：分類邊界為半開區間 `[DAY_START, DAY_END)` = `[08:30:00, 13:45:05)`
- **夜盤 (Night)**：其餘時間（`15:00:00 ~ 05:00:00`，跨午夜）

> **⚠️ 已修正 2026-06-16**：
> - `DAY_START = time(8, 30)`（**不是** 08:45）。08:30 並非真實開盤，而是「夜盤跨日 → 日盤」的**日期切換樞紐**（`resampler.py:91` 用它把 `ts < 08:30` 的 tick 退一天）。實際開盤 08:45；經實測 `[08:30, 08:45)` 區間目前 0 筆 tick，故僅作樞紐用途。原始碼註解寫「08:45」是**過時註解**（QA-14）。
> - `DAY_END = time(13, 45, 5)`（**刻意多 5 秒**），因為日盤最後一筆收盤集合競價成交常落在 `13:45:00.0xx`；`times < DAY_END` 確保它仍歸 Day，再由收盤 Snap 併入最後一個合法 bucket。

```python
# 時段分類表達式（Polars Expression）
def get_session_expression(col_name="ts"):
    times = pl.col(col_name).dt.time()
    return (
        pl.when((times >= DAY_START) & (times < DAY_END))
        .then(pl.lit("Day"))
        .otherwise(pl.lit("Night"))
        .alias("session")
    )
```

> **⚠️ 重要坑**：`DAY_START = time(8, 30)` 而非 `time(8, 45)`，是為了讓 `08:30~08:44` 的 tick 也能被標為日盤時段（早盤準備期）。

---

### 3.2 `core/loader.py` — 歷史資料讀取

`DataLoader` 負責從 Parquet Lake 讀取並組合歷史 K 棒。

**關鍵邏輯**：

1. **讀取策略**：
   - 分時線：逐日讀取（`{date}_{symbol}_{tf}.parquet`）
   - 日線：逐年讀取（`{symbol}_1d_{year}.parquet`）

2. **combine_sessions（合併日夜盤）模式**：
   使用「後向填充 (backward fill)」決定夜盤歸屬的交易日。
   - 夜盤的交易日 = 其後第一個日盤的日期（例如週三夜盤歸屬週四的交易日）
   - Fallback：若最後一筆是夜盤，使用 `date + 1d`（週末 `+3d`）

3. **Session patch**：
   舊版 ETL 可能有 `08:xx` 的 K 棒被誤標為 `Night` 的情況，loader 會強制修正。

4. **動態重採樣**：
   若請求的 `timeframe` 不在 TIMEFRAMES 列表中（例如 `15m`, `4h`），會找最近的底層週期（`1m` 或 `1h`），再呼叫 `resample_kbars()` 動態聚合。

**get_latest_record_time()** 的用途：
啟動即時模式時，找出 Parquet 最後一根 K 棒的時間戳，作為 Gap Replay 的起始點。

---

### 3.3 `core/resampler.py` — K 棒重採樣引擎

這是整個專案中最精密的模組，解決了台指期跨午夜交易的時間對齊問題。

#### 核心挑戰：Period Alignment

台指期的交易時段不從整點開始：
- 日盤：`08:45` 開始（不是 `09:00`）
- 夜盤：`15:00` 開始

若用 Polars 的 `group_by_dynamic` 直接以原始時間分組，`08:45` 的 tick 會被切到錯誤的時間桶（例如 `09:00` 桶而非 `08:45` 桶）。

#### 解法：時間平移 + 對齊 + 還原

```python
# 步驟1：時間平移，讓開盤對齊 00:00
aligned_ts = ts - 8h45m  if session == "Day"   # 08:45 → 00:00
aligned_ts = ts - 15h    if session == "Night"  # 15:00 → 00:00

# 步驟2：group_by_dynamic 以 aligned_ts 分組（現在可以整點切齊）
group_by_dynamic("aligned_ts", every=timeframe, group_by=["date", "session"])

# 步驟3：平移還原
ts = aligned_ts + 8h45m  if session == "Day"
ts = aligned_ts + 15h    if session == "Night"
```

#### 收盤 Snap（`_snap_aligned_ts_to_session`）

台指期收盤時間不是整點（日盤 `13:45`，夜盤 `05:00`），收盤後可能有零星 tick 落在超出 session 範圍的時間桶。

**問題**：這些 tick 會形成「殘餘迷你 K-bar」（例如 `13:45:00` 那分鐘只有 2 筆 tick，但會被切成獨立的 K 棒）。

**解法**：計算每個 session 的「最後合法 bucket 起點」，超出範圍的 tick 強制歸入最後一個合法 bucket。

```python
# 日盤最後合法 bucket：floor((5*3600 - 1) / tf_sec) * tf_sec
# 例如 5m 日盤：floor(17999 / 300) * 300 = 17700 = 295min = 4h55m → 13:40
day_last_bucket_sec = (((_DAY_SESSION_LIMIT_SEC - 1) // tf_sec)) * tf_sec
```

> **⚠️ 重要坑**：`dt.hour()` 回傳 `Int8/Int16`，乘以 3600 會溢出，必須先 `.cast(pl.Int32)`。

#### `true_pv_sum`

```python
(pl.col("close") * pl.col("volume")).sum().alias("true_pv_sum")
```

在聚合時同時計算加權價格乘積之和，供 VWAP 計算用。這確保 VWAP 是完整的成本加權均價，而非各 K 棒收盤的簡單平均。

---

### 3.4 `core/processor.py` — 指標計算引擎

`DataProcessor.process_data()` 的計算步驟：

```
Step A: 防呆補強
  → 補全 session 欄位（時間 derivation）
  → 計算 is_up（close >= open）
  → 時間格式化（time 欄位）
  → 日期補全（date 欄位）

Step B: VWAP 分組日期計算
  → vwap_group_date = (ts + 9h).date()
  ← 使用 9h 平移完美對齊期交所「交易日」
    （夜盤 15:00 = 隔天的開始，+9h 後為 00:00）

Step C: VWAP 計算
  → cum_sum(true_pv_sum) / cum_sum(volume)  [分組：vwap_group_date × session]
  → 若無 true_pv_sum：使用 (H+L+C)/3 × volume 近似（精度略低）

Step D: 顏色 + MA 批次計算
  → K 棒顏色（4 色：日漲/日跌/夜漲/夜跌）
  → 成交量顏色（基礎色經 _lighten(VOL_LIGHTEN=-0.1)，實際是「略為加深」而非加亮，見 §3.6）
  → MA/EMA（向量化運算）
  → 清理暫存欄位（date_temp, vwap_group_date）
```

#### MA 設定（`ColorScheme.MA_SETTINGS`）

| 代號 | 類型 | 顏色 | 備注 |
|------|------|------|------|
| MA5 | SMA | 白色 | 短線趨勢 |
| MA10 | SMA | 深天藍 | 短線支撐 |
| MA20 | SMA | 亮橘（80% 透明）| 月線 |
| MA55 | SMA | 橘紅（50% 透明）| 季線 |
| EMA660 | EMA | 金色（50% 透明）| ~3月均線（1m 週期）|
| EMA3300 | EMA | 藍色（50% 透明）| ~年均線（1m 週期）|

**1d 週期特殊處理**：分開日夜盤時，每天有 2 根 K 棒，MA 週期需乘以 2（MA5 實際用 period=10 才等效 5 個交易日）。

---

### 3.5 `core/kafka_reader.py` — Kafka 即時消費者

#### 設計要點

```python
class KafkaTickReader:
    # Kafka 設定（connect() 完整 conf，⚠️ 已修正 2026-06-16 補齊）
    # bootstrap.servers: 192.168.1.50:9092   ← 硬編碼預設（kafka_reader.py:14 與 view_chart.py:391 各一份）
    # topics: txf-tick, txfr2-tick
    # auto.offset.reset: latest
    # enable.auto.commit: False（手動控制，全程用 assign() 不用 subscribe）
    # fetch.min.bytes: 1
    # fetch.wait.max.ms: 10ms（快速響應）
    # socket.nagle.disable: True
    # group_id: KafkaTickReader 自身預設是「靜態」'txf_chart_live_v2'；
    #           每次啟動的唯一 UUID group 其實是在 view_chart.main() 組的
    #           （`txf_chart_live_{uuid4().hex[:8]}`, view_chart.py:420），不是 reader 內部
    
    # 使用 RLock 而非 Lock：保護底層 librdkafka Consumer 物件、允許同執行緒重入
    self.lock = threading.RLock()
    # ⚠️ 注意：只有 KafkaTickReader 用 RLock；LiveDeltaManager 用的是普通 threading.Lock（見 §5.1）
```

#### `fetch_gap_ticks(since_ts_ms)`

Gap Replay 流程（⚠️ 已修正 2026-06-16）：
1. **對每個 topic** 各跑一次：`offsets_for_times([tp@since_ts_ms])` → 找到對應 offset
2. `get_watermark_offsets()` → 取得 high watermark（當前最新位置）
3. `assign([tp@start_offset])` 後批次消費（`consume(2000, 1.0)`）到 high_watermark
4. **回傳 `dict{topic: pl.DataFrame}`**（不是單一 DataFrame）；`main()` 在 `view_chart.py:479-480` 把 dict 拆成 `delta_manager`(txf-tick) 與 `delta_manager_r2`(txfr2-tick)
5. **刻意棄用 `subscribe()`**：歷史抓完後直接 `assign(live_tps)`（所有 topic 設到 high watermark）切回 live，跳過 rebalance（原始碼註解：「完全棄用 self.consumer.subscribe()」）

> **⚠️ 單一 partition 假設（風險，live-kafka）**：每個 `TopicPartition(topic, 0)` 都硬編碼 partition 0；若 topic 未來被 repartition，partition 1..N 的 tick 會被**靜默丟失**。建議在 `connect()` 加 `assert len(partitions)==1`，或列舉全部 partition。

**Protobuf Schema**（`core/data_schemas/txf_data_pb2.py`，⚠️ 已修正 2026-06-16 補齊）：

`Tick`（7 個欄位，kafka_reader 只取其中 4 個）：
```
code:             string  (合約代碼，如 'TXFL5'；kafka_reader 不取)
timestamp_ms:     int64   (毫秒 Unix timestamp)
close:            int64   (價格 × 10000，例如 22000.0 → 220000000)
volume:           int32
tick_type:        int32   (內外盤 1:外/2:內/0:未知 — ⚠️ kafka_reader 丟棄，未取用)
underlying_price: int64   (現貨/指數價格 × 10000)
total_volume:     int32   (累計量 — ⚠️ kafka_reader 丟棄)
```

`BidAsk`（5 檔委託簿深度，⚠️ **完整定義但全專案無任何程式消費**）：
```
code, timestamp_ms,
bid_total_vol: int32,  ask_total_vol: int32,
bid_price:  repeated int64 (×10000),  bid_volume: repeated int32,  diff_bid_vol: repeated int32,
ask_price:  repeated int64 (×10000),  ask_volume: repeated int32,  diff_ask_vol: repeated int32
```
> `kafka_reader.py` 只 `import Tick`；`BidAsk` 從未被 import/instantiate。`tick_type`/`total_volume`/`BidAsk` 正是 CVD/COFI/COBI 所需的原料，目前全被丟棄（見 §14）。

#### `poll_new_ticks()`（優化版）

```python
msgs = self.consumer.consume(num_messages=200, timeout=0.1)
# timeout=0.1 → 100ms blocking，讓 OS scheduler 有機會休息
# num_messages=200 → 降低單批大小，避免一次鎖住 consumer 太久
```

---

### 3.6 `visualization/style_config.py` — 視覺主題

#### 配色系統

台灣慣例：**紅漲綠跌**（`TAIWAN_STYLE = True`）

| 場景 | 顏色 | 代號 |
|------|------|------|
| 日盤上漲 | `#ef5350`（紅） | `C_UP` |
| 日盤下跌 | `#26a69a`（綠） | `C_DN` |
| 夜盤上漲 | 日盤 × 0.6 亮度 | `C_UP_DIM` |
| 夜盤下跌 | 日盤 × 0.6 亮度 | `C_DN_DIM` |
| VWAP | `#F5A623`（金黃） | `COLOR_VWAP` |
| TAIEX | `#00E5FF`（亮青） | `COLOR_TAIEX` |

成交量顏色：基礎顏色 in HSL 空間 +(-10)% 亮度（`VOL_LIGHTEN = -0.1`）。

---

### 3.7 `visualization/chart_builder.py` — 圖表渲染器

#### 視圖組成

```
┌─────────────────────────────────────────────┐
│  主圖 (chart)                                │
│  - K 線（OHLC + 顏色）                       │
│  - TAIEX 對照線（折線）                       │
│  - VWAP（折線）                               │
│  - MA5/10/20/55/EMA660/3300（折線）           │
│  - TXFR2 次月線（折線）                       │
│  - 結算日垂直虛線（副圖上）                    │
├─────────────────────────────────────────────┤
│  副圖 (basis_subchart)                       │
│  - basis = TXF - TAIEX（零軸柱狀，四色）     │
│  - r2_basis = TXFR2 - TAIEX（折線）          │
│  - calendar_spread = TXFR2 - TXF R1（折線）  │
│  - 結算日垂直虛線                             │
├─────────────────────────────────────────────┤
│  成交量圖 (vol_series)                       │
│  - 成交量直方圖（區分日夜盤顏色）              │
└─────────────────────────────────────────────┘
```

#### 週期切換（`on_timeframe_change_cb`）

```python
def _on_timeframe_change(chart, new_tf):
    # 防抖：_is_switching flag 避免切換期間 tick 觸發重算
    self._is_switching = True
    df_processed = self.on_timeframe_change_cb(new_tf)  # 呼叫 get_data()
    self.plot(df_processed)
    self._is_switching = False
```

#### `update_live_bar(live_bar: dict)`

只更新最後一根 K 棒（不重繪全部）：
- 更新 K 棒 OHLCV、顏色
- 更新所有 MA 數值
- 更新 basis、r2_basis、calendar_spread
- 更新 VWAP
- 更新 TAIEX 折線的最後一點

**⚠️ 注意**：`viewer.update_live_bar()` 只支援 `dict` 格式，MA key 的命名規則是 `{type}{period}`（例如 `SMA5`, `EMA3300`），而非 `ma5`。

#### 結算日垂直線（C 項優化後）

```python
# __init__ 時讀取一次
self._settle_date_set = set(pd.to_datetime(pd.read_csv(SETTLEMENT_CSV_PATH)['date']).dt.date)

# plot() 時直接查集合，不重複 I/O
settle_bars = df_kbars[(df_kbars['date_only'].isin(self._settle_date_set)) & hour_mask]
```

---

### 3.8 `view_chart.py` — 主程式（最複雜）

這個檔案承載了幾乎所有業務邏輯的 orchestration，是重構的主要目標。

#### 模組級函數

| 函數 | 功能 |
|------|------|
| `perform_delta_merge()` | 將 Parquet 歷史 + live ticks 合併為完整 K 棒序列 |
| `_incremental_ma_update()` | 增量 MA 計算（O(num_periods)，最終優化） |
| `apply_adjustment()` | 前復權（join_asof 策略，累計 delta 調整） |
| `load_historical_raw()` | 讀取 Parquet 並計算動態日期範圍（防止資料過多） |
| `_detect_today_settlement_date()` | ⚠️ 新增：啟動時免 Parquet 即時判斷今日是否結算日（三層：CSV→第三禮拜三→假日順延），讓背景路徑開盤即能 Smart Rollover。詳見 §4.3 |

#### `perform_delta_merge()`

```python
def perform_delta_merge(df_hist, ticks_df, tf, symbol, is_combined):
    # 1. Resample live ticks → K 棒
    replay_df = resample_to_kbars(ticks_df, tf)
    
    # 2. 對齊欄位（確保 schema 一致）
    replay_df = replay_df.select(df_hist.columns)
    
    # 3. 防重複：移除 Parquet 中被 live 覆蓋的最後一根（未完成 K 棒）
    # 使用 first_replay_ts 作為截斷點
    if "ts" in df_raw.columns and not replay_df.is_empty():
        first_replay_ts = replay_df["ts"][0].replace(microsecond=0)
        df_hist = df_hist.filter(pl.col("ts") < first_replay_ts)
    
    # 4. 垂直拼接
    return df_hist.vstack(replay_df)
```

#### `LiveDeltaManager` 類別

```python
class LiveDeltaManager:
    # 使用 threading.RLock() 保護 live_ticks
    # add_ticks(raw_ticks_list): 
    #   - 解析並轉換 tick 格式
    #   - 追加到 self.live_ticks（pl.DataFrame）
    #   - 超過 26h 的資料自動截斷（上限 = 夜盤+日盤完整週期）
    # get_ticks():
    #   - clone() 後回傳（執行緒安全）
```

**26 小時的原因**：台指期完整的「夜盤開始到次日日盤結束」週期約 22.75 小時（15:00 到次日 13:45），加 3h buffer = 25.75h ≈ 26h。

#### `load_historical_raw()`

有個隱藏的智慧邏輯：**動態計算需要讀取的天數**（避免讀取 20 年資料）。

```python
# 根據 max_bars 和 bars_per_day 計算最少需要多少天
days_needed = math.ceil(max_bars / bars_per_day) + buffer_days

# 從 actual_max_date 往前推算 start_date
effective_start = max(args.date, calculated_start)
```

---

## 4. 關鍵演算法與設計決策

### 4.1 時間對齊：Period Alignment（最難坑）

台指期有兩個開盤時間（日盤 08:45、夜盤 15:00），不從整點起始，導致標準的 `group_by_dynamic` 切出錯誤的 K 棒邊界。

**最終解法**（已在 resampler.py 實作）：
```
原始 ts → 減去 session offset → aligned_ts（對齊到 00:00）
→ group_by_dynamic（整點切割，正確）
→ 加回 session offset → 原始時間
```

**Snap 機制**（收盤殘餘 tick 處理）：
收盤後超出 session 上限的 tick 強制歸入最後一個合法 bucket，避免出現「13:45~13:47」這種 2 分鐘迷你 K 棒。

---

### 4.2 即時 Delta 合併

**問題**：Parquet 只到昨日收盤，當日 tick 從 Kafka 來，兩者的最後一根 K 棒可能重疊（Parquet 的最後一根是當天開盤後第一根，未完成）。

**解法**：以 live ticks 重採樣後的第一根 K 棒時間戳作為截斷點，移除 Parquet 中晚於或等於這個時間的 K 棒，再 vstack 合併。

**Gap Replay 的必要性**：如果程式在盤中啟動（而非開盤前），Kafka 的 offset 從最新開始，中間有 gap。Gap Replay 先抓取 Parquet 最後時間到現在的所有 tick，填入 LiveDeltaManager，確保 K 棒連續。

---

### 4.3 Smart Rollover（結算日換倉）

結算日當天，期交所以現貨指數結算當月合約（TXF R1），次月合約（TXF R2）成為新的主力。

**問題**：結算日當天 TXF R1 的成交量急劇萎縮，K 棒失真。

**解法**：結算日當天的 K 棒強制替換為 TXF R2 的資料。

```python
# 偵測結算日（從 monthly_settlements.csv）
# 使用 ts + 9h offset 完美對齊期交所「交易日」（15:00 夜盤算入隔日）
GLOBAL_SETTLEMENT_DATES = set(...)

# 對結算日 K 棒執行 full join + 條件替換
df_raw = df_raw.join(r2_join, on=join_cols, how="full", coalesce=True)
is_settlement = trading_date_expr.is_in(list(algorithmic_settlements))
replace_exprs = [when(is_settlement & r2_not_null).then(r2_col).otherwise(r1_col)]
```

**⚠️ 重要坑**：full join 會打亂資料排序，事後必須 `.sort(sort_col)` 還原，否則 `fill_null(forward)` 會把舊資料填到新資料上。

#### 啟動即時結算日偵測：`_detect_today_settlement_date()`（⚠️ 新增 2026-06-16）

**問題**：`GLOBAL_SETTLEMENT_DATES` 原本只在 `get_data()` 的**完整路徑**裡建立（需先載入歷史 Parquet 才算得出 `algorithmic_settlements`）。但**快速背景路徑**（live 每 0.5s 增量更新）若在結算日**開盤當下**就跑，可能還沒有 `GLOBAL_SETTLEMENT_DATES` → Smart Rollover 開盤瞬間不生效。

**解法**：在 `main()` 啟動時（`--smart-rollover` 且 `symbol==TXF`）先呼叫 `_detect_today_settlement_date()`，**不依賴任何歷史 Parquet** 就判斷「今日是否結算日」，命中就先 `GLOBAL_SETTLEMENT_DATES = {today_str}`，讓背景路徑開盤即能轉倉；完整路徑稍後再擴充歷史結算日。

```python
# view_chart.py：函式 _detect_today_settlement_date() → (today_str, is_settlement)
# 呼叫點 main()：
if getattr(args, 'smart_rollover', False) and args.symbol == 'TXF':
    _today_str, _is_settlement_today = _detect_today_settlement_date()
    if _is_settlement_today:
        GLOBAL_SETTLEMENT_DATES = {_today_str}   # 先放今天，完整路徑跑完後擴充歷史
```

**三層確認（依可靠度排序）**：
1. **CSV 靜態查找**：`monthly_settlements.csv` 是否含今日交易日（`(now()+9h).date()`）。最可靠，但**當月結算日通常要等結算後才會被補進 CSV**，故開盤當天多半靠下面兩層。
2. **演算法**：當月「第三個禮拜三」（`first_wed_offset=(2-first_day.weekday())%7; third_wed=first_day + (offset+14)d`）。
3. **假日順延（關鍵創新）**：若第三個禮拜三逢國定假日，期交所結算順延到下一交易日。用「**該日的 1m Parquet 是否存在**」當「是否為交易日」的判據，從第三個禮拜三起逐日 +1 往後找（上限 20 天，涵蓋春節長假），直到找到有 Parquet 的工作日。兩個邊界處理得很巧：
   - **candidate > 今日**：候選日還沒到 → 代表今日就是連假後第一個開市日 = 實際結算日 → 回退到今日。
   - **candidate == 今日**：今日日盤進行中、Parquet 尚未產生 → 同樣判定今日為連假後第一交易日 = 結算日。
   - 20 天找不到 → 退回純第三禮拜三；無法讀 `DATA_ROOT` → fallback 只跳週末。

> **設計洞見**：用「Parquet 是否存在」來推斷「是否為交易日」，等於**免費借用資料湖當交易日曆**，避免額外維護假日表——這比純演算法（不懂假日）穩健得多。
> **已移植到 `txf-quant-platform`**（2026-06-16）：`DataEngine._resolve_settlement_day()` 採用同樣的 Parquet-probe 順延，並用真實資料驗證——對 75 個 CSV 歷史月份能重現 74 個的實際結算日（含 2026-02 因農曆年由 2/18 順延到 2/23）。唯一例外 2023-01（3rd Wed 2/18 為交易日，但 TAIFEX 把結算延到農曆年後的 1/30）證明了「CSV 權威、演算法只補未涵蓋月份」才是對的設計：純演算法（即使有順延）無法涵蓋這種特例，故 CSV 有的月份一律以 CSV 為準。

---

### 4.4 增量 MA 計算（最終版優化）

**問題**：原本每 0.5s 重跑 `DataProcessor.process_data(1500 rows)` × 3（TXF + TSE + TXFR2），CPU 高達 46%。

**核心數學**：

EMA 遞推：
```
α = 2 / (span + 1)
EMA[t] = α × close[t] + (1 - α) × EMA[t-1]
```

SMA 滑動窗口遞推：
```
SMA[t] = SMA[t-1] + (close[t] - close[t-period]) / period
```

**實作**（`_incremental_ma_update()`）：

```python
# 是否為新 K 棒（決定索引）
if is_new_bar:
    prev_idx = -1           # cached 最後一行 = 已完成的上一根
    drop_idx = -period      # SMA 掉出窗口的那根
else:
    prev_idx = -2           # 當根仍在更新，prev 是倒數第二行
    drop_idx = -(period+1)  # 位移一位
```

**精度保證**：初始全量計算使用完整歷史（20,000+ 根），EMA3300 已正確收斂。後續每次增量更新只需 O(num_periods) = O(6) 算術，數值永遠精確。

---

### 4.5 VWAP 計算的跨日對齊問題

**問題**：夜盤 `23:00:00` 的 date 是 6/10，但它屬於 6/11 的交易日（因為 15:00 開始的夜盤算入隔日）。若以 `date` 分組計算 VWAP，夜盤和日盤的 VWAP 會斷裂。

**解法**：用 `ts + 9h` 的日期作為 VWAP 分組鍵：
```python
vwap_group_date = (ts + 9h).date()
# 效果：15:00（夜盤開始）+ 9h = 00:00 隔天，完美對齊交易日
```

---

### 4.6 EMA 在截斷資料上的誤差問題

**問題**：EMA3300 需要 ~7,600 根 K 棒才能收斂（1% 誤差閾值）。若只用 tail(1500) 計算，最舊那根 K 棒對今日 EMA 的貢獻仍有 40%。

```
weight(1500 bars ago) = (1 - α)^1500 = (1 - 0.000606)^1500 ≈ 40%
```

**解法**：
1. 初始載入時用**完整歷史**（20,000 根）計算 EMA，確保收斂
2. 將結果快取在 `cache_proc[tf]`
3. 後續背景更新使用**遞推公式**（O(1)），不重新截斷計算

---

## 5. 即時系統運作細節

### 5.1 LiveDeltaManager

> **⚠️ 已重寫 2026-06-16**：本節原本的程式碼與 schema **整段都是錯的**（憑印象寫成「理想化」版本，與實際碼不符）。以下為實際行為（`view_chart.py:22-64`）：
> - 用的是 `threading.Lock()`（**不是 RLock**），屬性名是 `self.lock`（**不是 `_lock`**）。
> - **沒有** `MAX_RETENTION_HOURS` 這個 class 常數；26h 是 `add_ticks` 內**就地計算**的 `_MAX_LIVE_MS = 26 * 3_600_000`（毫秒）。
> - 截斷錨點是**最後一筆 tick 的 epoch-ms**（`live_ticks["ts"][-1] - _MAX_LIVE_MS`），**不是 `datetime.now()`**。`get_ticks` 還特別**不做**第二次 now() 截斷（註解說明：避免凌晨把夜盤資料誤刪）。
> - `add_ticks` **不做任何格式轉換**：`live_ticks["ts"]` 是 **`Int64` 原始 epoch-ms**（不是 Datetime）；`close` 的 `/10000` 是在 `KafkaTickReader` 內就做好的（不是這裡）。ms→Asia/Taipei-naive 的轉換延後到 `perform_delta_merge`。

```python
class LiveDeltaManager:
    def __init__(self):
        self.live_ticks = pl.DataFrame()
        self.lock = threading.Lock()      # 普通 Lock（leaf 臨界區，無重入需求）
        self.simulation_queue = []        # 漸進式重播用（見下）

    def initialize(self, gap_ticks, simulate_flow=False):
        self.live_ticks = gap_ticks       # 由 Gap Replay 的結果 seed
        if simulate_flow and not gap_ticks.is_empty():
            self.simulation_queue = gap_ticks.to_dicts()  # 暫存，待背景執行緒慢慢餵
            self.live_ticks = pl.DataFrame()              # 先清空，讓圖先畫歷史

    def add_ticks(self, new_ticks: list):
        new_df = pl.DataFrame(new_ticks).sort("ts")       # 只排序「本批」，未對合併後整體重排
        with self.lock:
            if self.live_ticks.is_empty():
                self.live_ticks = new_df
            else:
                self.live_ticks = self.live_ticks.vstack(new_df)
                _MAX_LIVE_MS = 26 * 3_600_000             # 26h（毫秒），就地常數
                _cutoff = self.live_ticks["ts"][-1] - _MAX_LIVE_MS   # 錨點 = 最後一列
                self.live_ticks = self.live_ticks.filter(pl.col("ts") >= _cutoff)

    def get_ticks(self):
        with self.lock:
            return self.live_ticks.clone()                # 不做二次截斷
```

**live_ticks Schema**（⚠️ 已修正：`ts` 是原始 epoch-ms 整數，非 Datetime）：
```
ts:                 Int64    (Kafka timestamp_ms，原始 epoch 毫秒；NOT Datetime)
close:              Float64  (已在 KafkaTickReader 內 /10000)
volume:             Int64
underlying_price:   Float64  (現貨指數，已 /10000；供 TAIEX 對照線使用)
```

**`initialize()` 與 `simulation_queue`（漸進式重播，原文未記載）**：
`--simulate-cut-date` + `--progressive` 會讓 `simulate_flow=True`，此時 `initialize` 把 gap ticks 塞進 `simulation_queue` 並清空 `live_ticks`（先畫歷史）；背景執行緒 `start_live_kafka_listener` 再以**每輪最多 500 筆**全速 `add_ticks` 餵回，追上後無縫轉 live。

> **⚠️ 小坑（low）**：`add_ticks` 只排序「本批」，`vstack` 後不重排整體，且 26h 截斷錨在 `[-1]`（最後附加）而非 `max(ts)`。正常 Kafka 單調到達時 `[-1] == max`，故穩態無害；但時鐘飄移/重播亂序時截斷量會抓錯，且 `get_ticks()[-1]` 餵給 live TAIEX/TXFR2 的純量讀取可能非最新。建議改 `vstack(...).sort("ts")` 並用 `["ts"].max()` 當錨。（live-26h-cutoff-anchor-unsorted）

### 5.2 Gap Replay（斷線重播）

```
Parquet 最後一根時間 → last_dt
since_ts_ms = last_dt 的毫秒 timestamp

KafkaTickReader.fetch_gap_ticks(since_ts_ms)
  → 對每個 topic 執行：
    1. offsets_for_times() → 找起始 offset
    2. 批次消費直到 high_watermark
    3. 過濾 ts >= since_ts_ms 的 tick
    4. 回傳 pl.DataFrame
  → 結果餵給 LiveDeltaManager.add_ticks()
  → consumer.subscribe(topics) 切回 live 模式
```

**模擬模式**（`--simulate-cut-date`）：
假裝 Parquet 只有到某個日期，用於測試 live 模式的 Gap Replay 邏輯。

### 5.3 背景執行緒架構

```
Main Thread:
  ├─ lightweight-charts 事件迴圈（圖表互動）
  └─ viewer.plot() → 阻塞等待視窗關閉

Background Thread (daemon=True):
  └─ start_live_kafka_listener()
      ├─ kafka_reader.poll_new_ticks()  ← 每 100ms 一次
      ├─ delta_manager.add_ticks()
      └─ on_tick_cb()  ← 每 0.5s 最多觸發一次 UI 更新
```

**執行緒安全**（⚠️ 已修正 2026-06-16）：
- `LiveDeltaManager.lock`（**普通 `threading.Lock`**）保護 live_ticks 的讀寫 — 臨界區皆為 leaf（純記憶體 vstack/filter/clone，互不巢狀），故 Lock 足夠，無死結/重入風險（已驗證 live-lock-no-reentrancy-deadlock）
- `KafkaTickReader.lock`（**RLock**）保護 Kafka Consumer 物件（UI 執行緒 gap-fetch 與背景 poll 共用單一 librdkafka consumer）
- `_is_switching` flag 防止週期切換中途的 tick 觸發更新
- `last_update_time` 實作 0.5s throttle

---

## 6. 資料 Schema 規格

> 完整的 Data Lake 實體結構（raw_ticks 9 欄、bidask 深度流、調整表）見新增的 **[§12](#12-data-lake-實體結構與完整-schema)**。

### K-Bar Parquet Schema（10 欄，全 tf/商品一致）

```
symbol:       Utf8          商品代碼 (TXF, TSE, TXFR2)
date:         Date          交易日期（夜盤歸入隔日；night = D-1，見 §15 I-09）
ts:           Datetime      K 棒起始時間（label='left'）— ⚠️ 單位不一致見下
session:      Utf8          "Day" | "Night"
open:         Float64       注意：OHLC 全部由 tick 的 close(成交價) 推導，無獨立 open 欄
high:         Float64
low:          Float64
close:        Float64
volume:       Int64
true_pv_sum:  Float64       Σ(close×volume)，供 True VWAP（resample_to_kbars 才有；
                            動態重採樣 resample_kbars 會丟掉 → 見 §15 I-04）
```

> **⚠️ 已修正 2026-06-16 — `ts` time_unit 跨週期不一致（實測於磁碟）**：
> | 週期 | `ts` time_unit | 成因 |
> |------|----------------|------|
> | 5s / 1m / 5m / 1h | `Datetime(us)` | 走 `offset_by` + `group_by_dynamic`；`_snap` 裡的 `pl.duration(microseconds=1)` 把 ns 降成 us |
> | 1d | `Datetime(ns)` | 走 `group_by(['date','session'])` + `ts.first()`，原樣保留 raw 的 ns |
> | raw_ticks | `Datetime(ns)` | `from_epoch(..., 'ns')` |
>
> 跨單位 `concat(how="diagonal")` 會直接 `SchemaError`（已實測）。目前 `load_kbars` 只併同 tf、live path 有 cast 保護，故尚未爆；但任何「1m(us) 與 1d(ns) 混用」的新程式都會中招。建議寫檔前統一 cast（見 §16）。

### 處理後 DataFrame Schema（df_proc）

```
ts:                 Datetime(us)
time:               Datetime(ns)    ← 供 lightweight-charts 使用
date:               Date
session:            Utf8
open/high/low/close: Float64
volume:             Float64
color:              Utf8            K 棒顏色 (hex)
borderColor:        Utf8
wickColor:          Utf8
vol_color:          Utf8
is_up:              Boolean
vwap:               Float64
ma5/10/20/55:       Float64         SMA
ma660/3300:         Float64         EMA
TAIEX:              Float64         (可選：TSE 指數)
basis:              Float64         (可選：TXF - TAIEX)
TXFR2:              Float64         (可選：次月收盤)
r2_basis:           Float64         (可選：TXFR2 - TAIEX)
calendar_spread:    Float64         (可選：TXFR2 - TXF R1)
r1_close_original:  Float64         (可選：smart_rollover 時保留原 TXF R1)
underlying_close:   Float64         (可選：現貨指數)
true_pv_sum:        Float64         (可選：加權價格乘積，供 VWAP）
```

### live_bar Dict（推送給圖表的格式）

```python
{
    "time":          datetime,
    "open":          float,
    "high":          float,
    "low":           float,
    "close":         float,
    "volume":        float,
    "color":         str,
    "borderColor":   str,
    "wickColor":     str,
    "vol_color":     str,
    "session":       str,
    "VWAP":          float,
    "TAIEX":         float,
    "basis":         float,
    "r2_basis":      float,
    "calendar_spread": float,
    "SMA5":          float,    # 注意：格式是 {type}{period}，不是 ma5
    "SMA10":         float,
    "EMA660":        float,
    "EMA3300":       float,
}
```

---

## 7. 已知問題與 Workarounds

### Bug 1: lightweight-charts datetime64[us] 轉換 Bug

**現象**：`Datetime(us)` 送入 lightweight-charts 後時間偏移（因為 pandas 的 `astype('int64')` 計算 us 時有精度問題）。

**Workaround**：強制 cast 為 `Datetime("ns")`：
```python
df_proc = df_proc.with_columns(
    pl.col("time").str.to_datetime(format="%Y-%m-%d %H:%M:%S").cast(pl.Datetime("ns"))
)
```

### Bug 2: 舊版 ETL 的 session 標記問題

**現象**：08:xx 的 K 棒在舊版 Parquet 中被標記為 `Night`。

**Workaround**（在 loader.py 中）：
```python
df = df.with_columns(
    pl.when(pl.col("ts").dt.hour() == 8)
      .then(pl.lit("Day"))
      .otherwise(pl.col("session"))
      .alias("session")
)
```

### Bug 3: Smart Rollover 後 full join 排序被破壞

**現象**：full join 後資料順序混亂，`fill_null(forward)` 產生錯誤。

**Workaround**：full join 後立即 `.sort(sort_col)`。

### Bug 4: EMA3300 在 tail(1500) 時嚴重失準

**現象**：1500 根計算出的 EMA3300 有 ~40% 誤差（最舊那根的權重仍有 40%）。

**Workaround（已實作）**：
- 初始載入用完整歷史計算並快取
- 背景更新用增量遞推公式（`_incremental_ma_update`）

### Bug 5: Int8/Int16 乘法溢出（resampler.py）

**現象**：`dt.hour() * 3600` 在 Polars 中可能因 `Int8` 型別溢出。

**Workaround**：
```python
pl.col("aligned_ts").dt.hour().cast(pl.Int32) * 3600
```

### Bug 6: 結算日邊界偵測（+9h offset）

**現象**：夜盤 15:00 開始的 K 棒屬於「隔日的結算日」，若用 `date` 欄位直接比對結算日，夜盤會錯誤判斷。

**解法（已實作）**：使用 `ts + 9h` 的日期作為結算日比對基準。

> **⚠️ 2026-06-16 review 新增的問題**：本節（Bug 1–6）是「已實作 workaround」的歷史 bug。本次 review 另外經對抗式驗證找出**尚未修掉**的正確性/完整性問題（1d 重複列、EMA adjust 不一致、true_pv_sum 在動態重採樣遺失、tail(1500) 破壞 EMA3300、收盤殘餘 Night bar…）與**安全問題（API 金鑰外洩）**，集中整理於 **[§15](#15-資料正確性與完整性問題清單)**，並在 **[§16](#16-重構任務清單依優先序)** 給出依優先序的修復清單。

---

## 8. 效能分析

### 優化前後對比（2026-06-16 優化後）

| 指標 | 優化前 | 優化後 |
|------|--------|--------|
| 背景更新頻率 | 20次/秒（always） | ~3次/秒（only on tick） |
| 每次 DataProcessor 呼叫 | 3次 × O(1500 rows) | 0次（增量路徑） |
| MA 計算複雜度 | O(4,500 rows) | O(6 pure arithmetic) |
| EMA3300 精度 | ~40% 誤差 | 100% 正確 |
| 6.5h 後 CPU | ~46%（且上升） | ~8-12%（穩定） |
| live_ticks 記憶體 | 無限增長 | 上限 26h |

### 殘餘瓶頸

`perform_delta_merge()` 仍需重採樣全部 live_ticks（O(N_ticks)），因為需要精確計算當根 K 棒的 high/low/volume。這是 txf-quant-platform 重構時的主要優化點。

---

## 9. 重構建議（給 txf-quant-platform）

### 架構層級建議

#### 9.1 維持 Polars 為核心資料框架

Polars 的向量化運算在大資料量時比 pandas 快 10-100x，尤其是 `rolling_mean`、`ewm_mean`、`group_by_dynamic` 等操作。繼續使用。

#### 9.2 狀態機化的即時 K 棒管理器

目前每次 tick 都需要 `perform_delta_merge`（全量重採樣），根本原因是沒有維護「當根 K 棒的 running state」。

**建議設計**：
```python
class KBarState:
    """
    維護當前 K 棒的 running state，支援 O(1) 更新。
    """
    def __init__(self, tf: str, session: str, bar_start_ts: datetime):
        self.tf = tf
        self.session = session
        self.bar_start_ts = bar_start_ts
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = 0
        self.pv_sum = 0  # 用於 VWAP 增量計算
        self._ema_state: dict[int, float] = {}  # {span: prev_ema}
    
    def update(self, tick_close: float, tick_volume: int):
        if self.open is None:
            self.open = tick_close
        self.high = max(self.high or tick_close, tick_close)
        self.low = min(self.low or tick_close, tick_close)
        self.close = tick_close
        self.volume += tick_volume
        self.pv_sum += tick_close * tick_volume
    
    def finalize(self) -> dict:
        """K 棒結束時呼叫，回傳完整 OHLCV dict"""
        ...
    
    def is_tick_in_current_bar(self, tick_ts: datetime) -> bool:
        """判斷 tick 是否屬於當前 K 棒時間桶"""
        ...
```

#### 9.3 分離「計算層」與「渲染層」

目前 `view_chart.py` 把所有邏輯混在一起。建議：

```
計算層 (core/)
  ├─ KBarEngine: 即時 K 棒狀態 + 歷史資料管理
  ├─ IndicatorEngine: 增量指標計算（MA, VWAP）
  └─ DataProvider: Parquet + Kafka 資料源抽象

渲染層 (visualization/)
  ├─ ChartRenderer: lightweight-charts 渲染
  └─ ThemeManager: 配色主題

業務層 (app/)
  └─ ViewChart: orchestration，連接計算層和渲染層
```

#### 9.4 事件驅動架構

目前是 polling 模式（sleep 100ms 輪詢）。可改用事件驅動：

```python
# Kafka consumer 有 tick → 直接觸發
class TickEventBus:
    def subscribe(self, handler: Callable[[Tick], None]): ...
    def publish(self, tick: Tick): ...

# 訂閱者：KBarEngine, IndicatorEngine
# 發布者：KafkaConsumerThread
```

#### 9.5 增量 MA 計算的統一抽象

目前 `_incremental_ma_update` 是一個 view_chart.py 層級的函數。建議移到 `IndicatorEngine`：

```python
class IndicatorEngine:
    def __init__(self, ma_settings: dict):
        self._state = {}  # {tf: {period: prev_ma_value}}
    
    def initialize(self, tf: str, df_full: pl.DataFrame):
        """用完整歷史初始化 MA 狀態"""
        for period, cfg in self.ma_settings.items():
            last_val = df_full[f"ma{period}"][-1]
            self._state.setdefault(tf, {})[period] = float(last_val)
    
    def update(self, tf: str, new_close: float, is_new_bar: bool) -> dict:
        """增量更新，回傳新的 MA 數值"""
        ...
    
    def compute_full(self, df: pl.DataFrame, tf: str) -> pl.DataFrame:
        """全量計算（初始載入用）"""
        ...
```

#### 9.6 VWAP 增量計算

目前快速路徑中 VWAP 只是「carry forward」（沿用上一個值）。建議在 `KBarState` 中追蹤每個 session 的累積 pv_sum 和 volume，實現真正的增量 VWAP：

```python
class SessionVWAPTracker:
    """追蹤當日 session 的累積 VWAP 狀態"""
    def __init__(self):
        self.cum_pv = 0.0
        self.cum_vol = 0
    
    def update(self, bar_pv_sum: float, bar_volume: int) -> float:
        self.cum_pv += bar_pv_sum
        self.cum_vol += bar_volume
        return self.cum_pv / self.cum_vol if self.cum_vol > 0 else None
    
    def reset(self):
        """換 session 時重置"""
        self.cum_pv = 0.0
        self.cum_vol = 0
```

#### 9.7 多商品配置化

目前 TXF/TSE/TXFR2 的關係是 hardcoded 在 view_chart.py 中。建議改為配置：

```yaml
# instruments.yaml
instruments:
  TXF:
    parquet_symbol: TXF
    kafka_topic: txf-tick
    reference:
      - symbol: TSE
        column: TAIEX
        display: "TXF-TAIEX Basis"
        kafka_field: underlying_price
      - symbol: TXFR2
        column: TXFR2
        display: "Calendar Spread"
        kafka_topic: txfr2-tick
```

#### 9.8 Time Zone 統一

目前系統使用 naive datetime（沒有 tz info），依賴「知道這是 Asia/Taipei」的隱式假設。建議統一：

```python
# 所有 ts 均使用 tz-aware
ts = datetime.now(tz=ZoneInfo("Asia/Taipei"))

# Polars 中
pl.Datetime("us", "Asia/Taipei")
```

#### 9.9 輸出的 `time` 欄位（Datetime("ns") Bug）

目前需要用 `Datetime("ns")` 繞過 lightweight-charts 的 bug。若重構時使用新版 LWC，可移除這個 workaround。追蹤 [lightweight-charts-python issue](https://github.com/louisnw01/lightweight-charts-python) 關於 timezone 的修復狀態。

---

## 10. CLI 使用方式

> **⚠️ 已修正 2026-06-16 — 旗標補充**：
> - `--tfs` 實際預設 = `['1m','5m','15m','30m','1h','4h','1d','1d (comb)']`（原文範例漏了 `30m` 與 `1d (comb)`）。
> - `1d (comb)` 是 UI 偽週期：`get_data` 偵測到結尾 ` (comb)` 會把 `is_combined=True` 並去掉後綴，等效強制合併日夜盤（不必再加 `--combined`）。
> - `--combine` 只是 `--combined` 的 alias（`dest='combined'`），兩種拼法等價。
> - 還有兩個 Kafka 旗標原文未列：`--kafka-broker`（預設 `192.168.1.50:9092`）、`--kafka-topic`（預設 `txf-tick`）。

```bash
# 基本看盤（日線，歷史資料）
python view_chart.py --symbol TXF --tf 1d

# 即時看盤（Kafka，5m 週期）
python view_chart.py --symbol TXF --tf 5m --live

# 合併日夜盤
python view_chart.py --symbol TXF --tf 5m --live --combined

# 前復權
python view_chart.py --symbol TXF --tf 1d --adjust

# 結算日換倉（Smart Rollover）
python view_chart.py --symbol TXF --tf 5m --live --smart-rollover

# 不顯示 TSE 對照線
python view_chart.py --symbol TXF --tf 5m --live --no-tse

# 限制載入數量
python view_chart.py --symbol TXF --tf 1m --max-bars 10000

# 自訂可用週期
python view_chart.py --tfs 1m 5m 15m 1h 4h 1d

# 指定日期範圍
python view_chart.py --symbol TXF --tf 1d --date 2024-01-01 --end-date 2024-12-31

# 模擬 live 模式（測試 Gap Replay）
python view_chart.py --symbol TXF --tf 5m --live --simulate-cut-date 2026-06-10

# 漸進式重播
python view_chart.py --symbol TXF --tf 5m --live --simulate-cut-date 2026-06-10 --progressive
```

---

## 11. 資料攝取 / ETL 管線（Ingestion）

> ⚠️ 新增 2026-06-16。原文 §1–§10 幾乎只寫「即時看盤 (`view_chart.py`)」這一半，完全沒寫「攝取/ETL」這一半。本節補齊。

### 11.0 ETL 資料流

```
Shioaji API ──fetch_ticks──▶ raw_ticks/*.parquet ──resample_to_kbars──▶ kbars/{tf}/*.parquet
   (永豐)        (逐日逐商品)      (原子 tick，9 欄)        (5 個週期)         (OHLCV + true_pv_sum)
                                          │
            增量：raw 檔已存在就「跳過下載、改讀本地」（main_etl.py:46-66）
```

### 11.1 `adapters/shioaji_source.py` — `ShioajiSource`

| 方法 | 用途 / 重點 |
|------|------|
| `__init__` | `sj.Shioaji(simulation=True)` — ⚠️ **`simulation=True` 硬編碼**，無 env 開關，要切正式帳得改碼。 |
| `connect()` | 冪等（`if not is_connected`）。`login()` 後印 `accounts[0].person_id`（假設帳號清單非空，否則 IndexError）。 |
| `get_contract(code)` | 工廠：`TXF→Futures.TXF.TXFR1`(近月)、`TSE→Indexs.TSE.TSE001`(現貨加權指數)、`TXFR2→Futures.TXF.TXFR2`(次月)，其餘 `ValueError`。需先 login。 |
| `fetch_ticks(date, code)` | 回 Polars df。base 欄 `{ts,close,volume}`；**TXF/TXFR2 另含** `bid_price/bid_volume/ask_price/ask_volume/tick_type`(內外盤 1外/2內/0未知)；TSE 只有 base。`ts` 由 `from_epoch(..., 'ns')`。注入 `symbol`、欄序 `ts,symbol,...`。無資料回空 df。 |
| `report_usage()` / `logout()` | 印 `api.usage()` / `api.logout()`。⚠️ `logout` **不重設** `is_connected`（重用物件會有殘留狀態）。 |

### 11.2 `main_etl.py` — `run_pipeline(date_str, shared_source=None)`

- `TARGET_SYMBOLS = ['TXF','TSE','TXFR2']`。
- **每商品**：先看 `raw_ticks/{symbol}/{year}/{month}/{date}_{symbol}_ticks.parquet` 是否存在 → 存在就讀本地、**跳過下載**（無內容/日期驗證，stale 檔會被永久信任）；否則 `fetch_ticks` 下載並存檔。
- **每週期 resample**，分流儲存：
  - **1d**：讀年檔 → `concat → unique(subset=['ts'], keep='last') → sort → 寫回**（append-merge，冪等但 dedup key `ts` 脆弱）。印 `Total days: len//2`（假設每天 2 個 session；⚠️ **TSE 是 Day-only，所以 //2 少算一半**）。
  - **intraday(5s/1m/5m/1h)**：直接 `write_parquet` **覆蓋**日檔。
- `is_local_session` 控制：自建的 source 在 `finally` 登出；外借的（batch）不登出。
- ⚠️ 整段包在 `except Exception as e: print(...)`，失敗只印一行、**不回非零碼**（batch 補資料時缺口難偵測）。

### 11.3 `batch_run.py` — 歷史補檔

- 建**單一** shared session（登入一次），整段日期共用 → 省 quota。
- `pd.date_range(start, end, freq='D')` **含週末**（刻意，為抓週六補班日；非交易日靠 `fetch_ticks` 回空跳過）。
- ⚠️ `START='2020-01-01' / END='2022-12-31'` **硬編碼**（無 argparse，要改得改碼）。
- module-level `sys.stdout = io.TextIOWrapper(..., 'utf-8')`（避免 Windows 印 emoji 崩潰）。

### 11.4 重處理腳本

- **`fix_kbars.py`**：掃所有 `raw_ticks/**/*_ticks.parquet`，重算 **intraday**（排除 1d）並覆蓋。用於修舊資料的 session/對齊。
- **`scripts/etl_true_vwap.py`**：全量重鑄 raw → **所有 tf（含 1d、含 `true_pv_sum`）**的 backfill。⚠️ **1d 用 `concat(df_list).sort('ts')` 沒有 `.unique()`** → 這就是 §15 **I-01（1d 年檔重複列）** 的根因。

### 11.5 排程（`AutoRun.md`）

macOS `launchd`（`com.garrett.shioaji.txf_tse_collector`）：週一–五 13:46（日盤收盤後）、週六 05:01 或登入（夜盤結算）執行 `python main_etl.py`；路徑寫死 `/Users/gtai/Projects/txf-data-lake`（mac 採集機）。Windows 端則靠 `x.txt` 內的指令手動跑。**沒有交易日曆模組**（假日/補班日全靠「空抓即跳過」）。

---

## 12. Data Lake 實體結構與完整 Schema

> ⚠️ 新增 2026-06-16。實測 `DATA_ROOT = D:/txf-data`，共 **26,564 個 parquet**。

### 12.1 分區策略（三區）

```
D:/txf-data/
├── raw_ticks/{symbol}/{year}/{month}/{date}_{symbol}_ticks.parquet   # 原子 tick；symbol∈{TXF,TSE,TXFR2}
├── raw_ticks/TXF/{year}/.../{date}_TXF_bidask.parquet                # ⚠️ 5 檔委託簿深度流（見 12.3）
├── kbars/{tf}/{symbol}/{year}/{date}_{symbol}_{tf}.parquet           # 分時：tf∈{5s,1m,5m,1h}；只到 year，無 month
├── kbars/1d/{symbol}/{symbol}_1d_{year}.parquet                      # 日線：年檔，無 year 子目錄
└── adjustments/*.csv                                                  # 調整/結算表（見 §13）
```
> 注意：intraday kbars 只分到 **year**，raw_ticks 分到 **month** — glob 時要注意層級不同。

### 12.2 `raw_ticks` Schema

| 欄位 | 型別 | 說明 |
|------|------|------|
| ts | `Datetime(ns)` | tick 時間（naive 台北牆鐘） |
| symbol | Utf8 | TXF / TSE / TXFR2 |
| close | Float64 | 成交價 |
| volume | Int64 | 成交量 |
| bid_price / bid_volume | Float64 / Int64 | **僅 TXF/TXFR2**；最佳買價/量（1 檔） |
| ask_price / ask_volume | Float64 / Int64 | **僅 TXF/TXFR2**；最佳賣價/量（1 檔） |
| tick_type | Int8 | **僅 TXF/TXFR2**；內外盤 **1=外盤(主買)/2=內盤(主賣)/0=未知** |

> TXF/TXFR2 = **9 欄**，TSE = **4 欄**。`bid/ask/tick_type` 是 CVD/COFI/COBI 的關鍵原料，**但 `resample_to_kbars` 全丟**（見 §14）。夜盤跨日：前一晚 15:00 的夜盤被歸入隔日的交易日檔。

### 12.3 ⚠️ 委託簿深度流 `*_TXF_bidask.parquet`（完全未被使用）

實測 **157 個** `*_TXF_bidask.parquet`（TXF only；2025 年 27 個 + 2026 年 130 個；**最早 2025-12-01**，即 Kafka 時代才有）。10 欄，對應 protobuf `BidAsk`：

```
timestamp_ms:Int64, code:String('TXFL5' 之類的活躍月代碼，非 'TXF'),
bid_total_vol:Int64, ask_total_vol:Int64,
bid_price:List(Int64,×10000), bid_volume:List(Int64), diff_bid_vol:List(Int64),
ask_price:List(Int64,×10000), ask_volume:List(Int64), diff_ask_vol:List(Int64)   # 各 5 檔深度
```
> 價格 Int64 ×10000（實測 `bid_price[0]=276490000 → 27649.0`）。**ETL/resampler/kafka_reader 全都不讀它** — 這是 **COBI 唯一的多檔深度來源**，目前白存（見 §14 COBI）。

### 12.4 K-Bar Schema 與 time_unit

見 **[§6](#6-資料-schema-規格)**（10 欄；含 `true_pv_sum`；us/ns 跨週期不一致表）。

---

## 13. 還原價差 / 除息調整邏輯

> ⚠️ 新增 2026-06-16。原文只在 §3.8 用一行帶過 `apply_adjustment`。

### 13.1 `txf_adjustment_table_final.csv`（換月還原表）

欄位 `date, delta, r1_settle, r2_settle, cum_delta`，**75 列**（2020/03/18 ~ 2026/05/20，每個結算日一列）。實測算術：

- `delta = r2_settle − r1_settle`（結算當天「次月 − 近月」的跳空價差），全 75 列 0 例外。
- `cum_delta` = `delta` 的**「含當列」反向後綴和**（inclusive suffix-sum；最新一列 = 自身 delta，最舊列累積最多）。

`apply_adjustment(df, path, tf)`（`view_chart.py:176-215`）：只用 `date + cum_delta` → 解析 `%Y/%m/%d`、建 `adj_dt = date + ' 13:50:00'` → `join_asof(strategy='forward')`（1d 用 date，intraday 用 ts）→ `cum_delta` 補 0 → **加到 OHLC**（`open/high/low/close + cum_delta`）。**volume 不調整**（正確）。

> **⚠️ 兩個坑**：
> 1. `cum_delta` 錨在「現在」（最後一列=自身），**新增一個結算列會位移全部 cum_delta** → 任何快取/匯出的還原序列都會 stale。建議加 as-of-date 欄做可重現性。
> 2. `--adjust` 時 `apply_adjustment` 在 §`get_data` **basis 計算之前**就改了 OHLC，於是 `basis = (還原後 close − 未還原 TAIEX)` 是個 malformed 量。**乾淨的「還原價差」其實不存在**（見 §14）。

### 13.2 其他調整/結算檔

- `monthly_settlements.csv`：`date, contract(YYYYMM), r1_settle`，75 列 → 供結算日垂直線（`SETTLEMENT_CSV_PATH`）。
- `adjustments/taifex_raw/*.csv`：TAIFEX 原始期貨每日結算（cp950/big5 中文表頭，6 個年檔 + 5 個 2026 單日結算檔）→ 是**真正交易日曆/結算日**的權威來源，目前未被程式使用（可拿來取代第三週三演算法，見 §16）。

---

## 14. 指標覆蓋表（現有 vs 缺漏）

> ⚠️ 新增 2026-06-16。回應使用者點名的 CVD/COFI/COBI/sigma bands/volume profile/還原價差。**全專案 grep 僅 `view_chart.py:524` 一句註解 `Order Flow 靈魂`，零實作。**

### 14.1 已實作 ✅

| 指標 | 位置 | 備註 |
|------|------|------|
| OHLC | `resampler.py:99-106` | 全由 tick `close` 推導 |
| **True VWAP** | `processor.py:90-105` | `Σtrue_pv_sum / Σvolume`，over `[vwap_group_date, session]` |
| SMA 5/10/20/55 | `processor.py:52-60` | `rolling_mean` |
| EMA 660/3300 | `processor.py:57-58` | `ewm_mean`（⚠️ adjust 不一致，見 §15 I-02） |
| basis / r2_basis / calendar_spread | `view_chart.py:881/888/893` | TXF−TAIEX / TXFR2−TAIEX / TXFR2−R1 |

### 14.2 缺漏 ❌ 與「如何補」

| 指標 | 狀態 | 原料在哪 | 卡在哪 | 建議 |
|------|------|---------|--------|------|
| **CVD**（累積量差） | 缺 | `tick_type`（raw_ticks 有、Kafka `Tick` 也有） | `resample_to_kbars` 與 `kafka_reader` **都丟** | 純 transform gap。resample 加 `delta_vol = Σvol(tick_type==1) − Σvol(tick_type==2)`，processor 累加。可從**現有 raw_ticks backfill，免重抓** |
| **COFI**（累積委託流不平衡） | 缺 | 同 CVD（`tick_type`） | 同上 | 同 CVD：另存 `buy_vol/sell_vol` per bar，`COFI=Σ(buy−sell)` |
| **COBI**（累積委託簿不平衡） | 缺 | best bid/ask（raw_ticks，1 檔）+ **`BidAsk` 5 檔深度**（`*_bidask.parquet` 2025-12 起 / protobuf 已定義） | raw 的 bid/ask 被 resample 丟；`BidAsk` **無任何 consumer** | feed-dependent，較重。消費 `*_bidask.parquet`：`OBI=(bid_total−ask_total)/(bid_total+ask_total)`，`COBI=ΣOBI`。歷史早於 2025-12 只能用 1 檔 imbalance |
| **sigma / Bollinger bands** | 缺 | 只需 `close`（已有） | 純沒寫 | processor 加 `rolling_std`，畫 `MA±kσ` 或 `VWAP±σ`。**無資料缺口** |
| **volume profile**（價量分布/TPO） | 缺 | raw_ticks `close+volume` | 只沿時間軸聚合，從不按價格分箱 | 由 raw_ticks 依 `round(close/tick_size)` 分箱 → sidecar parquet（不塞進 OHLC schema），算 POC/VAH/VAL；配 `tick_type` 可做 delta profile |
| **還原價差**（乾淨版） | 缺 | 需 adjusted close 與對應現貨 | 見 §13.1 坑 2 | 新增專屬 `adjusted_basis` 欄，先與使用者確認語意 |

### 14.3 單一阻斷點

`resample_to_kbars`（`resampler.py:76-177`）是 **raw→kbar 唯一 transform**，只吐 `OHLCV + true_pv_sum`，把 `bid/ask/tick_type` 全部丟掉；`kafka_reader` 同時丟 `tick_type/total_volume`。**所有缺漏的 order-flow 指標都卡在這兩個 drop site** — 原料其實在磁碟（歷史）與線上（live）都有。

---

## 15. 資料正確性與完整性問題清單

> ⚠️ 新增 2026-06-16。經 9 面向、對抗式驗證（confirmed/refuted/adjusted）後彙整。嚴重度：🔴 critical／🟠 high／🟡 medium／🟢 low。

| ID | 級 | 位置 | 問題（現況） | 建議 |
|----|----|------|------|------|
| **I-SEC** | 🟡 | `.env` | API 金鑰以明文存放於工作目錄。`.env` 已 gitignore、**未進 git 史**（2026-07-21 複驗：全歷史無金鑰賦值、無 dangling blob、.gitignore 自首個 commit 即生效）。 | 定期輪換；加 `.env.example` 佔位；修 `README.md`（別叫人把金鑰寫進原始碼）。 |
| **I-01** | 🟠 | `etl_true_vwap.py:80` | **1d 年檔大量重複列**（實測 TXF 2022=63、2023=70、2024=95、2025=62 dup；某 `(date,session)` 重複 16 次）。reprocessor `concat().sort()` 無 `unique`。`_aggregate_sessions` 會 `volume.sum()` → **灌水並可能毀 OHLC**。 | dedup key 改 `['date','session']`（`main_etl.py:89` 與 reprocessor 都改）；對既有年檔跑一次性去重；修 `Total days: len//2`。 |
| **I-02** | 🟠 | `processor.py:58` vs `view_chart.py:154-158` | **EMA 模型不一致**：全量路徑 `ewm_mean` 用 polars 預設 `adjust=True`，live 遞推用 `adjust=False`。EMA3300 約 **6pt 持續偏移**，且每次全量重算就跳一下。 | `processor.py:58` 明確加 `adjust=False`（標準交易者 EMA，也是唯一能 O(1) 遞推的形式）；加回歸測試。 |
| **I-03** | 🟠 | `resampler.py:194-200` | `resample_kbars`（kbar→kbar）**不透傳 `true_pv_sum`** → 動態週期（15m/30m/4h…）的 VWAP 退化成 `(H+L+C)/3` 近似。 | aggs 加 `pl.col('true_pv_sum').sum()`（子棒可加，數學精確）並補進 `desired_order`。 |
| **I-04** | 🟠 | `main_etl.py:87-96`、各 `write_parquet` | **無原子寫入/無驗證**：1d 讀同檔再覆蓋，中斷毀整年；`"Merge error, overwriting"` fallback 會**靜默丟棄整年歷史**只寫當天。 | 寫 `*.tmp` 再 `os.replace()`；fallback 改 fail-fast；寫前加 schema/dtype/time_unit assert + 寫後 row-count 回讀。 |
| **I-05** | 🟠 | `view_chart.py:832` | **硬編碼 `ADJ_PATH = D:\txf-data\...`** 繞過 `DATA_ROOT/ADJUSTMENTS_DIR`；改 `DATA_ROOT` 後 `--adjust` 靜默顯示未還原價。 | 路徑進 `config/settings.py`；缺檔且 `--adjust` 時大聲 log/raise。 |
| **I-06** | 🟠 | `view_chart.py:177,760`、`chart_builder.py:87,143,269`、`main_etl.py:108` | **多處靜默吞錯**（`except: pass`／blanket catch）：缺調整檔回原值、settlement CSV、live JS 更新、ETL 整段。 | 收斂成具型別 except + logging；ETL 收集逐日失敗並非零退出。 |
| **I-07** | 🟠 | `view_chart.py:487-928` | `get_data` 是 **~442 行 god-closure**（IO+transform+rollover+join+adjust+render-prep），巢狀於 `main()`，**無法 import/單測**（`test_bug.py` 的 `from view_chart import get_data` 其實會直接失敗）。 | 升成 `ChartDataService` class，方法切開、cache 變實例屬性（對齊鐵律 1/2）。 |
| **I-08** | 🟡 | `view_chart.py:721-722, 925` | 背景全量路徑**先 `tail(1500)` 再算 EMA** 才寫入 `cache_proc` → 冷快取時切到新週期，seed 的 EMA3300（span 3300 > 1500）**約 5.8pt 偏移**並被後續遞推永久繼承。**與 §4.6「不因 tail(1500) 失準」的宣稱矛盾**。 | seed cache 的那條路徑不可 tail(1500)；只裁「送圖的列」，EMA 用未截斷資料算。 |
| **I-09** | 🟡 | `kafka_reader.py:57,65,70,74,110,114` | **單一 partition 假設**（partition 0 硬編碼、`assign` 不 `subscribe`）→ topic 一旦 repartition，其餘 partition 的 tick **靜默丟失**。 | 列舉全部 partition 或 `connect()` 加 `assert len(partitions)==1`。 |
| **I-10** | 🟡 | `view_chart.py:442,469`、`chart_builder.py:288-291` | **naive `.timestamp()` 依賴主機時區**：gap-fetch 起點/fallback/live-bar epoch 在非台北主機（CI/UTC VM）會位移 8h。目前剛好對是因本機 UTC+8。 | 改 `zoneinfo('Asia/Taipei')`-aware 或 `(naive-8h).replace(tzinfo=UTC)`；統一文件化 parquet ts = naive 台北、Kafka ms = UTC。 |
| **I-11** | 🟡 | `view_chart.py:875,881`、`chart_builder.py:151` | **現貨/期貨收盤時間不一**：TSE 日盤 13:33 收、TXF 13:44 收、TSE 無夜盤。basis 在 13:34–13:44 與**整個夜盤**是 forward-fill 的 stale spot（夜盤 basis 經濟上無意義仍照畫）；結算線又用硬編碼 13:30。 | 夜盤/非重疊區的 basis 標 null 或區別呈現；對齊真實 13:33/13:44。 |
| **I-12** | 🟡 | `resampler.py:57-72` + `calendar_rules.py:7` | **收盤殘餘 Night bar**：tick 落在 `≥13:45:05` 會被標 Night，經 −15h 對齊在**每個** sub-day tf 產生 1 根假 Night bar（掛在 03:00–04:55、日盤日期）。目前實資料 0 筆觸發（latent），但脆弱。 | snap 條件涵蓋「Night 標記但原始 tod 在日盤收盤窗」的 tick；加 13:45:05 邊界測試。 |
| **I-13** | 🟡 | §6 / `resampler.py` | **ts time_unit 跨週期不一致**（intraday us、1d/raw ns）。跨單位 `concat` 直接 `SchemaError`。 | 兩個 resampler 與 ETL 寫檔前統一 `cast_time_unit`；既有 1d 年檔一次性遷移；`load_kbars` 加 assert。 |
| **I-14** | 🟡 | `resampler.py:99-110`、`kafka_reader.py:96-101` | **order-flow 原料被丟**（bid/ask/tick_type/total_volume）。見 §14。 | resample 與 kafka_reader 透傳並聚合 order-flow 欄。 |
| **I-15** | 🟡 | 全專案 | **無中央 config**（settings 為扁平常數）；`print()` logging（69 處/11 檔，唯 kafka_reader 用 `logging`）；`simulation=True` 硬編碼。 | 引入 pydantic `BaseSettings`/dataclass；統一 logger；simulation 由 config 控。 |
| **I-16** | 🟡 | `tests/*`、`requirements.txt` | **無真正測試**（`tests/` 無任何 assert、是手動看圖腳本）；**無 CI**；**requirements 未鎖版本**；無 pandera/pydantic 資料契約。 | pytest（resampler/loader/processor 對 fixture parquet 斷言）+ pandera kbar 契約 + 鎖版本 + 最小 CI（對齊鐵律 5）。 |

**其他 🟢 low（程式品質，不影響當下數字）**：
- **重複碼**：session 對齊數學（`resample_to_kbars` ↔ `resample_kbars`，QA-01）；`+9h` 交易日重寫 3 處（QA-02）；第三週三結算演算法散落 + `chart_builder` 另讀 CSV（QA-03）；週末 fallback 3 份且皆不懂假日（QA-04）；session 配色 expr 重複且繞過 `get_color`（QA-05）。
- **magic numbers**：26h/0.5s/0.1s/0.001/1500/20000/9h/8h45m/15h/2000/200（QA-08，且 price 與 volume 共用 0.001 epsilon 不妥）。
- **命名**：`ma{d}` vs 圖例 `SMA{d}/EMA{d}` vs `MA_SETTINGS` 用 int key（`int()` round-trip 脆弱，QA-11）；`TSE`/`TSE001`/`TAIEX` 三名同物；`combine/combined/comb` 三拼法（QA-12）。
- **兩條交易日軸並存**：parquet `date`（夜盤=D-1）vs `(ts+9h).date()`（夜盤=D）— VWAP/結算用後者、1d rollover join 用前者，未來 CVD/COFI 要選定一軸（tz-08/tz-11）。
- **健壯性**：`resample_kbars` 無 `session` 欄會 `ColumnNotFoundError`（RS-04）；`_timeframe_to_seconds` 對 `'1d'` 回 3600 的潛在地雷（RS-05）；`filter(volume>0)` 丟零量 bucket、gap 不補（RS-06）；`_aggregate_sessions` 丟 `true_pv_sum/symbol`（QA-21）。
- **註解寫反**：週末 fallback 註解把 Polars `weekday()==5` 標成「週六」，其實 Polars 是 Mon=1..Sun=7，==5 是**週五→週一（正確）**；真正罕見 bug 是補班日「週六 dated session → 週日」（tz-02，已 refute 原 finding）。
- **殘留檔**：`x.txt`、`tests/x.txt`、`*.log`、空的 `to_be_checked/`；`README`/`AutoRun` 與碼漂移（QA-19/QA-20）。

**✅ 已查核「無誤」（避免未來誤改）**：SMA 增量 drop-index 數學正確（live-sma-dropindex-correct）；Lock 模型無死結（leaf 臨界區）；`perform_delta_merge` seam 截斷正確（us/us 對齊、1d 被 gate 擋掉）；snap bucket 數學正確（chain vs direct 0 誤差）；Int8→Int32 cast 必要且完整；26h 截斷單位正確（僅錨點小坑，見 §5.1）。

---

## 16. 重構任務清單（依優先序）

> ⚠️ 新增 2026-06-16。對齊 `txf-quant-platform` 五大鐵律（介面隔離／單向唯讀資料流／訂單狀態機／零硬編碼／測試即文件）。架構層級建議見 §9。

### P0 — 安全 / 資料毀損（立即）
1. **[I-SEC]** 定期輪換 API 金鑰；建 `.env.example`；修 README 金鑰指引。
2. **[I-01]** 修 `etl_true_vwap.py` 的 1d dedup（key=`['date','session']`），對既有年檔跑一次性去重，修 `Total days` 報數。
3. **[I-04]** 所有 `write_parquet` 改「temp + `os.replace()`」原子寫入；1d 的 "merge error → overwrite" fallback 改 fail-fast。

### P1 — 正確性（會影響顯示/指標數字）
4. **[I-02]** `processor.py:58` EMA 加 `adjust=False`，與 live 遞推一致。
5. **[I-03]** `resample_kbars` 透傳 `true_pv_sum`，恢復動態週期的 True VWAP。
6. **[I-08]** seed `cache_proc` 的路徑不得 `tail(1500)`；EMA 用未截斷資料算。
7. **[I-05]** ADJ_PATH 進 `config`，缺檔大聲報。
8. **[I-13]** 統一 kbar `ts` time_unit（建議 us），遷移既有 1d 年檔。
9. **[I-11]** basis 在夜盤/非重疊區標 null，對齊真實 13:33/13:44。

### P2 — 架構 / 可維護（鐵律落地）
10. **[I-07]** 把 `get_data` 442 行 god-closure 拆成 `ChartDataService`（鐵律 1/2：介面隔離、唯讀單向流）。
11. **[I-06]** 統一 logging、停止吞錯。
12. **[I-15]** 中央 config 物件 + `simulation` 旗標化（鐵律 4：零硬編碼）；順手收掉 §15「重複碼/magic numbers/命名」清單（抽 `align/restore_session`、`trading_date_expr`、`get_settlement_dates`、`session_color_expr` 共用 helper）。
13. **[I-16]** pytest + pandera/pydantic kbar 契約 + 鎖版本 + 最小 CI（鐵律 5：測試即文件）。
14. **[I-09]** Kafka 列舉 partition 或 assert 單 partition；**[I-10]** 全面 tz-aware。

### P3 — 指標擴充（回應使用者需求，對齊 §14）
15. 讓 `resample_to_kbars` **order-flow-aware**：由 `tick_type` 算 `buy_vol/sell_vol/delta_vol` 寫入 kbar（可從現有 raw_ticks backfill）→ 解鎖 **CVD / COFI**。
16. 在 `kafka_reader` 補 `tick_type/total_volume`；新增 `BidAsk` consumer 消費 `*_bidask.parquet` 深度 → **COBI**。
17. `processor` 加 `rolling_std` → **sigma / Bollinger bands**（無資料缺口）。
18. raw_ticks 價格分箱 sidecar → **volume profile（POC/VAH/VAL）**。
19. 新增乾淨 `adjusted_basis`（**還原價差**），先與使用者確認語意。

---

*文件初版生成：2026-06-16*  
*最後深度 review：2026-06-16（逐檔實讀 + 9 面向對抗式驗證；新增 §11–§16，修正 §3.1/§3.4/§3.5/§5.1/§5.3/§6/§7/§10）*  
*涵蓋版本：txf-data-lake 最終優化版（含增量 MA 計算）*
