# txf-quant-platform (量化交易平台) 架構藍圖

> **專案定位**：這是一個專為「自動化交易」、「極速回測」與「策略開發」打造的專業級量化平台。

基於第一性原理，並吸取了 `txf-data-lake` 專案中 UI、資料與邏輯逐漸耦合的教訓，新專案將嚴格遵守**「事件驅動架構 (Event-Driven Architecture)」**。最核心的目標是：**同一支策略程式碼，必須能夠「一字不改」地同時運行在回測環境與實盤環境中。**

---

## 🏗️ 核心架構設計 (Directory Structure)

專案將嚴格劃分為五個獨立的模組，各司其職：

```text
txf-quant-platform/
├── core/                  # 🧠 [共用核心] 演算法與資料結構
│   ├── data_engine.py     # 繼承完美的 True VWAP 邏輯，負責高效能特徵運算
│   └── types.py           # 嚴謹的資料定義 (Tick, KBar, Signal, Order, Position)
│
├── feeds/                 # 👁️ [感知器] 市場資料來源
│   ├── historical.py      # 讀取 Data Lake (Parquet)，提供極速回測資料流
│   ├── live_kafka.py      # 訂閱 Kafka，提供實盤毫秒級 Tick
│   └── replayer.py        # 事件回放器 (將歷史資料偽裝成即時事件餵給策略)
│
├── execution/             # 🦾 [執行器] 券商與風控
│   ├── broker_base.py     # 統一的下單介面 (Interface)
│   ├── backtest_broker.py # 虛擬券商 (模擬撮合、計算手續費/滑價、維持保證金)
│   ├── shioaji_broker.py  # 實體券商 (永豐 API 實盤串接)
│   └── risk_manager.py    # 風控守門員 (最大口數限制、當日虧損強制平倉)
│
├── strategies/            # 🎯 [策略庫] 量化交易邏輯
│   ├── base_strategy.py   # 策略模板 (定義 on_tick, on_bar 等生命週期)
│   └── vwap_momentum.py   # 範例策略：利用 True VWAP 進行動能突破
│
└── app/                   # 🚀 [進入點] 啟動程式
    ├── run_backtest.py    # 執行回測，產出績效報告
    └── run_live.py        # 啟動實盤自動交易
```

---

## 🔄 核心運作邏輯：事件驅動 (Event-Driven)

不論是回測還是實盤，資料的流向都是單向且一致的：
1. **Feed (資料源)** 產生一個 `Tick` 或 `Bar` 事件。
2. 事件送入 **Data Engine** 進行處理，抽出最新的 `true_pv_sum`、`VWAP` 與各項特徵。
3. 引擎呼叫 **Strategy (策略)** 的 `on_bar()` 或 `on_tick()` 方法。
4. 策略根據最新數據，決定呼叫 `self.buy()` 或 `self.sell()`。
5. 指令傳遞給 **Broker (券商)**。回測時 `BacktestBroker` 會偷偷扣除滑價並記錄損益；實盤時 `ShioajiBroker` 會將真實委託送往交易所。

---

## User Review Required

> [!IMPORTANT]
> **開放性問題討論 (Open Questions)**
> 
> 在我為您建立這些基礎架構與目錄之前，有兩個核心設計決策想聽聽您的意見：
> 
> 1. **關於回測的精準度**：您希望回測系統預設是基於「K棒 (Bar-by-Bar)」進行模擬撮合（速度極快，幾秒鐘跑完幾年），還是要支援「微秒級逐筆 (Tick-by-Tick)」的還原撮合（速度較慢，但能精準模擬掛單流與排隊）？或兩者皆支援？
> 
> 2. **關於量化平台的 UI**：既然此專案以自動交易與回測為主，未來回測結束後：
>    - **選項 A (專業 Quant 路線)**：直接產出一份豐富的靜態 HTML/PDF 績效報告圖表（包含資金曲線、MDD、夏普值、月度熱力圖等）。
>    - **選項 B (視覺化路線)**：您依然希望有一個即時動態的網頁 UI（類似現在的 view_chart），可以看到程式「即時」在圖表上標註買賣點並顯示資金跳動？
> 
> 請告訴我您的偏好，我們馬上就能開始動工！
