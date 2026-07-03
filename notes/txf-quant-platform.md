# txf-quant-platform (量化交易平台) 終極架構藍圖

> **專案定位**：這是一個專為「自動化交易」、「極速回測」、「AI/ML 策略開發」與「乾淨解耦的看盤視覺化」打造的專業級量化平台。

---

## 🏗️ 核心架構設計 (Directory Structure)

專案嚴格劃分為五大獨立模組，徹底解決過去「看盤 UI、資料處理、Kafka 接收」全部黏在一起的技術債：

```text
txf-quant-platform/
├── core/                  # 🧠 [共用核心]
│   ├── data_engine.py     # 負責將 Tick 組裝成 KBar，計算 VWAP/MA 等特徵
│   ├── types.py           # 嚴謹的資料定義 (Tick, KBar, Signal, Order)
│   └── interfaces.py      # 抽象基底類別 (FeedBase, BrokerBase, StrategyBase)
│
├── feeds/                 # 👁️ [感知器]
│   ├── historical.py      # 讀取 Parquet (回測)
│   └── live_kafka.py      # 實盤 Tick (Kafka)
│
├── execution/             # 🦾 [執行器]
│   ├── backtest_broker.py # 虛擬券商 (模擬撮合、滑價)
│   └── shioaji_broker.py  # 永豐 API 實盤串接
│
├── strategies/            # 🎯 [策略庫]
│   └── base_strategy.py   # 策略模板
│
├── visualization/         # 📊 [視覺化與 UI] (取代原本髒亂的 view_chart.py)
│   ├── lightweight_ui.py  # 純粹負責接收 KBar 並畫圖的 UI 元件
│   └── chart_overlay.py   # 負責將策略的買賣點標註在圖表上
│
└── app/                   # 🚀 [進入點] (這些是執行檔)
    ├── run_viewer.py      # 📈 [純看盤模式] 取代舊版的 view_chart.py
    ├── run_research.py    # 🔬 [視覺化研究] 開啟策略主副圖觀察模式
    ├── run_backtest.py    # ⏪ [大規模回測] 產出靜態報告
    └── run_live.py        # 🤖 [實盤交易] 啟動全自動下單
```

## 🧩 為什麼新專案的看盤程式 (run_viewer) 不會重蹈覆轍？

在舊的 `txf-data-lake` 中，`view_chart.py` 是一個 700 多行的巨獸，它自己去讀 Kafka、自己算 K 棒、自己處理跨日邏輯、還要自己畫圖。

在新專案的 `app/run_viewer.py` 中，程式碼會像樂高積木一樣乾淨：
1. 它呼叫 `feeds.live_kafka` 啟動資料流。
2. 它呼叫 `core.data_engine`，要求把資料流轉換成帶有 VWAP 的 `KBar`。
3. 它呼叫 `visualization.lightweight_ui`，要求把算好的 `KBar` 畫在螢幕上。

**解耦的魔力**：UI 模組根本不知道資料是從 Kafka 來的還是硬碟來的，也不知道 VWAP 怎麼算的。UI 只負責一件事：「拿到 `KBar` 型別的資料，把它畫成紅綠蠟燭」。這樣未來就算您要加 100 個技術指標，UI 的程式碼也完全不會變亂！
