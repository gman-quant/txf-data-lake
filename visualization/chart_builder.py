# visualization/chart_builder.py
import polars as pl
from lightweight_charts import Chart
from visualization.style_config import ColorScheme

class ChartBuilder:
    """
    負責 Lightweight Charts 的初始化、圖層設定與繪圖
    """
    def __init__(self, symbol: str, timeframe: str, title_suffix: str = ""):
        self.chart = Chart(toolbox=True)
        ColorScheme.apply_theme(self.chart)
        self.chart.topbar.textbox('symbol', f'{symbol} {timeframe} {title_suffix}')

    def plot(self, df: pl.DataFrame):
        if df.is_empty():
            print("⚠️ No data to plot.")
            return

        # 1. 資料分流 (K棒層 vs 成交量層)
        df_kbars = df.select(['time', 'open', 'high', 'low', 'close', 'color', 'borderColor', 'wickColor']).to_pandas()
        # 注意：這裡將 'vol_color' 改名為 'color' 以符合 Histogram 格式
        df_volume = df.select(['time', 'volume', pl.col('vol_color').alias('color')]).to_pandas()

        # 2. 繪製 K 線 (Main Series)
        self.chart.set(df_kbars)

        # 3. 繪製成交量 (Volume Series)
        vol = self.chart.create_histogram('volume', color='color', price_line=False, price_label=False)
        vol.scale(scale_margin_top=0.8) # 沉底 (佔據下方 20%)
        vol.set(df_volume)
        
        # 4. 啟動視窗
        print(f"🚀 Chart launching... ({len(df_kbars)} bars)")
        self.chart.fit()
        self.chart.show(block=True)