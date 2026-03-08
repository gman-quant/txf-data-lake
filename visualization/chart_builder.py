# visualization/chart_builder.py
import polars as pl
from lightweight_charts import Chart
from visualization.style_config import ColorScheme

class ChartBuilder:
    """
    負責繪圖與圖層管理
    """
    def __init__(self, symbol: str, timeframe: str, title_suffix: str = ""):
        self.chart = Chart(toolbox=True)
        ColorScheme.apply_theme(self.chart)
        self.chart.topbar.textbox('symbol', f'{symbol} {timeframe} {title_suffix}')

    def plot(self, df: pl.DataFrame):
        if df.is_empty():
            print("⚠️ No data to plot.")
            return

        # 1. 基礎資料分流
        # K棒
        df_kbars = df.select(['time', 'open', 'high', 'low', 'close', 'color', 'borderColor', 'wickColor']).to_pandas()
        
        # 🟢 [修正] 成交量欄位必須叫 'volume' (對應 create_histogram 的名稱)
        # 之前 alias('value') 是錯誤的，因為圖層名稱我們取為 'volume'
        df_volume = df.select(['time', pl.col('volume').alias('volume'), pl.col('vol_color').alias('color')]).to_pandas()

        # 2. 繪製 K 線
        self.chart.set(df_kbars)

        # 3. 繪製成交量
        # create_histogram('volume'...) 宣告了圖層名稱為 volume，所以上面的 df 必須有 volume 欄位
        vol = self.chart.create_histogram('volume', color='color', price_line=False, price_label=False)
        vol.scale(scale_margin_top=0.8)
        vol.set(df_volume)
        
        # 4. 全家桶指標繪製
        # 根據設定檔動態載入 SMA
        indicators = [(f'ma{period}', f'MA{period}', cfg['color'], cfg['width']) 
                      for period, cfg in ColorScheme.SMA_SETTINGS.items()]
        
        # VWAP 維持獨立設定
        indicators.append(('vwap', 'VWAP', ColorScheme.COLOR_VWAP, 2))

        print(f"🚀 Chart launching... ({len(df_kbars)} bars)")

        for col_name, label, color, width in indicators:
            if col_name in df.columns:
                # 🟢 [修正] 指標也一樣，線叫什麼名字 (label)，欄位就要叫什麼名字
                line_data = df.select(['time', pl.col(col_name).alias(label)]).drop_nulls().to_pandas()
                
                if not line_data.empty:
                    line = self.chart.create_line(name=label, color=color, width=width)
                    line.set(line_data)
                    print(f"   - Added {label}")

        # 5. 啟動
        self.chart.fit()
        self.chart.show(block=True)