# visualization/chart_builder.py
import polars as pl
from lightweight_charts import Chart
from visualization.style_config import ColorScheme

class ChartBuilder:
    """
    負責繪圖與圖層管理
    """
    def __init__(self, symbol: str, timeframe: str, title_suffix: str = "", combine_sessions: bool = False):
        self.timeframe = timeframe
        self.chart = Chart(toolbox=True)
        ColorScheme.apply_theme(self.chart)
        self.chart.topbar.textbox('symbol', f'{symbol} {timeframe} {title_suffix}')
        
        # 如果是合併後的日線/週線等長週期，強制關閉時間顯示以保持乾淨的純日期顯示
        is_daily_or_longer = timeframe in ('1d', '1w', '1mo') or timeframe.endswith('d') or timeframe.endswith('w')
        if is_daily_or_longer and (timeframe != '1d' or combine_sessions):
            self.chart.time_scale(time_visible=False)

    def plot(self, df: pl.DataFrame):
        if df.is_empty():
            print("[Warning] No data to plot.")
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
        
        # 4. TAIEX 指數對照線繪製 (限 TXF K棒時)
        # 建立在最前面以確保在 Legend (圖例) 中顯示在最上方
        if 'TAIEX' in df.columns:
            taiex_data = df.select(['time', 'TAIEX']).drop_nulls().to_pandas()
            if not taiex_data.empty:
                taiex_line = self.chart.create_line(name='TAIEX', color='#00E5FF', width=2)
                taiex_line.set(taiex_data)
                print("   - Added TAIEX Line (TSE Index)")

        # 5. 全家桶指標繪製
        indicators = []
        
        for period, cfg in ColorScheme.MA_SETTINGS.items():
            ma_type = cfg.get('type', 'SMA')
            indicators.append((f'ma{period}', f'{ma_type}{period}', cfg['color'], cfg['width']))
        
        # vwap 維持獨立設定
        # indicators.append(('vwap', 'VWAP', ColorScheme.COLOR_VWAP, 2))

        print(f"[Chart] Chart launching... ({len(df_kbars)} bars)")

        for col_name, label, color, width in indicators:
            if col_name in df.columns:
                # 🟢 [修正] 指標也一樣，線叫什麼名字 (label)，欄位就要叫什麼名字
                line_data = df.select(['time', pl.col(col_name).alias(label)]).drop_nulls().to_pandas()
                
                if not line_data.empty:
                    line = self.chart.create_line(name=label, color=color, width=width)
                    line.set(line_data)
                    print(f"   - Added {label}")

        # 預設隱藏所有均線 (TAIEX 線維持預設顯示)，使用者可自行點圖例開啟顯示。
        for line in self.chart.lines():
            if line.name != 'TAIEX':
                line.hide_data()

        # 6. 啟動
        self.chart.fit()
        self.chart.show(block=True)