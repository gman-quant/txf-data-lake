# visualization/chart_builder.py
import polars as pl
import pandas as pd
from lightweight_charts import Chart
from visualization.style_config import ColorScheme

class ChartBuilder:
    """
    負責繪圖與圖層管理
    """
    def __init__(self, symbol: str, timeframe: str, title_suffix: str = "", combine_sessions: bool = False, on_timeframe_change_cb = None, available_tfs = None):
        self.symbol = symbol
        self.timeframe = timeframe
        self.title_suffix = title_suffix
        self.combine_sessions = combine_sessions
        self.on_timeframe_change_cb = on_timeframe_change_cb

        self.chart = Chart(toolbox=True)
        ColorScheme.apply_theme(self.chart)
        
        # 1. 設置 Title
        self.chart.topbar.textbox('symbol', f'{symbol} {timeframe} {title_suffix}')
        
        # 2. 時間顯示
        self.apply_time_visibility(timeframe)
        
        # 3. 設置 Timeframe 切換器
        tfs = list(available_tfs) if available_tfs else ['1m', '5m', '15m', '30m', '1h', '4h', '1d']
        if timeframe not in tfs:
            tfs.append(timeframe)
            
        self.chart.topbar.switcher(
            name='timeframe',
            options=tuple(tfs),
            default=timeframe,
            func=self._on_timeframe_change
        )

        # 4. 初始化圖層引用
        self.vol_series = None
        self.taiex_line = None
        self.ma_lines = {}
        self._chart_shown = False

    def apply_time_visibility(self, tf: str):
        is_daily_or_longer = tf in ('1d', '1w', '1mo') or tf.endswith('d') or tf.endswith('w')
        if is_daily_or_longer and (tf != '1d' or self.combine_sessions):
            self.chart.time_scale(time_visible=False)
        else:
            self.chart.time_scale(time_visible=True)

    def _on_timeframe_change(self, chart):
        new_tf = chart.topbar['timeframe'].value
        print(f"\n[Chart] Switcher triggered. Switching timeframe to: {new_tf}")
        if self.on_timeframe_change_cb:
            df_processed = self.on_timeframe_change_cb(new_tf)
            if df_processed is not None and not df_processed.is_empty():
                self.timeframe = new_tf
                self.chart.topbar['symbol'].set(f'{self.symbol} {new_tf} {self.title_suffix}')
                self.apply_time_visibility(new_tf)
                self.plot(df_processed)
                print(f"[Chart] Timeframe {new_tf} loaded successfully.")
            else:
                print(f"[Warning] Failed to load data for timeframe: {new_tf}")

    def plot(self, df: pl.DataFrame):
        if df.is_empty():
            print("[Warning] No data to plot.")
            return

        # 1. 基礎資料分流
        df_kbars = df.select(['time', 'open', 'high', 'low', 'close', 'color', 'borderColor', 'wickColor']).to_pandas()
        df_volume = df.select(['time', pl.col('volume').alias('volume'), pl.col('vol_color').alias('color')]).to_pandas()

        # 2. 繪製/更新 K 線
        self.chart.set(df_kbars)

        # 3. 繪製/更新成交量
        if self.vol_series is None:
            self.vol_series = self.chart.create_histogram('volume', color='color', price_line=False, price_label=False)
            self.vol_series.scale(scale_margin_top=0.8)
        self.vol_series.set(df_volume)
        
        # 4. TAIEX 指數對照線繪製
        if 'TAIEX' in df.columns:
            taiex_data = df.select(['time', 'TAIEX']).drop_nulls().to_pandas()
            if not taiex_data.empty:
                if self.taiex_line is None:
                    self.taiex_line = self.chart.create_line(name='TAIEX', color='#00E5FF', width=2)
                self.taiex_line.set(taiex_data)
                print("   - Added/Updated TAIEX Line (TSE Index)")
        else:
            if self.taiex_line is not None:
                self.taiex_line.set(pd.DataFrame(columns=['time', 'TAIEX']))

        # 5. 全家桶指標繪製
        indicators = []
        for period, cfg in ColorScheme.MA_SETTINGS.items():
            ma_type = cfg.get('type', 'SMA')
            indicators.append((f'ma{period}', f'{ma_type}{period}', cfg['color'], cfg['width']))

        print(f"[Chart] Chart updating... ({len(df_kbars)} bars)")

        for col_name, label, color, width in indicators:
            if col_name in df.columns:
                line_data = df.select(['time', pl.col(col_name).alias(label)]).drop_nulls().to_pandas()
                
                if label not in self.ma_lines:
                    line = self.chart.create_line(name=label, color=color, width=width)
                    self.ma_lines[label] = line
                    line.hide_data()
                
                if not line_data.empty:
                    self.ma_lines[label].set(line_data)
            else:
                if label in self.ma_lines:
                    self.ma_lines[label].set(pd.DataFrame(columns=['time', label]))

        # 6. 啟動/調整視野
        self.chart.fit()
        
        if not self._chart_shown:
            self._chart_shown = True
            self.chart.show(block=True)