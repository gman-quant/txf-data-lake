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
        self._is_switching = True
        try:
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
        except Exception as e:
            print(f"[Chart] Error switching timeframe: {e}")
        finally:
            self._is_switching = False

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

        # 6. 適配/調整視角
        self.chart.fit()
        
        if not self._chart_shown:
            self._chart_shown = True
            self.chart.show(block=True)

    def update_live_bar(self, live_bar: dict):
        if not self._chart_shown:
            return
            
        import json
        import pandas as pd
        from datetime import datetime
        
        time_val = live_bar.get('time')
        # 嚴格對齊 set() 時的底層行為：強制套用 UTC 時區以吻合 Pandas astype('int64') 的預設行為
        if isinstance(time_val, str):
            time_val = pd.to_datetime(time_val).to_pydatetime()
        
        if isinstance(time_val, pd.Timestamp):
            time_val = time_val.to_pydatetime()
            
        if isinstance(time_val, datetime):
            if time_val.tzinfo is None:
                from datetime import timezone
                time_val = time_val.replace(tzinfo=timezone.utc)
            time_val = int(time_val.timestamp())
            
        # 1. 繞過 Buggy 套件，直接對 JS 引擎下達 Candle 更新指令
        candle_data = {k: v for k, v in live_bar.items() if k in ['open', 'high', 'low', 'close', 'color', 'borderColor', 'wickColor']}
        if time_val is not None:
            candle_data['time'] = time_val
        self.chart.run_script(f'{self.chart.id}.series.update({json.dumps(candle_data)})')
        
        # 2. 直接更新 Volume (注意 JS 內部單一數值指標的欄位名為 value)
        if self.vol_series and 'volume' in live_bar:
            vol_data = {
                'time': time_val, 
                'value': live_bar['volume'], 
                'color': live_bar.get('vol_color', live_bar.get('color'))
            }
            self.chart.run_script(f'{self.vol_series.id}.series.update({json.dumps(vol_data)})')
            
        # 3. 直接更新 TAIEX
        if self.taiex_line and 'TAIEX' in live_bar and live_bar['TAIEX'] is not None:
            taiex_data = {'time': time_val, 'value': live_bar['TAIEX']}
            self.chart.run_script(f'{self.taiex_line.id}.series.update({json.dumps(taiex_data)})')
            
        # 4. 直接更新 MAs
        for label, line in self.ma_lines.items():
            if label in live_bar and live_bar[label] is not None:
                ma_data = {'time': time_val, 'value': live_bar[label]}
                self.chart.run_script(f'{line.id}.series.update({json.dumps(ma_data)})')