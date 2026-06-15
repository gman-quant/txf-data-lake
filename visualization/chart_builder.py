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

        # 1. 建立圖表與套用主題 (設定 inner_height 預留空間給下方的附圖)
        self.chart = Chart(toolbox=True, inner_width=1.0, inner_height=0.75)
        ColorScheme.apply_theme(self.chart)
        
        # 1. 設置 Title
        self.chart.topbar.textbox('symbol', f'{symbol} {timeframe} {title_suffix}')
        
        # 2. 時間顯示
        self.apply_time_visibility(timeframe)
        
        # 3. 設置 Timeframe 切換器
        tfs = list(available_tfs) if available_tfs else ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1d (comb)']
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
        
        # 5. 期現價差附圖 (Basis Spread)
        self.basis_subchart = self.chart.create_subchart(position='bottom', width=1, height=0.25, sync=True)
        ColorScheme.apply_theme(self.basis_subchart)
        self.basis_subchart.legend(visible=True)
        
        # [BugFix 1] 繞過 lightweight-charts-python 內部在 sync=True 時的 Legend 同步崩潰 Bug，並關閉 K/M 縮寫
        from visualization.js_snippets import get_legend_shorthand_fix, get_crosshair_sync_fix
        js_patch = get_legend_shorthand_fix(self.basis_subchart.id)
        self.chart.run_script(js_patch)

        self.basis_series = self.basis_subchart.create_histogram(name='期現價差 (Basis)', color=ColorScheme.C_UP)
        self.basis_series.horizontal_line(0, color='rgba(255, 255, 255, 0.4)', width=1, style='dashed')
        
        # 5.5 次月期現價差與跨月轉倉價差 (疊加)
        self.r2_basis_series = self.basis_subchart.create_line(name='次月期現 (R2-Spot)', color=ColorScheme.COLOR_R2_SPOT_POS, width=2)
        self.r2_basis_series.hide_data()  # 預設隱藏
        
        self.calendar_series = self.basis_subchart.create_line(name='跨月轉倉 (R2-R1)', color=ColorScheme.COLOR_CAL_SPREAD_POS, width=2)
        self.calendar_series.hide_data()  # 預設隱藏
        
        # 強制設定為價格格式 (2位小數)，禁止轉換為 'K' (千) 這種成交量縮寫
        # [BugFix 2] 攔截 setCrosshairPosition 避免 Value is null 崩潰 (包含主副圖的雙向修復)
        js_patch_2 = get_crosshair_sync_fix(
            main_chart_id=self.chart.id,
            subchart_id=self.basis_subchart.id,
            subchart_series_id=self.basis_series.id
        )
        self.chart.run_script(js_patch_2)
        
        self.basis_series.precision(2)
        self.chart.run_script(f"{self.basis_series.id}.series.applyOptions({{ priceScaleId: 'right', priceFormat: {{ type: 'price', precision: 2, minMove: 0.01 }} }})")
        

        
        self._chart_shown = False

    def apply_time_visibility(self, tf: str):
        base_tf = tf.replace(' (comb)', '').strip()
        is_daily_or_longer = base_tf in ('1d', '1w', '1mo') or base_tf.endswith('d') or base_tf.endswith('w')
        is_combined = self.combine_sessions or '(comb)' in tf
        if is_daily_or_longer and (base_tf != '1d' or is_combined):
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
        from visualization.style_config import ColorScheme
        
        if df.is_empty():
            print("[Warning] No data to plot.")
            return

        # 1. 基礎資料分流
        df_kbars = df.select(['time', 'open', 'high', 'low', 'close', 'color', 'borderColor', 'wickColor']).to_pandas()
        df_volume = df.select(['time', pl.col('volume').alias('volume'), pl.col('vol_color').alias('color')]).to_pandas()

        # 2. 繪製/更新 K 線
        self.chart.set(df_kbars)

        # 2.5 繪製結算日垂直線 (動態對齊當天最後一根 K 棒)
        try:
            import os
            import pandas as pd
            from config.settings import SETTLEMENT_CSV_PATH
            if os.path.exists(SETTLEMENT_CSV_PATH):
                # 1. 清除舊的垂直線 (避免切換週期時重複疊加)
                if hasattr(self, '_settlement_lines'):
                    for line in self._settlement_lines:
                        try:
                            line.delete()
                        except:
                            pass
                self._settlement_lines = []

                settlements = pd.read_csv(SETTLEMENT_CSV_PATH)
                settle_dates = set(pd.to_datetime(settlements['date']).dt.date)
                
                # 計算 K 棒對應的日期
                temp_time = pd.to_datetime(df_kbars['time'])
                df_kbars['date_only'] = temp_time.dt.date
                
                # 2. 篩選出結算日的 K 棒，並排除夜盤 (時間 <= 13:30)
                hour_mask = (temp_time.dt.hour < 13) | ((temp_time.dt.hour == 13) & (temp_time.dt.minute <= 30))
                settle_bars = df_kbars[
                    (df_kbars['date_only'].isin(settle_dates)) & hour_mask
                ]
                
                if not settle_bars.empty:
                    # 抓取每個結算日「日盤最晚」的一根 K 棒時間
                    settle_times = settle_bars.groupby('date_only')['time'].max()
                    
                    for d, t in settle_times.items():
                        if isinstance(t, pd.Timestamp):
                            t_val = t.to_pydatetime()
                        else:
                            t_val = pd.to_datetime(t).to_pydatetime()
                            
                        # 依據您的需求，只在「副圖」畫上垂直虛線，保持主圖乾淨
                        l2 = self.basis_subchart.vertical_line(
                            time=t_val, 
                            color='rgba(255, 255, 255, 0.3)', 
                            style='dashed'
                        )
                        self._settlement_lines.extend([l2])
        except Exception as e:
            print(f"[Chart] Error drawing settlement lines: {e}")

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
                    self.taiex_line = self.chart.create_line(name='TAIEX', color=ColorScheme.COLOR_TAIEX, width=2)
                self.taiex_line.set(taiex_data)
                print("   - Added/Updated TAIEX Line (TSE Index)")
        else:
            if self.taiex_line is not None:
                self.taiex_line.set(pd.DataFrame(columns=['time', 'TAIEX']))

        # 4.5. 期現價差附圖繪製
        if 'basis' in df.columns:
            if 'session' in df.columns:
                
                # 方案 A: 零軸柱狀圖 + 四種狀態顏色
                basis_color_expr = (
                    pl.when(pl.col("basis") >= 0)
                    .then(pl.when(pl.col("session") == "Night").then(pl.lit(ColorScheme.C_UP_DIM)).otherwise(pl.lit(ColorScheme.C_UP)))
                    .otherwise(pl.when(pl.col("session") == "Night").then(pl.lit(ColorScheme.C_DN_DIM)).otherwise(pl.lit(ColorScheme.C_DN)))
                )
                
                basis_data = df.select([
                    'time', 
                    pl.col('basis').round(2).alias('期現價差 (Basis)'),
                    basis_color_expr.alias('color')
                ]).to_pandas()
            else:
                basis_data = df.select(['time', pl.col('basis').round(2).alias('期現價差 (Basis)')]).to_pandas()
                
            if not basis_data.empty:
                self.basis_series.set(basis_data)
                print("   - Added/Updated Basis Subchart")
        else:
            print("   - No basis column found in df!")
            self.basis_series.set(pd.DataFrame(columns=['time', '期現價差 (Basis)', 'color']))
            
        # 4.6. R2 相關價差疊加繪製
        if 'r2_basis' in df.columns:
            r2_color_expr = pl.when(pl.col("r2_basis") >= 0).then(pl.lit(ColorScheme.COLOR_R2_SPOT_POS)).otherwise(pl.lit(ColorScheme.COLOR_R2_SPOT_NEG))
            r2_data = df.select(['time', pl.col('r2_basis').round(2).alias('次月期現 (R2-Spot)'), r2_color_expr.alias('color')]).to_pandas()
            if not r2_data.empty:
                self.r2_basis_series.set(r2_data)
        else:
            self.r2_basis_series.set(pd.DataFrame(columns=['time', '次月期現 (R2-Spot)', 'color']))
            
        if 'calendar_spread' in df.columns:
            cal_color_expr = pl.when(pl.col("calendar_spread") >= 0).then(pl.lit(ColorScheme.COLOR_CAL_SPREAD_POS)).otherwise(pl.lit(ColorScheme.COLOR_CAL_SPREAD_NEG))
            cal_data = df.select(['time', pl.col('calendar_spread').round(2).alias('跨月轉倉 (R2-R1)'), cal_color_expr.alias('color')]).to_pandas()
            if not cal_data.empty:
                self.calendar_series.set(cal_data)
        else:
            self.calendar_series.set(pd.DataFrame(columns=['time', '跨月轉倉 (R2-R1)', 'color']))

        # 5. 全家桶指標繪製
        indicators = []
        for period, cfg in ColorScheme.MA_SETTINGS.items():
            ma_type = cfg.get('type', 'SMA')
            indicators.append((f'ma{period}', f'{ma_type}{period}', cfg['color'], cfg['width']))
            
        if 'vwap' in df.columns:
            indicators.append(('vwap', 'VWAP', ColorScheme.COLOR_VWAP, 2))

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

    def _safe_update_series(self, js_target: str, data: dict):
        import json
        try:
            self.chart.run_script(f'{js_target}.update({json.dumps(data)})')
        except Exception:
            pass

    def update_live_bar(self, live_bar: dict):
        if not self._chart_shown:
            return
            
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
            
        self._safe_update_series(f'{self.chart.id}.series', candle_data)
            
        # 2. 直接更新 Volume (注意 JS 內部單一數值指標的欄位名為 value)
        if self.vol_series and 'volume' in live_bar:
            vol_data = {
                'time': time_val, 
                'value': live_bar['volume'], 
                'color': live_bar.get('vol_color', live_bar.get('color'))
            }
            self._safe_update_series(f'{self.vol_series.id}.series', vol_data)
            
        # 3. 直接更新 TAIEX
        if self.taiex_line and 'TAIEX' in live_bar and live_bar['TAIEX'] is not None:
            taiex_data = {'time': time_val, 'value': live_bar['TAIEX']}
            self._safe_update_series(f'{self.chart.id}.legend._lines.find((l) => l.series === {self.taiex_line.id}.series).series', taiex_data)
                
        # 4. 直接更新 MAs
        for name, line_obj in self.ma_lines.items():
            if name in live_bar and live_bar[name] is not None:
                line_data = {'time': time_val, 'value': live_bar[name]}
                self._safe_update_series(f'{self.chart.id}.legend._lines.find((l) => l.series === {line_obj.id}.series).series', line_data)
                    
        # 5. 直接更新 Basis Subchart
        if 'basis' in live_bar and live_bar['basis'] is not None:
            basis_val = round(float(live_bar['basis']), 2)
            basis_color = '#FFA500'
            if 'session' in live_bar:
                from visualization.style_config import ColorScheme
                session = live_bar['session']
                if basis_val >= 0:
                    basis_color = ColorScheme.C_UP_DIM if session == 'Night' else ColorScheme.C_UP
                else:
                    basis_color = ColorScheme.C_DN_DIM if session == 'Night' else ColorScheme.C_DN
                    
            basis_data = {'time': time_val, 'value': basis_val, 'color': basis_color}
            self._safe_update_series(f'{self.basis_series.id}.series', basis_data)
                
        # 6. 直接更新 R2 相關價差疊加折線
        if hasattr(self, 'r2_basis_series') and 'r2_basis' in live_bar and live_bar['r2_basis'] is not None:
            r2_val = round(float(live_bar['r2_basis']), 2)
            from visualization.style_config import ColorScheme
            r2_color = ColorScheme.COLOR_R2_SPOT_POS if r2_val >= 0 else ColorScheme.COLOR_R2_SPOT_NEG
            r2_data = {'time': time_val, 'value': r2_val, 'color': r2_color}
            self._safe_update_series(f'{self.r2_basis_series.id}.series', r2_data)
                
        if hasattr(self, 'calendar_series') and 'calendar_spread' in live_bar and live_bar['calendar_spread'] is not None:
            cal_val = round(float(live_bar['calendar_spread']), 2)
            from visualization.style_config import ColorScheme
            cal_color = ColorScheme.COLOR_CAL_SPREAD_POS if cal_val >= 0 else ColorScheme.COLOR_CAL_SPREAD_NEG
            cal_data = {'time': time_val, 'value': cal_val, 'color': cal_color}
            self._safe_update_series(f'{self.calendar_series.id}.series', cal_data)