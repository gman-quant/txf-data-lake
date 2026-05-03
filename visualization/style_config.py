# visualization/style_config.py

from typing import Any
import colorsys

class ColorScheme:
    """
    圖表配色與主題管理 (MVC Model)
    負責定義 K 棒、成交量顏色及全域圖表樣式
    """
    
    # ==========================
    # 1. 核心參數 (Core Config)
    # ==========================
    TAIWAN_STYLE = True     # True: 紅漲綠跌 (台股), False: 綠漲紅跌 (國際)
    DIM_FACTOR   = 0.6      # 夜盤亮度係數 (0.5 = 亮度減半)
    VOL_LIGHTEN  = -0.1      # 成交量增亮係數 (0.0=原色, 0.5=混入50%白)

    # ==========================
    # 2. 色票定義 (Palette)
    # ==========================
    _RED   = '#ef5350'
    _GREEN = '#26a69a'

    # 🟢 [全家桶配色] (動態設定)
    # 建議邏輯:
    # 1. 關鍵防守線加粗 (width: 2) -> 21(月線), 60(季線)
    # 2. 顏色層次: 極短線(白/黃) -> 短段(藍/粉) -> 中段(橘/綠) -> 長段(紅/紫)
    MA_SETTINGS = {
        # --- 短線 (實色，專注當下) ---
        4:   {'type': 'SMA', 'color': '#FFFFFFFF', 'width': 1},  # 白色 (100% 不透明)
        5:   {'type': 'EMA', 'color': '#FFFF00FF', 'width': 1},  # 黃色 (100% 不透明)
        9:   {'type': 'SMA', 'color': '#00BFFFFF', 'width': 1},  # 深天藍 (100% 不透明)
        10:  {'type': 'EMA', 'color': '#FF69B4FF', 'width': 2},  # 亮粉紅 (100% 不透明)

        # --- 中線 (80% 透明度，過渡過濾) ---
        19:  {'type': 'SMA', 'color': '#FFA500CC', 'width': 1},  # 亮橘色 (80% 不透明)
        20:  {'type': 'EMA', 'color': '#32CD32CC', 'width': 2},  # 萊姆綠 (80% 不透明)

        # --- 長線 (50% 透明度，背景參考，減少搶戲) ---
        59:  {'type': 'SMA', 'color': '#FF450080', 'width': 1},  # 橘紅色 (50% 不透明)
        60:  {'type': 'EMA', 'color': '#9370DB80', 'width': 2},  # 中紫色 (50% 不透明)
        120: {'type': 'SMA', 'color': '#FFD70080', 'width': 2},  # 金金色 (50% 不透明)
        240: {'type': 'SMA', 'color': '#4D96FF80', 'width': 2},  # 藍色 (50% 不透明)
    }

    COLOR_VWAP  = '#DA70D6'  # 蘭花紫 (VWAP - 成本)

    @staticmethod
    def _darken(hex_color: str, factor: float) -> str:
        """Hex 轉暗 (RGB 乘法運算)"""
        c = hex_color.lstrip('#')
        try:
            r, g, b = int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16)
            return f"#{int(r*factor):02x}{int(g*factor):02x}{int(b*factor):02x}"
        except ValueError:
            return hex_color
        
    @staticmethod
    def _lighten(hex_color: str, amount: float) -> str:
        """Hex 增亮 (HSL 空間調整亮度)"""
        c = hex_color.lstrip('#')
        try:
            # Hex -> RGB -> HSL
            r, g, b = int(c[0:2], 16)/255.0, int(c[2:4], 16)/255.0, int(c[4:6], 16)/255.0
            h, l, s = colorsys.rgb_to_hls(r, g, b)
            
            # 調整亮度 (避免過曝變純白)
            new_l = min(1.0, l + (1.0 - l) * amount)
            
            # HSL -> RGB -> Hex
            r, g, b = colorsys.hls_to_rgb(h, new_l, s)
            return f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"
        except Exception:
            return hex_color

    # 預算夜盤深色版
    _RED_DIM   = _darken(_RED, DIM_FACTOR)
    _GREEN_DIM = _darken(_GREEN, DIM_FACTOR)

    # 依模式綁定顏色 (C_UP=漲, C_DN=跌)
    if TAIWAN_STYLE:
        C_UP, C_DN = _RED, _GREEN
        C_UP_DIM, C_DN_DIM = _RED_DIM, _GREEN_DIM
    else:
        C_UP, C_DN = _GREEN, _RED
        C_UP_DIM, C_DN_DIM = _GREEN_DIM, _RED_DIM

    # ==========================
    # 3. 圖表主題 (Theme)
    # ==========================
    BG_COLOR   = '#131722'
    TEXT_COLOR = '#d1d4dc'
    GRID_COLOR = 'rgba(42, 46, 57, 0.6)'
    
    # 圖例與十字線配置
    LEGEND_COLOR = '#FFFFFF'
    LEGEND_SIZE  = 14
    CROSSHAIR    = {'color': '#CCCCCC', 'bg': '#4c525e', 'style': 1} # style: 1=Dash, 0=Solid

    # 十字線磁吸模式
    # 'normal' → 十字線自由移動，不磁吸（精確追蹤滑鼠位置）
    # 'magnet' → 磁吸到 K 棒收盤價（Close）
    # 'hidden' → 隱藏十字線
    # 注意：LWC 原生僅支援磁吸 Close，若需 OHLC 需客製 JS
    CROSSHAIR_MODE: str = 'normal'

    @classmethod
    def get_color(cls, is_up: bool, session: str) -> str:
        """取得 K 棒實體顏色 (區分日夜盤)"""
        if is_up:
            return cls.C_UP if session == 'Day' else cls.C_UP_DIM
        else:
            return cls.C_DN if session == 'Day' else cls.C_DN_DIM
        
    @classmethod
    def get_volume_color(cls, is_up: bool, session: str) -> str:
        """取得成交量顏色 (基礎色增亮)"""
        base = cls.get_color(is_up, session)
        return cls._lighten(base, cls.VOL_LIGHTEN)

    @classmethod
    def apply_theme(cls, chart: Any):
        """套用全域樣式 (背景、網格、圖例、十字線)"""
        # 基礎外觀
        chart.layout(background_color=cls.BG_COLOR, text_color=cls.TEXT_COLOR)
        chart.grid(vert_enabled=True, horz_enabled=True, color=cls.GRID_COLOR)
        
        # 圖例
        chart.legend(visible=True, ohlc=True, percent=True, 
                     font_size=cls.LEGEND_SIZE, color=cls.LEGEND_COLOR)

        # 十字查價線
        chart.crosshair(
            mode=cls.CROSSHAIR_MODE,
            vert_color=cls.CROSSHAIR['color'],
            vert_width=1,
            vert_style='dashed',
            vert_label_background_color=cls.CROSSHAIR['bg'],
            horz_color=cls.CROSSHAIR['color'],
            horz_width=1,
            horz_style='dashed',
            horz_label_background_color=cls.CROSSHAIR['bg'],
        )