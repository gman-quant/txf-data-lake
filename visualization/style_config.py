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
    VOL_LIGHTEN  = 0.1      # 成交量增亮係數 (0.0=原色, 0.5=混入50%白)

    # ==========================
    # 2. 色票定義 (Palette)
    # ==========================
    _RED   = '#ef5350'
    _GREEN = '#26a69a'

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
    LEGEND_SIZE  = 16
    CROSSHAIR    = {'color': '#CCCCCC', 'bg': '#4c525e', 'style': 1} # style: 1=Dash, 0=Solid

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
        line_style = {
            "color": cls.CROSSHAIR['color'],
            "width": 1,
            "style": cls.CROSSHAIR['style'],
            "labelBackgroundColor": cls.CROSSHAIR['bg']
        }
        chart.crosshair({"mode": 1, "vertLine": line_style, "horzLine": line_style})