# visualization/style_config.py
from typing import Any

class ColorScheme:
    """
    配色方案管理：包含 K 棒顏色、成交量透明度與圖表主題
    """
    
    # ==========================
    # 1. 基礎色票 (Base Palette)
    # ==========================
    # TradingView 標準紅綠
    COLOR_RED   = '#ef5350'
    COLOR_GREEN = '#26a69a'
    
    # 🟢 [優化 1] 台股模式開關 (True=紅漲綠跌, False=綠漲紅跌)
    # 做 TXF 建議設為 True
    TAIWAN_STYLE = False 

    # 亮度係數 (0.5 代表亮度減半，用於夜盤)
    DIM_FACTOR = 0.5 

    @staticmethod
    def _darken(hex_color: str, factor: float) -> str:
        """[內部工具] 自動把 HEX 顏色變暗"""
        hex_color = hex_color.lstrip('#')
        try:
            r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
            r, g, b = int(r * factor), int(g * factor), int(b * factor)
            return f"#{r:02x}{g:02x}{b:02x}"
        except ValueError:
            return hex_color # 防呆

    # 自動計算深色版 (夜盤用)
    COLOR_RED_DIM   = _darken(COLOR_RED, DIM_FACTOR)
    COLOR_GREEN_DIM = _darken(COLOR_GREEN, DIM_FACTOR)

    # 根據模式決定 漲/跌 代表色
    if TAIWAN_STYLE:
        UP_COLOR        = COLOR_RED
        DOWN_COLOR      = COLOR_GREEN
        UP_COLOR_DIM    = COLOR_RED_DIM
        DOWN_COLOR_DIM  = COLOR_GREEN_DIM
    else:
        UP_COLOR        = COLOR_GREEN
        DOWN_COLOR      = COLOR_RED
        UP_COLOR_DIM    = COLOR_GREEN_DIM
        DOWN_COLOR_DIM  = COLOR_RED_DIM

    # ==========================
    # 2. 圖表主題 (Chart Theme)
    # ==========================
    CHART_BG_COLOR    = '#131722'  # 深灰黑背景
    AXIS_TEXT_COLOR   = '#d1d4dc'  # 座標軸文字
    GRID_COLOR        = 'rgba(42, 46, 57, 0.6)' # 網格線
    
    # 圖例設定
    LEGEND_TEXT_COLOR = '#FFFFFF'
    LEGEND_FONT_SIZE  = 16 # 字體加大比較清楚

    # 🟢 [優化 2] 十字線設定集中管理 (不再寫死在下面)
    CROSSHAIR_COLOR   = '#CCCCCC'  # 改回淺灰，比純白柔和一點，且確保看得見
    CROSSHAIR_BG      = '#4c525e'  # 座標標籤背景色
    CROSSHAIR_STYLE   = 1          # 0=實線, 1=虛線 (如果不清楚可改回 0)

    @classmethod
    def get_color(cls, is_up: bool, session: str) -> str:
        """取得 K 棒實體顏色"""
        # 判斷漲跌
        if is_up:
            color = cls.UP_COLOR if session == 'Day' else cls.UP_COLOR_DIM
        else:
            color = cls.DOWN_COLOR if session == 'Day' else cls.DOWN_COLOR_DIM
        
        return color

    @classmethod
    def apply_theme(cls, chart: Any):
        """
        [核心方法] 套用主題到 chart 物件
        """
        # 1. 基礎外觀
        chart.layout(background_color=cls.CHART_BG_COLOR, text_color=cls.AXIS_TEXT_COLOR)
        chart.grid(vert_enabled=True, horz_enabled=True, color=cls.GRID_COLOR)
        
        # 2. 圖例
        chart.legend(
            visible=True, 
            ohlc=True, 
            percent=True, 
            font_size=cls.LEGEND_FONT_SIZE, 
            color=cls.LEGEND_TEXT_COLOR
        )

        # 3. 十字查價線 (使用您驗證過的 Dict 結構，但帶入變數)
        chart.crosshair({
            "mode": 1,  # 1 = Normal (自由移動), 0 = Magnet
            "vertLine": {
                "color": cls.CROSSHAIR_COLOR,
                "width": 1,
                "style": cls.CROSSHAIR_STYLE,
                "labelBackgroundColor": cls.CROSSHAIR_BG
            },
            "horzLine": {
                "color": cls.CROSSHAIR_COLOR,
                "width": 1,
                "style": cls.CROSSHAIR_STYLE,
                "labelBackgroundColor": cls.CROSSHAIR_BG
            }
        })