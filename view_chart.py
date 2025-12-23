# view_chart.py
import argparse
from datetime import datetime
import os
import sys

# 路徑設定：確保能引用 core 和 visualization
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.loader import DataLoader
from core.processor import DataProcessor
from visualization.chart_builder import ChartBuilder

def main():
    # 1. 參數解析
    parser = argparse.ArgumentParser(description="TXF Interactive Chart Viewer")
    parser.add_argument('--symbol', type=str, default='TXF', help="商品代碼")
    parser.add_argument('--date', type=str, default=datetime.now().strftime('%Y-%m-%d'), help="開始日期")
    parser.add_argument('--end-date', type=str, default=None, help="結束日期")
    parser.add_argument('--tf', type=str, default='5m', help="K棒週期")
    parser.add_argument('--combine', action='store_true', help="合併日夜盤")
    args = parser.parse_args()

    # 預設結束日期
    if args.end_date is None: args.end_date = args.date
    
    print(f"🔍 Task: {args.symbol} {args.tf} | {args.date} ~ {args.end_date}")

    # 2. ETL 流程
    # [E]xtract: 讀取資料
    df_raw = DataLoader.load_kbars(args.symbol, args.tf, args.date, args.end_date)
    if df_raw.is_empty():
        print("❌ Data not found.")
        return

    # [T]ransform: 資料運算 (顏色、指標)
    print("⚡️ Processing...")
    df_processed = DataProcessor.process_data(df_raw, args.tf, args.combine)

    # [L]oad/Visualize: 繪圖
    title_suffix = f"({args.date}~{args.end_date})" + (" [Comb]" if args.combine else "")
    viewer = ChartBuilder(args.symbol, args.tf, title_suffix)
    try:
        viewer.plot(df_processed)
    except KeyboardInterrupt:
        # 當偵測到 Ctrl+C 時，優雅地結束
        print("\n👋 Chart closed by user. Exiting...")
        sys.exit(0)

if __name__ == "__main__":
    main()