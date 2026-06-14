import polars as pl
from view_chart import get_data, main
import sys

def debug_data():
    try:
        # Override args to prevent UI block
        sys.argv = ['view_chart.py', '--live', '--max-bars', '100']
        
        print("Fetching 1d data...")
        df_1d = get_data('1d')
        print("1d ROWS:", len(df_1d))
        if not df_1d.is_empty():
            print("1d TIME HEAD:", df_1d["time"].head().to_list())
            print("1d TS HEAD:", df_1d["ts"].head().to_list() if "ts" in df_1d.columns else "NO TS")

        print("\nFetching 1m data...")
        df_1m = get_data('1m')
        print("1m ROWS:", len(df_1m))
        if not df_1m.is_empty():
            print("1m TIME HEAD:", df_1m["time"].head().to_list())
            print("1m TIME TAIL:", df_1m["time"].tail().to_list())
            
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_data()
