import pandas as pd
from lightweight_charts import Chart

chart = Chart()
df = pd.DataFrame({
    'time': ['2023-01-01', '2023-01-02', '2023-01-03'],
    'open': [100, 101, 102],
    'high': [105, 106, 107],
    'low': [95, 96, 97],
    'close': [101, 102, 103]
})
chart.set(df)

subchart = chart.create_subchart(position='bottom', width=1, height=0.3, sync=True)
line = subchart.create_line(name='Test', color='red')
df2 = pd.DataFrame({
    'time': ['2023-01-01', '2023-01-02', '2023-01-03'],
    'Test': [1, 2, 3]
})
line.set(df2)

print("showing chart...")
chart.show(block=False)
