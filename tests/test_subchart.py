import pandas as pd
from lightweight_charts import Chart

chart = Chart()
subchart = chart.create_subchart(position='bottom', width=1, height=0.3, sync=True)
print("Subchart created successfully!")
