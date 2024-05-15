import time
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import statistics as st

cursor = duckdb.connect('tpch.duckdb')
tpch_query_results = []
tpch_exec_times = []

for i in range(1, 23):
    start = time.clock_gettime_ns(time.CLOCK_REALTIME)
    tpch_query_results.append(cursor.execute("PRAGMA tpch(" + str(i) + ")").df())
    tpch_exec_times.append(round((time.clock_gettime_ns(time.CLOCK_REALTIME) - start)/1000000, 2))

for i, df in enumerate(tpch_query_results):
    df.to_csv("queryresults/tpch" + str(i+1) +".csv", index = False)

pd.DataFrame(tpch_exec_times, columns = ["execution_time_ms"]).to_csv("queryresults/exec_times.csv", index = False)    

_, ax = plt.subplots()

queries = list(range(1, 23))

ax.bar(queries, tpch_exec_times)

ax.set_ylabel('time (ms)')
ax.set_title('TPC-H exec times')
plt.xticks(list(range(1, 23)))

max_value = tpch_exec_times.index(max(tpch_exec_times))
min_value = tpch_exec_times.index(min(tpch_exec_times))
ax.patches[max_value].set_facecolor('#f5424e')
ax.patches[min_value].set_facecolor('#1b9c10')

plt.show()