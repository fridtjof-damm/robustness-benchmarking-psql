import duckdb

cursor = duckdb.connect(".open tpch-kit-v.duckdb")
print(cursor.execute("PRAGMA tpch(22)").df())
