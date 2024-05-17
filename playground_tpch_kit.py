import duckdb

cursor = duckdb.connect(".open tpch-kit-v.duckdb")

print(cursor.execute('tpch-kit/dbgen/queries/22.sql').df())
# print(cursor.execute("PRAGMA tpch(22)").df())
