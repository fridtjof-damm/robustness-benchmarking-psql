import duckdb

cursor = duckdb.connect(".open tpch-kit-v.duckdb")
print(cursor.execute("SHOW TABLES").fetchall())


