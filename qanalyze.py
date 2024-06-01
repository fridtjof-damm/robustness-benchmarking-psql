import duckdb

cursor = duckdb.connect('tpch_sf_100.duckdb')
# mit Q9 ausprobieren !!
cursor.execute("PRAGMA enable_profiling = 'json'")
cursor.execute(f"")