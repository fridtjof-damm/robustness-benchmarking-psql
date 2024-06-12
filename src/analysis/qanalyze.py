# https://stackoverflow.com/questions/5243596/python-sql-query-string-formatting
import duckdb

cursor = duckdb.connect('dummy_db.duckdb')
# mit Q9 ausprobieren !!
cursor.execute("PRAGMA enable_profiling = 'json'")
cursor.execute(f"PRAGMA profiling_output='{'analysis/dummy_with_index.json'}';")
cursor.execute("SELECT COUNT(*) FROM numbers;")
cursor.execute("PRAGMA disable_profiling;")
