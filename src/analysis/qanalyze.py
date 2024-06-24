# https://stackoverflow.com/questions/5243596/python-sql-query-string-formatting
import duckdb
import psycopg2 as pg 

db_params = {
    'database': "dummydb",
    'user':'fridtjofdamm',
    'password':'',
    'host':'localhost',
    'port':'5432'
}

def duckdb_profiling():
    cursor = duckdb.connect('dummy_db.duckdb')
    # mit Q9 ausprobieren !!
    cursor.execute("PRAGMA enable_profiling = 'json'")
    cursor.execute(f"PRAGMA profiling_output='{'analysis/dummy_with_index.json'}';")
    cursor.execute("SELECT COUNT(*) FROM numbers;")
    cursor.execute("PRAGMA disable_profiling;")

def psql_profiling():
    conn = pg.connect(**db_params)
    
