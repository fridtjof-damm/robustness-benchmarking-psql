import duckdb

# query 
QUERY = """
        SELECT COUNT(*) FROM numbers WHERE number = {}; 
        """
# connection to db
conn = duckdb.connect('resources/db/dummy_db.duckdb')

for number in range(20):
    with open(f'results/dummyresults/plan{number}.json', mode='w', encoding='UTF-8') as file:
        conn.execute("PRAGMA enable_profiling = 'json'")
        conn.execute(f"PRAGMA profiling_output='results/dummyresults/plan{number}.json';")
        conn.execute(QUERY.format(number))
        conn.execute("PRAGMA disable_profiling;")
        file.close()



