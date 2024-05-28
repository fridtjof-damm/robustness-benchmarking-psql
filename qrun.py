import time 
import duckdb
from qgen import generate_query

cursor = duckdb.connect("tpch.duckdb")

def run_query(query_id: int, execution_file):   
    with open(f'queries/{query_id}.sql', encoding="UTF8") as statement_file:
        template = statement_file.read()
        queries = generate_query(template, query_id)

        for query in queries:
            start = time.time()
            cursor.execute(query).fetchall()
            end = time.time() - start
            execution_file.write(str(query_id)+';'+str(end))
            execution_file.write('\n')


with open('query_execution.csv', encoding='UTF8', mode='a') as execution:
    for pquery in range(1,23):
        run_query(pquery, execution)
