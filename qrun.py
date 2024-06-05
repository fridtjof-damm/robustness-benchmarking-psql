import time
import duckdb
from qgen import generate_query
from utils import format_tuple

cursor = duckdb.connect("tpch_sf_100.duckdb")
#cursor = duckdb.connect("tpch_sf_100.duckdb")
def run_query(query_id: int, execution_file):   
    with open(f'queries/{query_id}.sql', encoding="UTF8") as statement_file:
        template = statement_file.read()
        queries, parameters = generate_query(template, query_id)
        assert len(queries) == len(parameters)
        for q,p in zip(queries,parameters):
            start = time.time()
            cursor.execute(q).fetchall()
            end = time.time() - start
            execution_file.write(str(end)+';'+format_tuple(p)+'\n')

for qid in range(1,23):
    with open(f'results_sf100/{qid}.csv', encoding='UTF8', mode='a') as execution:
        run_query(qid, execution)
