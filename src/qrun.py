import time
import json
import duckdb
import src.qgen as qg
import src.utils.utils as ut

cursor = duckdb.connect("resources/db/tpch.duckdb")
#cursor = duckdb.connect("tpch_sf_100.duckdb")
def run_query(query_id: int, execution_file):   
    with open(f'resources/queries/{query_id}.sql', encoding="UTF8") as statement_file:
        template = statement_file.read()
        queries, parameters = qg.generate_query(template, query_id)
        assert len(queries) == len(parameters)
        for q,p in zip(queries,parameters):
            start = time.time()
            cursor.execute(q).fetchall()
            end = time.time() - start
            execution_file.write(str(end)+';'+ut.format_tuple(p)+'\n')

for qid in range(21,23):
    with open(f'results/parameters_sf10/{qid}.csv', encoding='UTF8', mode='w') as execution:
        run_query(qid, execution)

def run_query_psql(cur, query_id, prefix):
    with open(f'resources/queries/{query_id}.sql', encoding="UTF8") as statement_file:
        template = statement_file.read()
        queries = qg.generate_query(template, query_id)[0]
        plans = []
        for q in queries:
            sql = prefix + q
            cur.execute(sql)
            json_plan = cur.fetchall()
            json_str = json.dumps(json_plan, indent=4)
            plans.append(json_str)
            statement_file.close()
        return plans