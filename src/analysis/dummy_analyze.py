import psycopg2 as pg
import json
from qanalyze import simplify


db_params = {
        "database": "dummydb",
        "user": "fridtjofdamm",
        "password": "",
        "host": "localhost",
        "port": "5432"
    }


queries = []
for i in range(1,19):
    sql = f"EXPLAIN (FORMAT JSON) SELECT COUNT(*) FROM numbers WHERE number BETWEEN {i} AND {i+1};"
    queries.append(sql)

conn = pg.connect(**db_params)
cur = conn.cursor()

query_plans = []
for query in queries: 
    cur.execute(query)
    plan = cur.fetchall()
    s = simplify(plan)
    query_plans.append(s)


def write_qp_to_file(query_id, plan_index, plan_data):
    # defining directory path
    dir = f'results/tpch/qplans/q{query_id}'
    # creating dir if not exist already
    os.makedirs(dir, exist_ok=True)
    filename = os.path.join(dir, f'q{query_id}_p{plan_index}.json')
    with open(filename, mode='w', encoding='UTF-8') as file:
        file.write(json.dumps(plan_data, indent=4))



print(query_plans[len(query_plans)-2])