import time
import json
import duckdb
import src.qgen as qg
import src.utils.utils as ut

def run_query_psql(cur, query_id, prefix):
    with open(f'resources/queries_tpch/{query_id}.sql', encoding="UTF8") as statement_file:
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
    
def run_query_picasso(cur, query_id, prefix):
    with open(f'resources/queries_picasso/qt{query_id}.sql', encoding="UTF8") as statement_file:
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