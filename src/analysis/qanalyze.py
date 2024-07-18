# https://stackoverflow.com/questions/5243596/python-sql-query-string-formatting
import json
import re
import duckdb
import psycopg2 as pg 
import src.qrun as qr

# setting db parameters
db_params = {
    'database': 'dummydb',
    'user':'fridtjofdamm',
    'password':'',
    'host':'localhost',
    'port':'5432'
}

# profiling tpch parameterized queries
def psql_tpch_profiling():
    conn = pg.connect(**db_params)
    cur = conn.cursor()
    prefix = 'EXPLAIN (FORMAT JSON) '
    # query 15 consists of view creation - "explain" not complatible
    query_indices = [i for i in range(1,2) if i != 15]
    for i in query_indices:
#       with open(f'results/postgres/tpch/qplan{i}.json', mode='w' ,encoding='UTF-8') as rfile:'''
        plans = qr.run_query_psql(cur, i, prefix)
    print(type(plans))
psql_tpch_profiling()

def compare_query_plans(query_id):
    plan_path = f'results/postgres/tpch/qplan{query_id}.json'
    with open(plan_path, encoding='UTF-8', mode='r') as file:
        file_str = file.read()
        file_json = json.loads(file_str)
        print(type(file_json))
#compare_query_plans(1)


def json_analyze(i):
    ### add right path to json files first ###
    dummy_psql = 'results/postgres/qplan'

    with open(f'{dummy_psql}{i}.json', encoding='UTF-8', mode='r') as file:
        file_str = file.read()
        file_json = json.loads(file_str)
        file.close()
        return file_json

def simplify(qplan):
    # check if exists, then delete
    # first selection from basic seq scan query plan 
    if 'Parallel Aware' in qplan:
        del qplan['Parallel Aware']
    if 'Async Capable' in qplan:
        del qplan['Async Capable']
    if 'Startup Cost' in qplan:
        del qplan['Startup Cost']
    if 'Total Cost' in qplan:
        del qplan['Total Cost']
    if 'Plan Rows' in qplan:
        del qplan['Plan Rows']
    if 'Single Copy' in qplan:
        del qplan['Single Copy']
    if 'Alias' in qplan:
        del qplan['Alias']
    if 'Parent Relationship' in qplan:
        del qplan['Parent Relationship']
    if 'Relation Name' in qplan:
        del qplan['Relation Name']
    if 'Filter' in qplan:
        match = re.search(r'\(([^=]+)\s*=\s*[^)]+\)', qplan['Filter'])
        if match:
            attr = match.group(1).strip()
            val = f'({attr})'
            qplan['Filter'] = val
    # further selection from index scan queries 
    # if '' in qplan:
    #     del qplan['']
    if 'Index Cond' in qplan:
        match = re.search(r'\(([^=]+)\s*=\s*[^)]+\)', qplan['Index Cond'])
        if match:
            attr = match.group(1).strip()
            val = f'{attr}'
            qplan['Index Cond'] = val

    if 'Plans' in qplan:
        for child in qplan['Plans']:
            simplify(child)
    return qplan

def persist_pg_profiling():
    plans = []
    for i in range(0,20):
        query_plan = json_analyze(i)
        simplified = simplify(query_plan)
        # add label of which relation parameter was used 
        plans.append(simplified)

        with open('results/postgres/tpch/simplified/qplan{i}.json', encoding='UTF-8', mode='w') as file:
            json.dump(plans, file, indent=4)
#persist_pg_profiling()


##############################################
##############################################
## DUCK DB, DUMMY DATA AND PLAYGROUND ########
##############################################
##############################################

def duckdb_dummy_profiling():
    cursor = duckdb.connect('dummy_db.duckdb')
    # mit Q9 ausprobieren !!
    cursor.execute("PRAGMA enable_profiling = 'json'")
    cursor.execute(f"PRAGMA profiling_output='{'analysis/dummy_with_index.json'}';")
    cursor.execute("SELECT COUNT(*) FROM numbers;")
    cursor.execute("PRAGMA disable_profiling;")
#duckdb_dummy_profiling()

def psql_dummy_profiling():
    conn = pg.connect(**db_params)
    cur = conn.cursor()
    # add format = json 
    query = "EXPLAIN (FORMAT JSON) SELECT * FROM numbers WHERE number = %s;"

    for i in range(0,20):
        cur.execute(query, (i,))
        json_plan = cur.fetchall()[0][0][0]['Plan']
        json_str = json.dumps(json_plan, indent=4)
        with open(f'results/postgres/qplan{i}.json', encoding='UTF-8', mode='w') as file:
            file.write(json_str)
            file.close()
    cur.close()
    conn.close()
#psql_dummy_profiling()   
#qr.test()

