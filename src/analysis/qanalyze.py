# https://stackoverflow.com/questions/5243596/python-sql-query-string-formatting
import json
import re
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
   # conn = pg.connect(**db_params)
    conn = pg.connect(
    database="dummydb",
    user='fridtjofdamm',
    password='',
    host='localhost',
    port='5432'
)
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
# uncomment to re run profiling of parametrized queries  
#psql_profiling()

def json_analyze(i):
    with open(f'results/postgres/qplan{i}.json', encoding='UTF-8', mode='r') as file:
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
    if 'Plan Width' in qplan:
        del qplan['Plan Width']
    if 'Single Copy' in qplan:
        del qplan['Single Copy']
    if 'Workers Planned' in qplan:
        del qplan['Workers Planned']
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
            val = f'({attr})'
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
        # add label of which realtion parameter was used 
        if 'Index Cond' in query_plan:
            plans.append('"'+str(query_plan['Index Cond']+'": "'+str(i)+'"'))
        plans.append(simplified)

        with open('results/postgres/simplified/plans_idx_nmb.json', encoding='UTF-8', mode='w') as file:
            json.dump(plans, file, indent=4)
persist_pg_profiling()
