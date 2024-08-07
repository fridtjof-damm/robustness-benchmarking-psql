# https://stackoverflow.com/questions/5243596/python-sql-query-string-formatting
import json
import re
import duckdb
import psycopg2 as pg 
import src.qrun as qr
import src.analysis.dummy.dummy_data_gen as ddg

# setting db parameters
db_params = {
    'database': 'dummydb',
    'user':'fridtjofdamm',
    'password':'',
    'host':'localhost',
    'port':'5432'
}

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

# profiling tpch parameterized queries
# returns simplified tpch query plans as list of list
def psql_tpch_profiling():
    conn = pg.connect(**db_params)
    cur = conn.cursor()
    prefix = 'EXPLAIN (FORMAT JSON) '
    # query 15 consists of view creation - "explain" not compatible
    query_indices = [i for i in range(21,22) if i != 15]
    plans = []
    for i in query_indices:
        plans_query_i = qr.run_query_psql(cur, i, prefix)
        plans_query_i_json = []
        for plan in plans_query_i:
            json_plan = json.loads(plan)
            plans_query_i_json.append(json_plan)
        plans.append(plans_query_i_json)
#    print(len(plans))
#    print(type(plans[0]))
#    print(len(plans[0]))
#    print(type(plans[0]))
#    print(plans[0][0])
    simplified = []
    for qplan in plans[0]:
        s = simplify(qplan[0][0][0]['Plan'])
        simplified.append(s)
    return simplified
test_plans = psql_tpch_profiling()

#def compare_query_plans(query_id):

def compare_query_plans(query_plans):
    #categories of query plans
    categories = []
    #query plans of i-th query
    #query_plans = psql_tpch_profiling()[query_id]
    for plan in query_plans:
        category_found = False
        for category in categories:
            #
            if plan == category[0]:
                category.append(plan)
                category_found = True
        if not category_found:
            categories.append([plan])
    return categories

# testen mit dummy data set
#test_plans = ddg.dummygen()

result = compare_query_plans(test_plans)

total_plans = sum(len(category) for category in result)


for i, category in enumerate(result):
    frequency = (len(category) / total_plans) * 100
    print(f"Plan Category {i}: {len(category)} plans, frequency: {frequency:.4f}%")

print(f"\nTotal categories: {len(result)}")
print(f"Total plans: {sum(len(category) for category in result)}")


def json_analyze(i):
    ### add right path to json files first ###
    dummy_psql = 'results/postgres/qplan'

    with open(f'{dummy_psql}{i}.json', encoding='UTF-8', mode='r') as file:
        file_str = file.read()
        file_json = json.loads(file_str)
        file.close()
        return file_json



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
