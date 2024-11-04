import json
import re
import os
import psycopg2 as pg 
from psycopg2.extensions import cursor
import src.qrun as qr
import src.analysis.dummy.dummy_data_gen as ddg
from src.utils.utils import extract_number
import src.utils.db_connector as dc
import pprint as pp
from typing import List, Tuple

# db config
db_params = {
        "database": "dummydb",
        "user": "fridtjofdamm",
        "password": "",
        "host": "localhost",
        "port": "5432"
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
    if 'Single Copy' in qplan:
        del qplan['Single Copy']
    if 'Alias' in qplan:
        del qplan['Alias']
    if 'Parent Relationship' in qplan:
        del qplan['Parent Relationship']
    if 'Relation Name' in qplan:
        del qplan['Relation Name']
    if 'Filter' in qplan:
        qplan['Filter'] = re.sub(r'\(\(([^)]+?)\)::text[^)]+\)', r'(\1)', qplan['Filter']) # q2 TIN NICKEL etc.
        qplan['Filter'] = re.sub(r'\(([^<>=!]+)\s*[<>=!]+\s*[^)]+\)', r'(\1)', qplan['Filter']) # q3 shipdate filter simplification
        qplan['Filter'] = re.sub(r'\(*([^()]+)\)*(?:%\'::text)?(?:\s*(?:AND|OR)\s*\(*\1\)*(?:%\'::text)?)*', r'\1', qplan['Filter'])
        #qplan['Filter'] = re.sub(r'(?:^|\s*(?:AND|OR)\s*)(?:\(*)([^()]+?)(?:\)*(?:%\'::text)?)(?=\s*(?:AND|OR|$))', r'\1', qplan['Filter'])
        qplan['Filter'] = ''.join(sorted(set(re.sub(r'[^a-zA-Z]', '', re.sub(r'(?:^|\s*(?:AND|OR)\s*)(?:\(*)([^()]+?)(?:\)*(?:%\'::text)?)(?=\s*(?:AND|OR|$))', r'\1', qplan['Filter']))), key=lambda x: re.sub(r'[^a-zA-Z]', '', qplan['Filter']).index(x)))
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
    if 'Join Filter' in qplan:
        qplan['Join Filter'] = re.sub(r'\(([^=]+)\s*=\s*\'[^\']+\'::bpchar\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^=]+)\s*=\s*\'[^\']+\'::[^\)]+\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^=]+)\s*=\s*ANY\s*\(\{[^\}]+\}::[^\)]+\)\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^<>=!]+)\s*[<>=!]+\s*\'[^\']+\'::[^\)]+\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^.]+)\.([^)=\s]+)[^)]*\)', r'\2', qplan['Join Filter'])
    if 'Hash Cond' in qplan:
        qplan['Hash Cond'] = re.search(r'\b(\w+)\.(\w+)', qplan['Hash Cond']).group(2) if re.search(r'\b(\w+)\.(\w+)', qplan['Hash Cond']) else qplan['Hash Cond']
    if 'Recheck Cond' in qplan:
        qplan['Recheck Cond'] = re.search(r'\((\w+)', qplan['Recheck Cond']).group(1) if re.search(r'\((\w+)', qplan['Recheck Cond']) else qplan['Recheck Cond']     
    if 'Cache Key' in qplan:
        qplan['Cache Key'] = re.search(r'\.(\w+)$', qplan['Cache Key']).group(1) if re.search(r'\.(\w+)$', qplan['Cache Key']) else qplan['Cache Key']
        

    if 'Plans' in qplan:
        for child in qplan['Plans']:
            simplify(child)
    return qplan

# only keep the filter condition (e.g. Index Scan, Seq Scan)
def analyze_filter(qplan) -> str:
    reduced_plan = {}
    if 'Filter' in qplan:
        reduced_plan['Node Type'] = qplan['Node Type']
        reduced_plan['Filter'] = qplan['Filter']
    if 'Plans' in qplan:
        filtered_plans = [analyze_filter(child) for child in qplan['Plans']]
        # only add non empty plans
        filtered_plans = [plan for plan in filtered_plans if plan]
        if filtered_plans:
            reduced_plan['Plans'] = filtered_plans
    return simplify(reduced_plan)

def psql_tpch_profiling(query_id, write_to_file=False):
    conn = dc.get_db_connection('dummydb')
    cur = conn.cursor()
    prefix = 'EXPLAIN (FORMAT JSON) '
    plans = []

    # run queries and get the json format query plans
    plans_query_i = qr.run_query_psql(cur, query_id, prefix)
    plans_query_i_json = []
    for plan in plans_query_i:
        json_plan = json.loads(plan)
        plans_query_i_json.append(json_plan)
    plans.append(plans_query_i_json)

    # simplify the query plans
    simplified = []
    for i, qplan in enumerate(plans[0]):
        s = simplify(qplan)
        simplified.append(s)

        # persist plans to file if intended
        if write_to_file:
            write_qp_to_file(query_id, i, s)

    return simplified

# persist query plans to files
def write_qp_to_file(query_id, plan_index, plan_data):
    # defining directory path
    dir = f'results/tpch/qplans/q{query_id}'
    # creating dir if not exist already
    os.makedirs(dir, exist_ok=True)
    filename = os.path.join(dir, f'q{query_id}_p{plan_index}.json')
    with open(filename, mode='w', encoding='UTF-8') as file:
        file.write(json.dumps(plan_data, indent=4))

# get all tpch queries to dict for comparison
# returns dictionary of all 
def all_qplans_tpch_to_dict(write_to_file=False):
    all_qplans_tpch = {}
    for query_id in query_ids:
        qp_key = f'Q{query_id}'
        try: 
            simplified_plans = psql_tpch_profiling(query_id, write_to_file)
            all_qplans_tpch[qp_key] = simplified_plans
            print(f'Success proccesing {qp_key}')
        except Exception as e:
            print(f'Failed to process {qp_key}: {e}')
    return all_qplans_tpch
#all_qplans_tpch_to_dict(write_to_file=True)

# categorizing  query plans
def compare_query_plans(query_plans):
    #categories of query plans
    categories = []

    for plan in query_plans:
        category_found = False
        for category in categories:

            if plan == category[0]:
                category.append(plan)
                category_found = True
        if not category_found:
            categories.append([plan])
    return categories

def test_dump() -> None:
    q9_plans = psql_tpch_profiling(9)
    result = compare_query_plans(q9_plans)
    total_plans = sum(len(category) for category in result)

    for i, category in enumerate(result):
        frequency = (len(category) / total_plans) * 100
        print(f"Plan Category {i}: {len(category)} plans, frequency: {frequency:.4f}%")

    print(f"\nTotal categories: {len(result)}")
    print(f"Total plans: {sum(len(category) for category in result)}") 

###############################################################
###### here begins the join order benchmark section ##########
###############################################################

def fetch_queries(dir: str) -> List[str]:
    job_queries = os.listdir(dir)
    job_queries = [file for file in job_queries if not file.startswith('.')]
    job_queries.sort(key=extract_number) # see utils.py
    return job_queries

def process_queries(queries: List[str], cur, prefix: str, dir: str, process_func) -> List[Tuple[str, dict]]:
    plans = []
    for query in queries: 
        query_id = query.split('.')[0]
        query_path = os.path.join(dir, query)
        with open(query_path, mode='r', encoding='UTF-8') as file:
            query_template = file.read()
            cur.execute(prefix + query_template)
            plan = cur.fetchall()
            processed_plan = process_func(plan[0][0][0]['Plan'])
            plans.append((query_id,processed_plan))
            file.close()
    return plans

def write_plans_to_file(plans: List[Tuple[str, dict]], dir: str) -> None:
    count_successful_writes = 0
    for plan in plans:
        filename = os.path.join(dir, f'{plan[0]}.json')
        with open(filename, 'w', encoding='UTF-8') as file:
            file.write(json.dumps(plan[1], indent=4))
            file.close()
            count_successful_writes += 1
    if count_successful_writes == len(plans):
        print('all plans written successfully')
    

# e.g. db_cursor('job') for join order benchmark db, db_cursor('dummydb') for tpc-h db
def db_cursor(db) -> cursor:
    conn = dc.get_db_connection(db)
    cur = conn.cursor()
    return cur

# 0 for EXPLAIN (FORMAT JSON), 1 for EXPLAIN (ANALYZE, FORMAT JSON)
def job_profiling(prefix: int, process_func, output_dir: str) -> None:
    cur = db_cursor('job')
    prefixes = ['EXPLAIN (FORMAT JSON) ', 'EXPLAIN (FORMAT JSON, ANALYZE) ']
    job_dir = 'resources/queries_job/'
    job_queries = fetch_queries(job_dir)
    plans = process_queries(job_queries, cur, prefixes[prefix], job_dir, process_func)
    write_plans_to_file(plans, output_dir)


###############################################################
###### here ends the join order benchmark section ############
###############################################################


###############################################################
###### here starts the countries example section ############
###############################################################

def benchmark_country_example():
    

###############################################################
###### here ends the countries example section ############
###############################################################



def main():
    #job_profiling(0, simplify, 'results/job/qplans/')
    job_profiling(1, simplify,'results/job/qplans/')

if __name__ == '__main__':
    main()

