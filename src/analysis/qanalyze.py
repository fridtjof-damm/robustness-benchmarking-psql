import json
import re
import os
import psycopg2 as pg 
from psycopg2.extensions import cursor
import src.qrun as qr
import src.qgen as qg
import src.analysis.dummy.dummy_data_gen as ddg
from src.utils.utils import extract_number
import src.utils.db_connector as dc
import pprint as pp
from typing import List, Tuple
import pandas as pd


# db config
db_params = {
        "database": "dummydb",
        "user": "fridtjofdamm",
        "password": "",
        "host": "localhost",
        "port": "5432"
    }

def simplify(qplan):
    # handling the plan structure
    # check if node exists, then delete

    # GATHERED FROM EXPLAIN prefix
    if 'Planning Time' in qplan:
        del qplan['Planning Time']
    if 'Parallel Aware' in qplan:
        del qplan['Parallel Aware']
    if 'Async Capable' in qplan:
        del qplan['Async Capable']
    if 'Single Copy' in qplan:
        del qplan['Single Copy']
    if 'Alias' in qplan:
        del qplan['Alias']
    if 'Parent Relationship' in qplan:
        del qplan['Parent Relationship']
    if 'Relation Name' in qplan:
        del qplan['Relation Name']
    if 'Alias' in qplan:
        del qplan['Alias']
    if 'Plan Width' in qplan:
        del qplan['Plan Width']
    if 'Actual Loops' in qplan:
        del qplan['Actual Loops']
    if 'Rows Removed by Index Recheck' in qplan:
        del qplan['Rows Removed by Index Recheck']
    if 'Exact Heap Blocks' in qplan:
        del qplan['Exact Heap Blocks']
    if 'Lossy Heap Blocks' in qplan:
        del qplan['Lossy Heap Blocks']
    if 'Parent Relationship' in qplan:
        del qplan['Parent Relationship']
    
    # EXPLAIN ANALYZE prefix
    if 'Filter' in qplan:
        qplan['Filter'] = re.sub(r'\(\(([^)]+?)\)::text\s*=\s*\'([^\']+)\'::text\)', r'\1 = \'\2\'', qplan['Filter'])
        qplan['Filter'] = re.sub(r'\(\(([^)]+?)\)::text\s*~~\s*\'([^\']+)\'::text\)', r'\1 LIKE \'\2\'', qplan['Filter'])
        qplan['Filter'] = re.sub(r'\\', '', qplan['Filter'])  # Remove unnecessary escape characters    
        #qplan['Filter'] = re.sub(r'\(\(([^)]+?)\)::text[^)]+\)', r'(\1)', qplan['Filter']) # q2 TIN NICKEL etc.
        #qplan['Filter'] = re.sub(r'\(([^<>=!]+)\s*[<>=!]+\s*[^)]+\)', r'(\1)', qplan['Filter']) # q3 shipdate filter simplification
        #qplan['Filter'] = re.sub(r'\(*([^()]+)\)*(?:%\'::text)?(?:\s*(?:AND|OR)\s*\(*\1\)*(?:%\'::text)?)*', r'\1', qplan['Filter'])
        #qplan['Filter'] = re.sub(r'(?:^|\s*(?:AND|OR)\s*)(?:\(*)([^()]+?)(?:\)*(?:%\'::text)?)(?=\s*(?:AND|OR|$))', r'\1', qplan['Filter'])
        #qplan['Filter'] = ''.join(sorted(set(re.sub(r'[^a-zA-Z]', '', re.sub(r'(?:^|\s*(?:AND|OR)\s*)(?:\(*)([^()]+?)(?:\)*(?:%\'::text)?)(?=\s*(?:AND|OR|$))', r'\1', qplan['Filter']))), key=lambda x: re.sub(r'[^a-zA-Z]', '', qplan['Filter']).index(x)))
        #match = re.search(r'\(([^=]+)\s*=\s*[^)]+\)', qplan['Filter'])
        #if match:
        #    attr = match.group(1).strip()
        #    val = f'({attr})'
        #    qplan['Filter'] = val
    # further selection from index scan queries
    # if '' in qplan:
    if 'Index Cond' in qplan:
        qplan['Index Cond'] = re.sub(r'\(\(([^)]+?)\)::text\s*=\s*\'([^\']+)\'::text\)', r'\1 = \'\2\'', qplan['Index Cond'])
        qplan['Index Cond'] = re.sub(r'\(\(([^)]+?)\)::text\s*~~\s*\'([^\']+)\'::text\)', r'\1 LIKE \'\2\'', qplan['Index Cond'])
        qplan['Index Cond'] = re.sub(r'\\', '', qplan['Index Cond'])  # Remove unnecessary escape characters
   # match = re.search(r'\(([^=]+)\s*=\s*[^)]+\)', qplan['Index Cond'])
   #     if match:
   #         attr = match.group(1).strip()
   #         val = f'{attr}'
   #         qplan['Index Cond'] = val 

    if 'Join Filter' in qplan:
        qplan['Join Filter'] = re.sub(r'\(([^=]+)\s*=\s*\'[^\']+\'::bpchar\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^=]+)\s*=\s*\'[^\']+\'::[^\)]+\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^=]+)\s*=\s*ANY\s*\(\{[^\}]+\}::[^\)]+\)\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^<>=!]+)\s*[<>=!]+\s*\'[^\']+\'::[^\)]+\)', r'(\1)', qplan['Join Filter'])
        qplan['Join Filter'] = re.sub(r'\(([^.]+)\.([^)=\s]+)[^)]*\)', r'\2', qplan['Join Filter'])
    if 'Hash Cond' in qplan:
        qplan['Hash Cond'] = re.search(r'\b(\w+)\.(\w+)', qplan['Hash Cond']).group(2) if re.search(r'\b(\w+)\.(\w+)', qplan['Hash Cond']) else qplan['Hash Cond']
    if 'Recheck Cond' in qplan: # Remove unnecessary escape characters
        qplan['Recheck Cond'] = re.sub(r'\(\(([^)]+?)\)::text\s*=\s*\'([^\']+)\'::text\)', r'\1 = \2', qplan['Recheck Cond'])
        qplan['Recheck Cond'] = re.sub(r'\\', '', qplan['Recheck Cond']) 
    if 'Cache Key' in qplan:
        qplan['Cache Key'] = re.search(r'\.(\w+)$', qplan['Cache Key']).group(1) if re.search(r'\.(\w+)$', qplan['Cache Key']) else qplan['Cache Key']
        

    if 'Plans' in qplan:
        for child in qplan['Plans']:
            simplify(child)
    return qplan

# only keep the filter condition (e.g. Index Scan, Seq Scan)
def analyze_filter(qplan) -> str:
    if isinstance(qplan, (list, tuple)):
        if len(qplan) > 0:
            if isinstance(qplan[0], (list, tuple)):
                qplan = qplan[0]
            if isinstance(qplan[0], dict) and 'Plan' in qplan[0]:
                qplan = qplan[0]['Plan']
            elif isinstance(qplan, tuple) and len(qplan) > 0:  # Handle the tuple case
                qplan = qplan[0]
                if isinstance(qplan, dict) and 'Plan' in qplan:
                    qplan = qplan['Plan']
                elif isinstance(qplan[0], dict) and 'Plan' in qplan[0]:
                    qplan = qplan[0]['Plan']

    reduced_plan = {}
    if 'Filter' in qplan:
        reduced_plan['Node Type'] = qplan['Node Type']
        reduced_plan['Filter'] = qplan['Filter']
    if 'Index Cond' in qplan:
        reduced_plan['Node Type'] = qplan['Node Type']
        reduced_plan['Index Cond'] = clean_condition(qplan['Index Cond'])
    if 'Recheck Cond' in qplan:
        reduced_plan['Node Type'] = qplan['Node Type']
        reduced_plan['Recheck Cond'] = clean_condition(qplan['Recheck Cond'])    
    if 'Plans' in qplan:
        filtered_plans = [analyze_filter(child) for child in qplan['Plans']]
        # only add non empty plans
        filtered_plans = [plan for plan in filtered_plans if plan]
        if filtered_plans:
            reduced_plan['Plans'] = filtered_plans
            
    return reduced_plan

def clean_condition(condition: str) -> str:
    # Remove ::text and extra parentheses
    cleaned = condition.replace('::text', '')  # Remove ::text
    cleaned = cleaned.replace('((', '(')  # Remove extra opening parenthesis
    cleaned = cleaned.replace('))', ')')  # Remove extra closing parenthesis
    cleaned = cleaned.replace('(', '')  # Remove all remaining opening parentheses
    cleaned = cleaned.replace(')', '')  # Remove all remaining closing parentheses
    return cleaned



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

def profiling_country_example() -> None:
    prefix = 'EXPLAIN (FORMAT JSON, ANALYZE) '
    conn = dc.get_db_connection('countries')
    cur = conn.cursor()
    queries = qg.generate_country_queries()
    plans: list[(str,dict)] = []
    for idx, query in enumerate(queries):
        query = prefix + query
        cur.execute(query)
        plan = cur.fetchall()
        plan[0][0][0]['Plan'] = simplify(plan[0][0][0]['Plan'])

        plans.append((str(idx+1),plan))
    
    output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
    os.makedirs(output_dir, exist_ok=True)

    write_plans_to_file(plans, output_dir)

    cur.close()
    conn.close()

####################################################
#### query plans directories to query info dicts ###
### extract scan nodes from plans for plotting #####
####################################################

# help function to traverse the dict 
def traverse(plan: dict, node_types: List[str], filters: List[List[Tuple[str, str]]], execution_times: List[float]) -> None:
    if isinstance(plan, list):
        for item in plan:
            traverse(item, node_types, filters, execution_times)
    elif isinstance(plan, dict):
        if 'Node Type' in plan and plan['Node Type'] not in  ['Limit', 'Gather']:
            node_types.append(plan['Node Type'])
        node_filters = []
        for key in ['Filter', 'Recheck Cond', 'Index Cond', 'Seq Scan', 'Index Scan']:
            if key in plan:
                matches = re.findall(r'(\w+)\s*(=|LIKE)\s*\'?([^\'\)]+)\'?', plan[key])
                for match in matches:
                    if len(match) == 3:
                        node_filters.append((match[0], match[2]))
        if node_filters:
            filters.append(node_filters)
        if 'Execution Time' in plan:
            execution_times.append(plan['Execution Time'])
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                traverse(subplan, node_types, filters,execution_times)
        if 'Plan' in plan:
            traverse(plan['Plan'], node_types, filters, execution_times)

# help funtion to collect the node types
def extract_node_types_from_plan(plan: dict) -> Tuple[List[str], List[List[Tuple[str, str]]], List[float]]:
    node_types = []
    filters = []
    execution_times = []
    traverse(plan, node_types, filters, execution_times)
    return node_types, filters, execution_times


##
## structure of the query info dict: node_types e.g. Index Scan , filters e.g. (country , India) , execution_time e.g. 0.1 (ms)
##
def query_nodes_info(directory: str) -> dict[int, tuple[list[str], list[List[Tuple[str, str]]], list[float]]]:
    query_info = {}

    for filename in sorted([f for f in os.listdir(directory) if f.endswith('.json')], key=extract_number):
        query_id = int(filename.split('.')[0])
        with open(os.path.join(directory, filename), 'r') as file:
            plan = json.load(file)
            node_types, filters, execution_times = extract_node_types_from_plan(plan)
            query_info[query_id] = (node_types, filters, execution_times)
    return query_info

# write the query info dict to csv 
def query_nodes_info_to_csv(query_info: dict[int, tuple[list[str], list[List[Tuple[str, str]]], list[float]]], output_dir: str, output_file: str) -> None:
    data = []
    for query_id, (node_types, filters, execution_times) in query_info.items():
        for node_type, node_filters, execution_time in zip(node_types, filters, execution_times):
            combined_filters = ','.join([f'({key},{value})' for key, value in node_filters])
            data.append((query_id, node_type, combined_filters, execution_time))

    # Convert the list of tuples into a DataFrame
    df = pd.DataFrame(data, columns=['Query ID', 'Node Type', 'Filters', 'Execution Time'])

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Write the DataFrame to a CSV file
    output_path = os.path.join(output_dir, output_file)
    df.to_csv(output_path, index=False)

    print(f"CSV file has been written successfully to {output_path}")


###############################################################
###### here begins the skew example section ############
###############################################################

def profiling_skew_example() -> None:
    prefix = 'EXPLAIN (FORMAT JSON, ANALYZE) '
    conn = dc.get_db_connection('skew_example')
    cur = conn.cursor()
    queries = qg.generate_skew_queries()
    plans: list[(str,dict)] = []
    for idx, query in enumerate(queries):
        query = prefix + query
        cur.execute(query)
        plan = cur.fetchall()
        #plan[0][0][0]['Plan'] = simplify(plan[0][0][0]['Plan'])

        plans.append((str(idx+1),plan))
    
    output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans'
    os.makedirs(output_dir, exist_ok=True)

    write_plans_to_file(plans, output_dir)

    cur.close()
    conn.close()



def main():
    #job_profiling(0, simplify, 'results/job/qplans/')
    #job_profiling(1, simplify,'results/job/qplans/')
    #profiling_country_example()
    #print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[1])
    #print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[236])
    
    # run skew example profiling
    #profiling_skew_example()
    #print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'))
    
    # skew to csv 
    directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
    output_dir = 'Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example'
    output_file = 'skew_example.csv'
    query_info = query_nodes_info(directory)
    query_nodes_info_to_csv(query_info, output_dir, output_file)

if __name__ == '__main__':
    main()

