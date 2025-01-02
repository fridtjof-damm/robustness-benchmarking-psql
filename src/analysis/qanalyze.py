import time
import json
import re
import os
import psycopg2 as pg
from psycopg2.extensions import cursor
import src.qrun as qr
import src.qgen as qg
from src.utils.utils import extract_number
import src.utils.db_connector as dc
import pprint as pp
from typing import List, Tuple, Dict, Union
import pandas as pd


def simplify(qplan, benchmark):
    plan_old = qplan.copy()

    # add/remove nodes to be removed here
    keys_to_remove = [
        'Planning Time', 'Parallel Aware', 'Async Capable', 'Single Copy', 'Alias',
        'Parent Relationship', 'Relation Name', 'Plan Width', 'Actual Loops',
        'Rows Removed by Index Recheck', 'Exact Heap Blocks', 'Lossy Heap Blocks'
    ]
    # Remove the keys from qplan
    """   for key in keys_to_remove:
            if key in qplan:
                del qplan[key]"""

    # EXPLAIN ANALYZE prefix
    if 'Filter' in qplan:
        if benchmark == 'tpch':
            # print("Original Filter:", qplan['Filter'])  # Debug print
            # Simplify the filter using the simplify_filter function
            qplan['Filter'] = simplify_filter(qplan['Filter'], benchmark)
            # print("Simplified Filter:", qplan['Filter'])  # Debug print
        elif benchmark == 'job':
            qplan['Filter'] = re.sub(
                r'\(\(([^)]+?)\)::text\s*~~\s*\'([^\']+)\'::text\)', r'\1 LIKE \'\2\'', qplan['Filter'])

    if 'Index Cond' in qplan:
        if benchmark == 'tpch':
            qplan['Index Cond'] = re.sub(
                r'\(([^=]+)\s*=\s*\'([^\']+)\'\)', r'(\1,\'\2\')', qplan['Index Cond'])
        elif benchmark == 'job':
            qplan['Index Cond'] = re.sub(
                r'\(\(([^)]+?)\)::text\s*=\s*\'([^\']+)\'::text\)', r'\1 = \'\2\'', qplan['Index Cond'])
            qplan['Index Cond'] = re.sub(
                r'\(\(([^)]+?)\)::text\s*~~\s*\'([^\']+)\'::text\)', r'\1 LIKE \'\2\'', qplan['Index Cond'])
            # Remove unnecessary escape characters
        qplan['Index Cond'] = re.sub(r'\\\\', '', qplan['Index Cond'])

    if 'Join Filter' in qplan:
        if benchmark == 'tpch':
            qplan['Join Filter'] = re.sub(
                r'\(([^=]+)\s*=\s*\'[^\']+\'::bpchar\)', r'(\1)', qplan['Join Filter'])
            qplan['Join Filter'] = re.sub(
                r'\(([^=]+)\s*=\s*\'[^\']+\'::[^\)]+\)', r'(\1)', qplan['Join Filter'])
            qplan['Join Filter'] = re.sub(
                r'\(([^=]+)\s*=\s*ANY\s*\(\{[^\}]+\}::[^\)]+\)\)', r'(\1)', qplan['Join Filter'])
            qplan['Join Filter'] = re.sub(
                r'\(([^<>=!]+)\s*[<>=!]+\s*\'[^\']+\'::[^\)]+\)', r'(\1)', qplan['Join Filter'])
            qplan['Join Filter'] = re.sub(
                r'\(([^.]+)\.([^)=\s]+)[^)]*\)', r'\2', qplan['Join Filter'])
        if 'Hash Cond' in qplan:
            qplan['Hash Cond'] = re.search(r'\b(\w+)\.(\w+)', qplan['Hash Cond']).group(
                2) if re.search(r'\b(\w+)\.(\w+)', qplan['Hash Cond']) else qplan['Hash Cond']
        if 'Recheck Cond' in qplan:  # Remove unnecessary escape characters
            qplan['Recheck Cond'] = re.sub(
                r'\(\(([^)]+?)\)::text\s*=\s*\'([^\']+)\'::text\)', r'\1 = \2', qplan['Recheck Cond'])
            qplan['Recheck Cond'] = re.sub(r'\\\\', '', qplan['Recheck Cond'])
        if 'Cache Key' in qplan:
            qplan['Cache Key'] = re.search(r'\.(\w+)$', qplan['Cache Key']).group(
                1) if re.search(r'\.(\w+)$', qplan['Cache Key']) else qplan['Cache Key']

    if 'Plans' in qplan:
        for child in qplan['Plans']:
            simplify(child, benchmark)

    return qplan

# only keep the filter condition (e.g. Index Scan, Seq Scan)


def simplify_filter(filter_str, benchmark):
    # Custom transformations for specific cases
    if benchmark == 'tpch':
        # print("Original Filter:", filter_str)  # Debug print
        # Remove unnecessary escape characters
        filter_str = re.sub(r'\\\\', '', filter_str)
        # print("After removing escape characters:", filter_str)  # Debug print

        # Split the filter string at 'AND' and 'OR' to handle each condition separately
        conditions = re.split(r' AND | OR ', filter_str)
        transformed_conditions = []

        for condition in conditions:
            # Remove type casts
            condition = re.sub(r'::[a-zA-Z\s]+', '', condition)
            # print("After removing type casts:", condition)  # Debug print

            # Apply transformations based on specific patterns
            match condition:
                case _ if re.search(r'p_type LIKE', condition):
                    # Transform (p_type LIKE '%TIN') to (p_type,'TIN')
                    condition = re.sub(
                        r'p_type LIKE \'%([^%]+)\'', r'(p_type,\'\1\')', condition)
                case _ if re.search(r'p_size =', condition):
                    # Transform (p_size = 1) to (p_size,1)
                    condition = re.sub(r'p_size = (\d+)',
                                       r'(p_size,\1)', condition)
                case _ if re.search(r'l_shipdate >', condition):
                    # Transform (l_shipdate > '1995-03-27') to (l_shipdate ,'1995-03-27')
                    condition = re.sub(
                        r'l_shipdate > \'([^\']+)\'', r'(l_shipdate,\'\1\')', condition)
                case _ if re.search(r'c_mktsegment =', condition):
                    # Transform (c_mktsegment = 'HOUSEHOLD') to (c_mktsegment,'HOUSEHOLD')
                    condition = re.sub(
                        r'c_mktsegment = \'([^\']+)\'', r'(c_mktsegment,\'\1\')', condition)
                case _ if re.search(r'r_name =', condition):
                    # Transform (r_name = 'AFRICA') to (r_name,'AFRICA')
                    condition = re.sub(
                        r'r_name = \'([^\']+)\'', r'(r_name,\'\1\')', condition)
                case _ if re.search(r'o_orderdate >=', condition):
                    # Transform (o_orderdate >= '1993-01-01') to (o_orderdate,'1993-01-01')
                    condition = re.sub(
                        r'o_orderdate >= \'([^\']+)\'', r'(o_orderdate,\'\1\')', condition)
                case _ if re.search(r'o_orderdate <=', condition):
                    # Transform (o_orderdate <= '1996-12-31') to (o_orderdate,'1996-12-31')
                    condition = re.sub(
                        r'o_orderdate <= \'([^\']+)\'', r'(o_orderdate,\'\1\')', condition)
                case _ if re.search(r'o_orderdate <', condition):
                    # Transform (o_orderdate < '1994-01-01 00:00:00') to (o_orderdate,'1994-01-01')
                    condition = re.sub(
                        r'o_orderdate < \'([^\']+)\s[^\']+\'', r'(o_orderdate,\'\1\')', condition)
                case _ if re.search(r'l_shipdate >=', condition):
                    # Transform (l_shipdate >= '1995-01-01') to (l_shipdate,'1995-01-01')
                    condition = re.sub(
                        r'l_shipdate >= \'([^\']+)\'', r'(l_shipdate,\'\1\')', condition)
                case _ if re.search(r'l_shipdate <=', condition):
                    # Transform (l_shipdate <= '1996-12-31') to (l_shipdate,'1996-12-31')
                    condition = re.sub(
                        r'l_shipdate <= \'([^\']+)\'', r'(l_shipdate,\'\1\')', condition)
                case _ if re.search(r'l_shipdate <', condition):
                    # Transform (l_shipdate < '1993-02-01 00:00:00') to (l_shipdate,'1993-02-01')
                    condition = re.sub(
                        r'l_shipdate < \'([^\']+)\s[^\']+\'', r'(l_shipdate,\'\1\')', condition)
                case _ if re.search(r'n_name =', condition):
                    # Transform (n_name = 'ALGERIA') to (n_name,'ALGERIA')
                    condition = re.sub(
                        r'n_name = \'([^\']+)\'', r'(n_name,\'\1\')', condition)
                case _ if re.search(r'p_type =', condition):
                    # Transform (p_type = 'TIN') to (p_type,'TIN')
                    condition = re.sub(
                        r'p_type = \'([^\']+)\'', r'(p_type,\'\1\')', condition)
                case _ if re.search(r'p_brand =', condition):
                    # Transform (p_brand = 'BRAND#11') to (p_brand,'BRAND#11')
                    condition = re.sub(
                        r'p_brand = \'([^\']+)\'', r'(p_brand,\'\1\')', condition)
                case _ if re.search(r'p_container =', condition):
                    # Transform (p_container = 'SM CASE') to (p_container,'SM CASE')
                    condition = re.sub(
                        r'p_container = \'([^\']+)\'', r'(p_container,\'\1\')', condition)
                case _ if re.search(r'\(\(p_type\): : text ~~', condition):
                    # Transform ((p_type): : text ~~ '%TIN': : text) to (p_type),'%TIN'
                    condition = re.sub(
                        r'\(\(p_type\): : text ~~ \'%([^%]+)\'', r'(p_type),\'\1\'', condition)
                case _ if re.search(r'\(p_type\),\'[^\']+\': : text', condition):
                    # Transform (p_type),'TIN': : text) to (p_type,'TIN')
                    condition = re.sub(
                        r'\(p_type\),\'([^\']+)\': : text\)', r'(p_type,\'\1\')', condition)

            # Remove unnecessary escape characters from the transformed condition
            condition = condition.replace("\\'", "'")
            condition = clean_condition(condition)

            transformed_conditions.append(condition)

        # Recombine the transformed conditions with commas
        filter_str = ','.join(transformed_conditions)

    return filter_str


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
    # Remove all remaining opening parentheses
    cleaned = cleaned.replace('(', '')
    # Remove all remaining closing parentheses
    cleaned = cleaned.replace(')', '')
    return cleaned

###############################################################
###### here begins the standard tpch  section ##########
###############################################################


def psql_tpch_profiling(query_id, write_to_file=False):
    print(f"tpc-h query {query_id}:")

    # Load the query template from the file
    template_path = f'/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/resources/queries_tpch/{query_id}.sql'
    with open(template_path, 'r') as file:
        query_template = file.read()
        file.close()
    queries, _ = qg.generate_query(query_template, query_id)

    total_queries = len(queries)

    conn = dc.get_db_connection('dummydb')
    cur = conn.cursor()
    prefix = 'EXPLAIN (FORMAT JSON, ANALYZE) '
    plans = []
    simplified_plans = []
    # run queries and get the json format query plans
    print(f"query execution starts ...")
    for i, query in enumerate(queries):
        cur.execute(prefix + query)
        plan = cur.fetchall()
        plans.append(plan)
        plan[0][0][0]['Plan'] = simplify(plan[0][0][0]['Plan'], 'tpch')
        simplified_plans.append(plan)
        percentage = ((i + 1) / total_queries) * 100
        print(f"{percentage:.2f}% of queries executed")

    # persist plans to file if intended
    if write_to_file:
        for i, qplan in enumerate(plans):
            write_qp_to_file(query_id, i, qplan, simplified=False)
        for i, qplan in enumerate(simplified_plans):
            write_qp_to_file(query_id, i, qplan, simplified=True)
    print(f"execution stage done")
    return simplified_plans

# persist query plans to files


def write_qp_to_file(query_id, plan_index, plan_data, simplified=False):
    # defining directory path
    dir = f'results/tpch/qplans/q{query_id}'
    if simplified:
        dir = os.path.join(dir, 'simplified')
    # creating dir if not exist already
    os.makedirs(dir, exist_ok=True)
    filename = os.path.join(dir, f'{plan_index+1}.json')
    with open(filename, mode='w', encoding='UTF-8') as file:
        file.write(json.dumps(plan_data, indent=4))


###############################################################
###### new function to profile parameterized queries ##########
###############################################################


def profile_parameterized_queries(query_id):
    # Profile the parameterized queries
    simplified_plans = psql_tpch_profiling(query_id, write_to_file=True)
    # Directory where the plans are saved
    directory = f'results/tpch/qplans/q{query_id}'
    # Get the correct query parameters for the given query_id
    query_parameters = set(qg.tpch_query_parameters.get(f'q{query_id}', []))
    # Debug print to check query parameters
    # print(f"Query parameters for q{query_id}: {query_parameters}")
    # Get query nodes info
    query_info = query_nodes_info(directory, query_parameters)
    if not query_info:
        # print(f"failed to extract nodes info of query {query_id}")
        return

    # Write query nodes info to CSV
    output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/new_csvs'
    output_file = f'q{query_id}.csv'
    query_nodes_info_to_csv(query_info, output_dir, output_file)


def profile_custom_tpch_queries():
    conn = dc.get_db_connection('dummydb')
    cur = conn.cursor()
    queries = qg.gen_custom_queries_non_aggregated()
    prefix = 'EXPLAIN (FORMAT JSON, ANALYZE) '
    plans = []
    for query in queries:
        cur.execute(prefix + query)
        plan = cur.fetchall()
        plan[0][0][0]['Plan'] = simplify(plan[0][0][0]['Plan'])
        plans.append(plan)

    write_plans_to_file(plans, 'results/tpch/custom_queries')
    return plans


###############################################################
###### here begins the join order benchmark section ##########
###############################################################

def fetch_queries(dir: str) -> List[str]:
    job_queries = os.listdir(dir)
    job_queries = [file for file in job_queries if not file.startswith('.')]
    job_queries.sort(key=extract_number)  # see utils.py
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
            plan[0][0][0]['Plan'] = process_func(plan[0][0][0]['Plan'])
            plans.append((query_id, plan))
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
    print(job_queries[0])
    plans = process_queries(
        job_queries, cur, prefixes[prefix], job_dir, process_func)
    write_plans_to_file(plans, output_dir)


def profiling_parameterized_job_queries(output_dir: str) -> None:
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    print("Dir exists, now establishing db connection")
    conn = dc.get_db_connection('job')
    cur = conn.cursor()
    print("got db connection, now generating queries")
    # Generate the job queries
    queries = qg.generate_job_query15a()
    print(len(queries))
    print("queries generated, now executing queries")
    # Execute EXPLAIN ANALYZE for each query and save the plan as a JSON file
    for i, query in enumerate(queries, start=1):
        # print(f"executing query {i}")
        cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
        # print(f"query executed, now fetching plan")
        plan = cur.fetchone()[0][0]  # Get the JSON plan
        # print(f"plan fetched, now storing plan")
        # Save the plan to a JSON file
        output_path = os.path.join(output_dir, f"{i}.json")
        with open(output_path, 'w') as file:
            json.dump(plan, file, indent=4)

    # Close the database connection
    cur.close()
    conn.close()


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
    plans: list[(str, dict)] = []
    for idx, query in enumerate(queries):
        query = prefix + query
        cur.execute(query)
        plan = cur.fetchall()
        plan[0][0][0]['Plan'] = simplify(plan[0][0][0]['Plan'])

        plans.append((str(idx+1), plan))

    output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
    os.makedirs(output_dir, exist_ok=True)

    write_plans_to_file(plans, output_dir)

    cur.close()
    conn.close()

####################################################
#### query plans directories to query info dicts ###
### extract scan nodes from plans for plotting #####
####################################################


def traverse(plan: Dict, node_types: List[str], filters: List[List[Tuple[str, str]]], execution_times: List[float], cardinalities: List[Tuple[int, int]], query_parameters: set) -> None:
    if isinstance(plan, list):
        for item in plan:
            traverse(item, node_types, filters, execution_times,
                     cardinalities, query_parameters)
    elif isinstance(plan, dict):
        if 'Node Type' in plan:
            if plan['Node Type'] not in ['Limit', 'Gather']:
                node_types.append(plan['Node Type'])
        node_filters = []
        appended_parameters = set()  # Track appended parameters for the current plan
        for key in ['Filter', 'Recheck Cond', 'Index Cond', 'Seq Scan', 'Index Scan']:
            if key in plan:
                # Updated regular expression to handle parentheses around parameter names
                matches = re.findall(
                    r'\(?(\w+)\)?\s*(=|LIKE|<|>|<=|>=|~~)\s*\'?([^\'\)]+)\'?', plan[key])
                # Debug print to check matches
                print(f"Matches found in {key}: {matches}")
                for match in matches:
                    condition = (match[0], match[2])
                    if len(match) == 3 and match[0] in query_parameters and condition not in appended_parameters:
                        node_filters.append(condition)
                        # Mark parameter as appended
                        appended_parameters.add(condition)
        if node_filters:
            filters.append(node_filters)
            # Debug print to check appended filters
            print(f"Appended filters: {node_filters}")
        else:
            # Debug print to check when no filters are appended
            print(f"No filters appended for plan.")
        if 'Execution Time' in plan:
            execution_times.append(plan['Execution Time'])
        if 'Plan Rows' in plan and 'Actual Rows' in plan:
            cardinalities.append((plan['Plan Rows'], plan['Actual Rows']))
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                traverse(subplan, node_types, filters, execution_times,
                         cardinalities, query_parameters)
        if 'Plan' in plan:
            traverse(plan['Plan'], node_types, filters,
                     execution_times, cardinalities, query_parameters)


def extract_node_types_from_plan(plan: Dict, query_parameters: set) -> Tuple[List[str], List[List[Tuple[str, str]]], List[float], List[Tuple[int, int]]]:
    # print(f"Extracting node types with query parameters: {query_parameters}")
    node_types = []
    filters = []
    execution_times = []
    cardinalities = []
    traverse(plan, node_types, filters, execution_times,
             cardinalities, query_parameters)
    return node_types, filters, execution_times, cardinalities

##
# structure of the query info dict: node_types e.g. Index Scan , filters e.g. (country , India) , execution_time e.g. 0.1 (ms)
##


def query_nodes_info(directory: str, query_parameters: set) -> Dict[Union[int, Tuple[int, str]], Tuple[List[str], List[List[Tuple[str, str]]], List[float], List[Tuple[int, int]]]]:
    query_info = {}
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            query_id, suffix = extract_number(filename)
            if query_id == float('inf'):
                try:
                    query_id = int(filename.split('.')[0])
                    suffix = ''
                except ValueError:
                    continue
            with open(os.path.join(directory, filename), 'r', encoding='UTF-8') as file:
                plan = json.load(file)
                node_types, filters, execution_times, cardinalities = extract_node_types_from_plan(
                    plan, query_parameters)
                # Debug print to check extracted filters
                # print(f"Extracted filters for {filename}: {filters}")
                query_info[(query_id, suffix) if suffix else query_id] = (
                    node_types, filters, execution_times, cardinalities)
    # Debug print to check final query_info
    # print(f"Final query_info: {query_info}")
    return query_info

# write the query info dict to csv


def query_nodes_info_to_csv(query_info, output_dir: str, output_file: str) -> None:
    data = []
    # Sort the keys of the query_info dictionary
    sorted_keys = sorted(query_info.keys(), key=lambda x: (
        x[0], x[1]) if isinstance(x, tuple) else (x, ''))
    for query_id in sorted_keys:
        node_types, filters, execution_times, cardinalities = query_info[query_id]
        combined_node_types = ', '.join(sorted(node_types))
        combined_filters = ', '.join([f'({k},{v})' for filter_list in sorted(
            filters) for k, v in sorted(filter_list)])
        # Debug statement
        # print(f"Writing filters for query {query_id}: {combined_filters}")
        # Assuming you want the total execution time
        combined_execution_time = sum(execution_times)
        combined_cardinality = ', '.join(
            [f'({e},{a})' for e, a in cardinalities])
        data.append([query_id, combined_node_types, combined_filters,
                    combined_execution_time, combined_cardinality])

    df = pd.DataFrame(data, columns=[
        'Query ID', 'Node Types', 'Filters', 'Execution Time', 'Cardinality e/a'])
    output_path = os.path.join(output_dir, output_file)
    df.to_csv(output_path, index=False)
    print(f"csv has been written successfully to {output_path}")


# Example usage
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
# output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example'
# output_file = 'skew_example.csv'
# query_info = query_nodes_info(directory)
# query_nodes_info_to_csv(query_info, output_dir, output_file)

#########################################
### qerror for cardinality estimation ###
#########################################

# calculate the query error for cardinality estimation accuracy


def calc_qerror(query_info) -> dict[int, float]:
    qerrors = {}
    for query_id, (node_types, filters, execution_times, cardinalities) in query_info.items():
        qerror_sum = 0
        valid_cardinalities = 0
        for estimated, actual in cardinalities:
            if actual == 0 or estimated == 0:
                continue
            if actual >= estimated:
                qerror = actual / estimated
            else:
                qerror = estimated / actual
            qerror_sum += qerror
            valid_cardinalities += 1
        if valid_cardinalities > 0:
            qerrors[query_id] = qerror_sum / len(cardinalities)
        else:
            qerrors[query_id] = 0
    return qerrors

###############################################################
###### here begins the skew example section ############
###############################################################


def profiling_skew_example() -> None:
    prefix = 'EXPLAIN (FORMAT JSON, ANALYZE) '
    conn = dc.get_db_connection('skew_example')
    cur = conn.cursor()
    queries = qg.generate_skew_queries()
    plans: list[(str, dict)] = []
    for idx, query in enumerate(queries):
        query = prefix + query
        cur.execute(query)
        plan = cur.fetchall()
        plan[0][0][0]['Plan'] = simplify(plan[0][0][0]['Plan'])

        plans.append((str(idx+1), plan))

    output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
    os.makedirs(output_dir, exist_ok=True)

    write_plans_to_file(plans, output_dir)

    cur.close()
    conn.close()


def main():
    ############################
    ############################
    ## standard tpch section ###
    query_ids = [2, 3, 5, 7, 8, 12, 13, 14, 17]
    for i in query_ids:
        profile_parameterized_queries(i)
        print(f'finished profiling for query {i}')
        ##################
        ## job section ###
        # job_profiling(0, simplify, 'results/job/qplans/')
        # job_profiling(1, simplify,'results/job/qplans/')
        ##################
        # output_file = 'all_job.csv'
        # output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job'
        # directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/qplans'
        # query_info_job = query_nodes_info(directory)
        # print(query_info_job)
        # query_nodes_info_to_csv(query_info_job, output_dir, output_file)

        ######
        # paeaameterized job queries
        # profiling_parameterized_job_queries(output_dir)
        # output_file = '1d_parameterized_job.csv'
        # output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/parameterized/1d'
        # directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/parameterized/1d'
        # query_info_job_1d = query_nodes_info(directory)
        # print(query_info_job_1d)
        # query_nodes_info_to_csv(query_info_job_1d, output_dir, output_file)
        #################

        # job 15a
        # output_file = '15a_job.csv'
        # output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/15a'
        # directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/15a'

        # profiling_parameterized_job_queries(directory)

        # query_info_job_15a = query_nodes_info(directory)
        # print(type(query_info_job_15a))
        # query_nodes_info_to_csv(query_info_job_15a, output_dir, output_file)

        ############################
        ## country example #########
        # profiling_country_example()
        # print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[1])
        # print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[236])
        # country to csv

        # directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
        # output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
        # output_file = 'country_extended_example_index_seq.csv'
        # query_nodes_info_to_csv(query_nodes_info(
        #    directory), output_dir, output_file)
        ############################
        ############################
        #### skew example ##########
        # profiling_skew_example()
        # print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified')[2465])
        # skew to csv
        # directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
        # output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example'
        # output_file = 'skew_example.csv'

        # query_info = query_nodes_info(directory)
        # print(query_info)
        # query_nodes_info_to_csv(query_info, output_dir, output_file)

        # Ensure the output directory exists
        # if not os.path.exists(output_dir):
        #    os.makedirs(output_dir)
        # query_info = query_nodes_info(directory)
        # print(query_info)
        # query_nodes_info_to_csv(query_info, output_dir, output_file)
        # Calculate qerrors
        # qerrors = calc_qerror(query_info)
        # print(min(qerrors.values()))
        # print(max(qerrors.values()))

        ############################
        # profile_custom_tpch_queries()
        ############################
        # custom tpch section to csv
        # directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/custom_queries'
        # output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch'
        # output_file = 'custom_queries.csv'
        # query_info = query_nodes_info(directory)
        # query_nodes_info_to_csv(query_info, output_dir, output_file)


if __name__ == '__main__':
    main()
