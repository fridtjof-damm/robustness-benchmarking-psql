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


def simplify(qplan):
    # handling the plan structure
    # check if node exists, then delete

    # add/remove nodes to be removed here
    keys_to_remove = [
        'Planning Time', 'Parallel Aware', 'Async Capable', 'Single Copy', 'Alias',
        'Parent Relationship', 'Relation Name', 'Plan Width', 'Actual Loops',
        'Rows Removed by Index Recheck', 'Exact Heap Blocks', 'Lossy Heap Blocks'
    ]
    # Remove the keys from qplan
    for key in keys_to_remove:
        if key in qplan:
            del qplan[key]

    # EXPLAIN ANALYZE prefix
    if 'Filter' in qplan:
        qplan['Filter'] = re.sub(
            r'\(\(([^)]+?)\)::text\s*=\s*\'([^\']+)\'::text\)', r'\1 = \'\2\'', qplan['Filter'])
        qplan['Filter'] = re.sub(
            r'\(\(([^)]+?)\)::text\s*~~\s*\'([^\']+)\'::text\)', r'\1 LIKE \'\2\'', qplan['Filter'])
        # Remove unnecessary escape characters
        qplan['Filter'] = re.sub(r'\\', '', qplan['Filter'])
        # qplan['Filter'] = re.sub(r'\(\(([^)]+?)\)::text[^)]+\)', r'(\1)', qplan['Filter']) # q2 TIN NICKEL etc.
        # qplan['Filter'] = re.sub(r'\(([^<>=!]+)\s*[<>=!]+\s*[^)]+\)', r'(\1)', qplan['Filter']) # q3 shipdate filter simplification
        # qplan['Filter'] = re.sub(r'\(*([^()]+)\)*(?:%\'::text)?(?:\s*(?:AND|OR)\s*\(*\1\)*(?:%\'::text)?)*', r'\1', qplan['Filter'])
        # qplan['Filter'] = re.sub(r'(?:^|\s*(?:AND|OR)\s*)(?:\(*)([^()]+?)(?:\)*(?:%\'::text)?)(?=\s*(?:AND|OR|$))', r'\1', qplan['Filter'])
        # qplan['Filter'] = ''.join(sorted(set(re.sub(r'[^a-zA-Z]', '', re.sub(r'(?:^|\s*(?:AND|OR)\s*)(?:\(*)([^()]+?)(?:\)*(?:%\'::text)?)(?=\s*(?:AND|OR|$))', r'\1', qplan['Filter']))), key=lambda x: re.sub(r'[^a-zA-Z]', '', qplan['Filter']).index(x)))
        # match = re.search(r'\(([^=]+)\s*=\s*[^)]+\)', qplan['Filter'])
        # if match:
        #    attr = match.group(1).strip()
        #    val = f'({attr})'
        #    qplan['Filter'] = val
    # further selection from index scan queries
    # if '' in qplan:
    if 'Index Cond' in qplan:
        qplan['Index Cond'] = re.sub(
            r'\(\(([^)]+?)\)::text\s*=\s*\'([^\']+)\'::text\)', r'\1 = \'\2\'', qplan['Index Cond'])
        qplan['Index Cond'] = re.sub(
            r'\(\(([^)]+?)\)::text\s*~~\s*\'([^\']+)\'::text\)', r'\1 LIKE \'\2\'', qplan['Index Cond'])
        # Remove unnecessary escape characters
        qplan['Index Cond'] = re.sub(r'\\', '', qplan['Index Cond'])
   # match = re.search(r'\(([^=]+)\s*=\s*[^)]+\)', qplan['Index Cond'])
   #     if match:
   #         attr = match.group(1).strip()
   #         val = f'{attr}'
   #         qplan['Index Cond'] = val

    if 'Join Filter' in qplan:
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
        qplan['Recheck Cond'] = re.sub(r'\\', '', qplan['Recheck Cond'])
    if 'Cache Key' in qplan:
        qplan['Cache Key'] = re.search(r'\.(\w+)$', qplan['Cache Key']).group(
            1) if re.search(r'\.(\w+)$', qplan['Cache Key']) else qplan['Cache Key']

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
    # Remove all remaining opening parentheses
    cleaned = cleaned.replace('(', '')
    # Remove all remaining closing parentheses
    cleaned = cleaned.replace(')', '')
    return cleaned

###############################################################
###### here begins the standard tpch  section ##########
###############################################################


def psql_tpch_profiling(query_id, write_to_file=False):
    conn = dc.get_db_connection('dummydb')
    cur = conn.cursor()
    prefix = 'EXPLAIN (FORMAT JSON, ANALYZE) '
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
        qplan[0][0][0]['Plan'] = simplify(qplan[0][0][0]['Plan'])
        simplified.append(qplan)

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
    filename = os.path.join(dir, f'{query_id}.json')
    with open(filename, mode='w', encoding='UTF-8') as file:
        file.write(json.dumps(plan_data, indent=4))

# categorizing  query plans


def compare_query_plans(query_plans):
    # categories of query plans
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

###############################################################
###### new function to profile parameterized queries ##########
###############################################################


def profile_parameterized_queries(query_id):
    # Profile the parameterized queries
    simplified_plans = psql_tpch_profiling(query_id, write_to_file=True)

    # Directory where the plans are saved
    directory = f'results/tpch/qplans/q{query_id}'

    # Get query nodes info
    query_info = query_nodes_info(directory)

    query_nodes_info_to_csv(query_info, 'results/tpch/', f'q{query_id}.csv')
    # Print query nodes info
    print(query_info)


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

    conn = dc.get_db_connection('job')
    cur = conn.cursor()
    # Generate the job queries
    queries = qg.generate_job_queries()

    # Execute EXPLAIN ANALYZE for each query and save the plan as a JSON file
    for i, query in enumerate(queries, start=1):
        cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
        plan = cur.fetchone()[0][0]  # Get the JSON plan

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


def traverse(plan: Dict, node_types: List[str], filters: List[List[Tuple[str, str]]], execution_times: List[float], cardinalities: List[Tuple[int, int]]) -> None:
    if isinstance(plan, list):
        for item in plan:
            traverse(item, node_types, filters, execution_times, cardinalities)
    elif isinstance(plan, dict):
        if 'Node Type' in plan:
            if plan['Node Type'] not in ['Limit', 'Gather']:
                node_types.append(plan['Node Type'])
        node_filters = []
        for key in ['Filter', 'Recheck Cond', 'Index Cond', 'Seq Scan', 'Index Scan']:
            if key in plan:
                matches = re.findall(
                    r'(\w+)\s*(=|LIKE|<|>|<=|>=)\s*\'?([^\'\)]+)\'?', plan[key])
                for match in matches:
                    if len(match) == 3:
                        node_filters.append((match[0], match[2]))
        if node_filters:
            filters.append(node_filters)
        if 'Execution Time' in plan:
            execution_times.append(plan['Execution Time'])
        if 'Plan Rows' in plan and 'Actual Rows' in plan:
            cardinalities.append((plan['Plan Rows'], plan['Actual Rows']))
        if 'Plans' in plan:
            for subplan in plan['Plans']:
                traverse(subplan, node_types, filters,
                         execution_times, cardinalities)
        if 'Plan' in plan:
            traverse(plan['Plan'], node_types, filters,
                     execution_times, cardinalities)

# Help function to collect the node types


def extract_node_types_from_plan(plan: Dict) -> Tuple[List[str], List[List[Tuple[str, str]]], List[float], List[Tuple[int, int]]]:
    node_types = []
    filters = []
    execution_times = []
    cardinalities = []
    traverse(plan, node_types, filters, execution_times, cardinalities)
    return node_types, filters, execution_times, cardinalities

##
# structure of the query info dict: node_types e.g. Index Scan , filters e.g. (country , India) , execution_time e.g. 0.1 (ms)
##


def query_nodes_info(directory: str) -> Dict[Union[int, Tuple[int, str]], Tuple[List[str], List[List[Tuple[str, str]]], List[float], List[Tuple[int, int]]]]:
    query_info = {}
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            # Try to extract the numeric part and suffix from the filename
            query_id, suffix = extract_number(filename)
            if query_id == float('inf'):
                # Fall back to the previous method for filenames with only numbers
                try:
                    query_id = int(filename.split('.')[0])
                    suffix = ''
                except ValueError:
                    continue  # Skip files that don't match the expected pattern
            with open(os.path.join(directory, filename), 'r', encoding='UTF-8') as file:
                print(f"Processing file: {filename}")
                plan = json.load(file)
                node_types, filters, execution_times, cardinalities = extract_node_types_from_plan(
                    plan)
                query_info[(query_id, suffix) if suffix else query_id] = (
                    node_types, filters, execution_times, cardinalities)
    return query_info

# write the query info dict to csv


def query_nodes_info_to_csv(query_info: Dict[Union[int, Tuple[int, str]], Tuple[List[str], List[List[Tuple[str, str]]], List[float], List[Tuple[int, int]]]], output_dir: str, output_file: str) -> None:
    data = []
    # Sort the keys of the query_info dictionary
    sorted_keys = sorted(query_info.keys(), key=lambda x: (
        x[0], x[1]) if isinstance(x, tuple) else (x, ''))
    for query_id in sorted_keys:
        node_types, filters, execution_times, cardinalities = query_info[query_id]
        combined_node_types = ', '.join(sorted(node_types))
        combined_filters = ', '.join([f'({k},{v})' for filter_list in sorted(
            filters) for k, v in sorted(filter_list)])
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
    print(f"CSV file has been written successfully to {output_path}")


# Example usage
directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example'
output_file = 'skew_example.csv'
query_info = query_nodes_info(directory)
query_nodes_info_to_csv(query_info, output_dir, output_file)

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
    ############################
    ## country example #########
    profiling_country_example()
    # print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[1])
    # print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[236])
    # country to csv
    directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
    output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
    output_file = 'country_extended_example_update.csv'
    query_nodes_info_to_csv(query_nodes_info(
        directory), output_dir, output_file)
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
    ############################
    ## standard tpch section ###
    # query_ids = [2,3,7,8,12,17]
    # query_ids = [i for i in range(1,23)]
    # query_ids = [1]
    # for i in query_ids:
    #   profile_parameterized_queries(i)
    # directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q2'
    # output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch'
    # output_file = 'q2.csv'
    # query_info = query_nodes_info(directory)
    # print(query_info)
    # query_nodes_info_to_csv(query_info, output_dir, output_file)
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
