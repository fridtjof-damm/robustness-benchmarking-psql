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
            qplan['Filter'] = simplify_filter(qplan['Filter'], benchmark)
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
    if benchmark == 'tpch' or benchmark == 'job':
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
    elif benchmark == 'countries':
        pass
    else:
        print(f"Invalid benchmark: {benchmark}")

        # Remove unnecessary escape characters from the transformed condition
        condition = condition.replace("\\'", "'")
        condition = clean_condition(condition)

        transformed_conditions.append(condition)

        # Recombine the transformed conditions with commas
        filter_str = ','.join(transformed_conditions)

    return filter_str

###############################################################
###### here begins the standard tpch  section ##########
###############################################################


def tpch_profiling(query_id, write_to_file=False):
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
            write_qp_to_file(query_id, i, qplan, 'tpch', simplified=False)
        for i, qplan in enumerate(simplified_plans):
            write_qp_to_file(query_id, i, qplan, 'tpch', simplified=True)
    print(f"execution stage done")
    return simplified_plans

# persist query plans to files


def job_profiling(query_id, write_to_file=False):
    print(f"job query {query_id}:")

    # Load the query template from the file
    template_path = f'/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/resources/job_parameterized/selected/{query_id}.sql'
    with open(template_path, 'r') as file:
        query_template = file.read()
        file.close()
    queries, _ = qg.generate_job_query(query_template, query_id)

    total_queries = len(queries)

    conn = dc.get_db_connection('job')
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
        plan[0][0][0]['Plan'] = simplify(plan[0][0][0]['Plan'], 'job')
        simplified_plans.append(plan)
        percentage = ((i + 1) / total_queries) * 100
        print(f"{percentage:.2f}% of queries executed")

    # persist plans to file if intended
    if write_to_file:
        for i, qplan in enumerate(plans):
            write_qp_to_file(query_id, i, qplan, 'job', simplified=False)
        for i, qplan in enumerate(simplified_plans):
            write_qp_to_file(query_id, i, qplan, 'job', simplified=True)
    print(f"execution stage done")
    return simplified_plans

# persist query plans to files


def write_qp_to_file(query_id, plan_index, plan_data, benchmark, simplified=False):
    # defining directory path
    if benchmark == 'job':
        dir = f'results/{benchmark}/qplans/{query_id}'
    else:
        dir = f'results/{benchmark}/qplans/q{query_id}'
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


def profile_parameterized_queries(query_id, benchmark):
    # Profile the parameterized queries
    if benchmark == 'tpch':
        simplified_plans = tpch_profiling(query_id, write_to_file=True)
        # Directory where the plans are saved
        # Get the correct query parameters for the given query_id
        query_parameters = set(
            qg.tpch_query_parameters.get(f'q{query_id}', []))
    elif benchmark == 'job':
        simplified_plans = job_profiling(
            query_id, write_to_file=True)
        # Directory where the plans are saved
        # Get the correct query parameters for the given query_id
        query_parameters = set(qg.job_query_parameters.get(f'q{query_id}', []))
    elif benchmark == 'countries':
        # TODO implement profiling for countries example
        pass
    else:
        print(f"Invalid benchmark: {benchmark}")
        return

    # Debug print to check query parameters
    # print(f"Query parameters for q{query_id}: {query_parameters}")
    # Get query nodes info
    if benchmark == 'job':
        directory = f'results/{benchmark}/qplans/{query_id}'
        output_file = f'{query_id}.csv'
    else:
        directory = f'results/{benchmark}/qplans/q{query_id}'
        output_file = f'q{query_id}.csv'

    query_info = query_nodes_info(directory, query_parameters)
    if not query_info:
        # print(f"failed to extract nodes info of query {query_id}")
        return

    # Write query nodes info to CSV
    output_dir = f'/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/{benchmark}/csvs'

    os.makedirs(output_dir, exist_ok=True)
    query_nodes_info_to_csv(query_info, output_dir, output_file)


####################################################
#### query plans directories to query info dict s###
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
        else:
            # Debug print to check the structure of the plan when 'Node Type' is missing
            print(f"Plan without 'Node Type': {plan}")

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
                    if len(match) == 3 and condition not in appended_parameters:
                        node_filters.append(condition)
                        # Mark parameter as appended
                        appended_parameters.add(condition)
        if node_filters:
            filters.append(node_filters)
            # Debug print to check appended filters
            print(f"Appended filters: {node_filters}")
        else:
            # Debug print to check when no filters are appended
            if 'Node Type' in plan:
                print(f"No filters appended for plan: {plan['Node Type']}")
            else:
                print(f"No filters appended for plan without 'Node Type'")

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


def query_nodes_info(directory: str, query_parameters: set) -> Dict[Union[int, Tuple[int, str]], Tuple[List[str], List[List[Tuple[str, str]]], List[float], List[Tuple[int, int]]]]:
    query_info = {}
    os.makedirs(directory, exist_ok=True)
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


def main():
    ############################
    ############################
    ## standard tpch section ###
    # query_ids = [2, 3, 5, 7, 8, 12, 13, 14, 17]
    query_ids = [3, 5, 7, 8, 12, 13, 14, 17]
    # query_ids = [2]
    for i in query_ids:
        profile_parameterized_queries(i, 'tpch')
        print(f'finished profiling for query {i}')


"""    # job se
    query_ids = ['12c', '14b', '15a']
    for i in query_ids:
        profile_parameterized_queries(i, 'job')
        print(f'finished profiling for query {i}')"""

############################
## countries #########
# profiling_country_example()
# print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[1])
# print(query_nodes_info('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified')[236])
# country to csv

# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
# output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
# output_file = 'country_extended_example_index_seq.csv'
# query_nodes_info_to_csv(query_nodes_info(
#    directory), output_dir, output_file)


if __name__ == '__main__':
    main()
