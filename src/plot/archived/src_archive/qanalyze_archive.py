

##############
##############
# archive
##############
##############

# 0 for EXPLAIN (FORMAT JSON), 1 for EXPLAIN (ANALYZE, FORMAT JSON)


"""def job_profiling(prefix: int, process_func, output_dir: str) -> None:
    cur = db_cursor('job')
    prefixes = ['EXPLAIN (FORMAT JSON) ', 'EXPLAIN (FORMAT JSON, ANALYZE) ']
    job_dir = 'resources/queries_job/'
    job_queries = fetch_queries(job_dir)
    print(job_queries[0])
    plans = process_queries(
        job_queries, cur, prefixes[prefix], job_dir, process_func)
    write_plans_to_file(plans, output_dir)"""


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
