import os
import json
import psycopg2 as pg
import src.utils.db_connector as db_conn
import matplotlib.pyplot as plt
#from src.analysis.qanalyze import simplify

# prepare queries
def prepare_queries1() -> list[str]:
    queries = []
    query_ids = [1, 2, 3, 4, 6, 7]
    with open('resources/queries_fd/job_1.sql', mode='r', encoding='UTF-8') as file:
        template = file.read()
        for i in query_ids:
            sql = template.format(KIND = i)
            queries.append((i,sql))
    return queries

def prepare_queries2() -> list[str]:
    queries = []
    query_ids = [i for i in range(2,2525746)]
    with open('resources/queries_fd/job_2.sql', mode='r', encoding='UTF-8') as file:
        template = file.read()
        for i in query_ids:
            sql = template.format(KIND = i)
            queries.append((i,sql))
    return queries

def profile_queries(db) -> None:
    conn = db_conn.get_db_connection(f'db')
    cur = conn.cursor()
    prefix = 'EXPLAIN (FORMAT JSON) '
    plans = []
    queries = prepare_queries1()
    for query in queries:
        cur.execute(prefix + query[1])
        plan = cur.fetchall()
        plan = simplify(plan)
        plans.append((f'own_imdb_1_{query[0]}', plan))
    
    # write plans to file
    dir = f'results/fd/qplans/'
    for plan in plans:
        filename = os.path.join(dir, f'{plan[0]}.json')
        with open(filename, 'w', encoding='UTF-8') as file:
            file.write(json.dumps(plan[1], indent=4))
            file.close()
            print(f'success writing plan {plan[0]} to file')

def get_extendedprice() -> list:
    conn = db_conn.get_db_connection('dummydb')
    cur = conn.cursor()
    cur.execute('SELECT (l_quantity * p_retailprice) as extended_price FROM part as p, lineitem as l WHERE p.p_partkey = l.l_partkey;')
    result = cur.fetchall()
    return result


def get_values(attr: str, rel: str) -> list:
    # relation lookup ?
    conn = db_conn.get_db_connection('dummydb')
    cur = conn.cursor()
    cur.execute(f'SELECT {attr} FROM {rel};')
    result = cur.fetchall()
    return result

#print(get_values('o_orderkey', 'orders'))

def analyze_production_years() -> list:
    conn = db_conn.get_db_connection('job')
    cur = conn.cursor()
    cur.execute('SELECT production_year, COUNT(*) FROM aka_title GROUP BY production_year;')
    result = cur.fetchall()
    result = [(x,y) for x, y in result if x is not None and y is not None]
    # sort by films produced
    result = sorted(result, key=lambda x: (x[1], x[0]), reverse=True)
    return result

# TODO: instead plot realtive orders of magnitude on y-axis (count movies in year / count total movies)
def viz_production_years(result: list) -> None:
    years, counts = zip(*result)
    orders_of_magnitude = [int(count / 10000) for count in counts]
    plt.figure(figsize=(12, 6))
    plt.bar(years, orders_of_magnitude)
    plt.xlabel('Production Year')
    plt.ylabel('Orders of Magnitude')
    plt.title('Distribution of Movies by Production Year')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

viz_production_years(analyze_production_years())

