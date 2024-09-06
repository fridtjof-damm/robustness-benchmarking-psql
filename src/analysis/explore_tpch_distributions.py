import psycopg2 as pg

# db config
db_params = {
        "database": "dummydb",
        "user": "fridtjofdamm",
        "password": "",
        "host": "localhost",
        "port": "5432"
    }

conn = pg.connect(**db_params)
cur = conn.cursor()
table = 'lineitem'
attribute = 'l_extendedprice'


# get stats min max cardinalities
q_stat = f'''select count(*) as cardinality,
    min({attribute}) as min_val, 
    max({attribute}) as max_val
    from {table};
    '''
cur.execute(q_stat)
cardinality, min_val, max_val = cur.fetchone()



n = 1000
step = (max_val - min_val) / n
values = [min_val + (step * i) for i in range(n + 1)]
# list of tuples: selectivity, val with range from min to induce given selectivity
selectivties = [i / 100 for i in range(0, 101, 1)]
selectivity_values = []
# queries 
queries_unique_vals = []

for val in values:
    query_uni = f'''select count(distinct {attribute})
        from {table}
        where {attribute} between {min_val} and {val};'''
    queries_unique_vals.append(("{:.2f}".format(float(val)),query_uni))

for query in queries_unique_vals:
    queries = []
    queries.append(query[1])
    cur.execute(query[1])
    selectivity_values.append(cur.fetchone()[0])
  #  cur.execute(q_unique_vals)
  #  val_count = cur.fetchone()[0]
  #  selectivity = val_count / cardinality
  #  if selectivity in selectivties:
  #      selectivity_values.append((selectivity, val))
# conn.close()
print(selectivity_values)


