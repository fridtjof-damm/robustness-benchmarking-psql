import json
import psycopg2 as pg
import matplotlib.pyplot as plt

with open('config.json', 'r') as config_file:
    db_params = json.load(config_file)

import json
import psycopg2 as pg
import matplotlib.pyplot as plt

# Load database credentials from config file
with open('config.json', 'r') as config_file:
    db_params = json.load(config_file)

def histogram(table: str, attribute: str) -> list[tuple[int, float]]:
    sql = f"""SELECT histogram_bounds
            FROM pg_stats
            WHERE tablename = '{table}' AND attname = '{attribute}';"""

    min_max_sql = f"SELECT min({attribute}), max({attribute}) FROM {table};"
    count_sql = f"SELECT COUNT(*) FROM {table};"

    # Connect to the database
    conn = pg.connect(**db_params)
    cur = conn.cursor()
    cur.execute(sql)
    histogram_bounds = cur.fetchall()
    cur.execute(min_max_sql)
    min_max = cur.fetchall()
    cur.execute(count_sql)
    total_count = cur.fetchone()
    cur.close()
    conn.close()

    min_val = float(min_max[0][0])
    max_val = float(min_max[0][1])
    histogram_bounds_str = histogram_bounds[0][0]
    histogram_bounds_list = histogram_bounds_str.strip('{}').split(',')
    histogram_bounds = [float(bound) for bound in histogram_bounds_list]

    histogram = []
    histogram.append((0, min_val))
    if isinstance(histogram_bounds, list):
        for i, element in enumerate(histogram_bounds):
            histogram.append((i + 1, element))
    histogram.append((len(histogram), max_val))
    return histogram

def selectivity(histogram) -> list[int]:
    selectivity = []
    for i in range(len(histogram) - 1):
        lower_bound = histogram[i]
        upper_bound = histogram[i + 1]
        conn = pg.connect(**db_params)
        cur = conn.cursor() 
        query = f"SELECT COUNT(*) FROM {table} WHERE {attribute} >= {lower_bound} AND {attribute} < {upper_bound};"
        cur.execute(query)
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        selectivity.append(count / total_count[0])
        print(f"Selectivity for range [{lower_bound}, {upper_bound}): {count}")
    return selectivity

# Example usage
table = 'lineitem'
attribute = 'l_extendedprice'
histogram_data = histogram(table, attribute)

# Plot histogram
x_values = [x for x, _ in histogram_data]
y_values = [y for _, y in histogram_data]

plt.figure(figsize=(10, 6))
plt.bar(x_values, y_values, width=0.25, align='center', alpha=0.5)
plt.xlabel('Bucket')
plt.ylabel('Bounds')
plt.title('Histogram of l_extendedprice')
plt.show()