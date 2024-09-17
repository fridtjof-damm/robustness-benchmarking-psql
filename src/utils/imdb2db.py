import gzip
import psycopg2 as pg
import os
import src.utils.db_connector as dc

# Join-Order Benchmark files
gz_dir = 'Users/fridtjofdamm/Documents/job-data'
job_schema = '/Users/fridtjofdamm/Documents/join-order-benchmark/schema.sql'

def parse_schema_file(schema_path): 
    tables = {}
    with open(schema_path, 'r') as f:
        for line in f:
            if line.strip().startswith('CREATE TABLE'):
                table_name = line.split()[2]
                tables[table_name] = []
            elif table_name and line.strip().startswith(')'):
                table_name = None
            elif table_name and ',' in line:
                column = line.strip().split()[0]
                tables[table_name].append(column)
    len(tables) == 21
    print('successfully parsed schema file')
    return tables

# get tables and columns from schema file
tables = parse_schema_file(job_schema)

# establish connection to database
conn = dc.get_db_connection('job')
cur = conn.cursor()
results = []
table_names = tables.keys()

for table in table_names:
    cur.execute(f'SELECT * FROM {table};')
    res = cur.fetchall()
    results.append(res)
cur.close()
conn.close()
print(results)
