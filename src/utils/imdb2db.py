import psycopg2 as pg
import os
import src.utils.db_connector as dc

# Join-Order Benchmark files
csv_dir = '/Users/fridtjofdamm/Documents/job-data/imdb'
job_schema = '/Users/fridtjofdamm/Documents/job-data/imdb/schematext.sql'

def parse_schema_file(schema_path) -> list: 
    table_names = []
    with open(schema_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line.startswith('CREATE TABLE'):
                table_name = line.split()[2]
                table_names.append(table_name)
    print('successfully parsed schema file')
    return table_names

# get tables and columns from schema file
tables = parse_schema_file(job_schema)

# connect to database
conn = dc.get_db_connection('job')
cur = conn.cursor()

def load_schema(file_path, cur, conn):
    with open(file_path, 'r') as f:
        sql_script = f.read()
    
    cur.execute(sql_script)
        
    conn.commit()
    print('successfully loaded schema')

def load_foreign_keys(cur, conn):
    with open('/Users/fridtjofdamm/Documents/join-order-benchmark/fkindexes.sql', 'r') as f:
        sql_script = f.read()
    cur.execute(sql_script)
    conn.commit()
    print('successfully loaded foreign keys')



def load_data(tables, csv_dir, cur, conn) -> None:
    for table in tables:
        csv_file = os.path.join(csv_dir, f'{table}.csv')
        if os.path.exists(csv_file):
            with open(csv_file, 'r') as f:
                cur.copy_expert(f"COPY {table} FROM STDIN WITH (FORMAT csv, ESCAPE '\\')", f)
                conn.commit()
                print(f'loaded {table}')
        else:
            print(f'file {csv_file} not found for table {table}')


def check_data(tables, cur):
    for table in tables:
        cur.execute(f'SELECT COUNT(*) FROM {table}')
        count = cur.fetchone()[0]
        print(f'{table}: {count}')

#load_schema(job_schema, cur, conn)
#load_data(tables, csv_dir, cur, conn)
#check_data(tables, cur)

cur.close()
conn.close()

print(tables)
