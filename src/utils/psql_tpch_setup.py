import psycopg2 as pg

db_params = {
    'database': "dummydb",
    'user':'fridtjofdamm',
    'password':'',
    'host':'localhost',
    'port':'5432'
}

def load_tpch_schema(): 
    conn = pg.connect(**db_params)
    conn.autocommit = True
    cur = conn.cursor()
    with open('resources/db/postgres/tpch/schema_tpch.sql', encoding='UTF-8', mode='r') as schema_file:
        schema = schema_file.read()
        cur.execute(schema)
    conn.commit()
    conn.close()
#load_tpch_schema()
    
def load_tbl_data():
    conn = pg.connect(**db_params)
    cur = conn.cursor()
    tables = ['customer.tbl','lineitem.tbl','nation.tbl','orders.tbl','part.tbl','partsupp.tbl','region.tbl','supplier.tbl']
    for table in tables:
        file_path = f'resources/db/postgres/tpch/{table}'
        with open(file_path, encoding='UTF-8', mode='r') as table_file:
            table_data = table_file.read()
            table_name = table[:-4]
            sql = "COPY %s FROM %s "
            #### to be continued ####
