import psycopg2 as pg

db_params = {
    'database': 'dummydb',
    'user':'fridtjofdamm',
    'password':'',
    'host':'localhost',
    'port':'5432'
}

def load_tpch_schema(): 
    conn = pg.connect(**db_params)
    conn.autocommit = True
    cur = conn.cursor()
    ## execption handling
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
    try: 
        for table in tables:
            file_path = f'resources/db/postgres/tpch/{table}'
            with open(file_path, encoding='UTF-8', mode='r') as table_file:
                table_name = table[:-4]
                sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, DELIMITER '|', HEADER OFF, ENCODING 'UTF8');"
                cur.copy_expert(sql, table_file)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error loading table data: {e}")
                #### to be continued ####
#load_tbl_data()
