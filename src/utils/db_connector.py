import json
import psycopg2 as pg

def get_db_connection(db_name):
    with open('config.json', 'r') as config_file:
        db_configs = json.load(config_file)
    
    if db_name not in db_configs:
        raise ValueError(f"Database '{db_name}' not found in config file")
    
    db_params = db_configs[db_name]
    return pg.connect(
        database=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
        host=db_params['host'],
        port=db_params['port']
    )