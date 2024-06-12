import duckdb
import dummy_data_gen as ddg
import pandas as pd
#import json

cursor = duckdb.connect('resources/db/dummy_db.duckdb')
# already created numbers table 
cursor.sql("DROP TABLE IF EXISTS numbers;")
cursor.sql("CREATE TABLE numbers (number INTEGER);")
tables = cursor.sql("SHOW TABLES;").fetchall()
#print(f'SUCCESS CREATING: {tables}')
print(tables)
data = ddg.dummygen()
df = pd.DataFrame(data)
cursor.sql("INSERT INTO numbers (number) SELECT * FROM df;")

#print(cursor.sql("SELECT COUNT(*) FROM numbers GROUP BY number;").fetchall())

#cursor.sql("CREATE INDEX n_idx ON numbers (number);")

