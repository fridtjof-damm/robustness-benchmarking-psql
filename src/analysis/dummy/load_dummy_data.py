import duckdb
import analysis.dummy.dummy_data_gen as ddg
import json

cursor = duckdb.connect('dummy_db.duckdb')
# already created numbers table 
#cursor.sql("CREATE TABLE numbers (number INTEGER);")
#tables = cursor.sql("SHOW TABLES;").fetchall()
#print(f'SUCCESS CREATING: {tables}')
#print(tables)
#data = ddg.dummygen()
#l = len(data) 
#for i,val in enumerate(data):
#    cursor.sql(f"INSERT INTO numbers (number) VALUES ({val});")
#    if (i+1) // 5000000 != i // 5000000:
#        print(f"INSERTED {i+1} ITEMS out of {len(data)}")
#print('success')
#res = cursor.sql("FROM numbers LIMIT 20;").fetchall()
#print(res)
#profile = cursor.sql("EXPLAIN SELECT COUNT(*) FROM numbers;").fetchall()
#print(profile)
cursor.sql("CREATE INDEX n_idx ON numbers (number);")
