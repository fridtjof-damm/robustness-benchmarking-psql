import duckdb
# duckdb connection
# sf 10 
cursor = duckdb.connect('tpch.duckdb')
# sf 100
#cursor = duckdb.connect('tpch_sf_100.duckdb')
# cnt tables on db instance
tables = cursor.execute('SELECT * FROM (SHOW TABLES);').fetchall()
# table cardinalities for SF = 1 from https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf#page=13 
tpch_tables_rows = [('customer', 150000), ('lineitem', 6000000),('nation', 25), ('orders', 1500000),
                    ('part', 200000), ('partsupp', 800000), ('region', 5), ('supplier', 10000)]

sfs: list[str] = []
for table in tpch_tables_rows:
    table_rows_n = cursor.execute('SELECT COUNT(*) FROM '+table[0]+';').fetchall()
    # if 'nation' and 'region' not in table:  
    sfs.append(table_rows_n[0][0] // table[1])
print('############################################################') 
print('############################################################')    
print('######### THIS DB INSTANCE HAS A SCALEFACCTOR OF: '+str(max(sfs))+' #######')
print('############################################################')
print('############################################################')   






    


