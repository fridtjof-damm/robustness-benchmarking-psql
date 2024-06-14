import duckdb
import dummy_data_gen as ddg
import pandas as pd

cursor = duckdb.connect('resources/db/dummy_db.duckdb')
# clean up before creating table
cursor.sql("DROP TABLE IF EXISTS numbers;")
cursor.sql("CREATE TABLE numbers (number INTEGER);")
# get the dummy data
data = ddg.dummygen()
df = pd.DataFrame(data)
cursor.sql("INSERT INTO numbers (number) SELECT * FROM df;")
#cursor.sql("CREATE INDEX n_idx ON numbers (number);")
