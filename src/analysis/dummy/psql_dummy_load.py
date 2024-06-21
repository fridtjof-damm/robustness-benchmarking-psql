import psycopg2 as pg 
from psycopg2.extras import execute_values
import dummy_data_gen as dd 
# connection
conn = pg.connect(
        database="dummydb",
        user='fridtjofdamm',
        password='',
        host='localhost',
        port='5432'
)   
# cursor
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS numbers(
            number integer
            ) ;''')
# get dummmy data and insert into
dataset = dd.dummygen()

for d, i in enumerate(dataset):
    print(i)
    #cur.executemany("INSERT INTO numbers (number) VALUES (%s);", (i[1],))

#cur.execute("SELECT COUNT(*) FROM numbers;")
#print(cur)
cur.close()
conn.close()
