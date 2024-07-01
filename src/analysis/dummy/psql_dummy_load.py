import psycopg2 as pg 
import dummy_data_gen as dd 
# define db parameters
db_params = {
    'database': "dummydb",
    'user':'fridtjofdamm',
    'password':'',
    'host':'localhost',
    'port':'5432'
}

# connection
conn = pg.connect(**db_params)
# cursor
cur = conn.cursor()
#cur.execute('DROP TABLE IF EXISTS numbers;')
""" cur.execute('''CREATE TABLE IF NOT EXISTS numbers(
            number integer
            ) ;''') """
#cur.execute("CREATE INDEX IF NOT EXISTS idx_nmb ON numbers (number);")
#cur.execute("DELETE FROM numbers WHERE number = 16;")
#conn.commit()
# get dummmy data and insert into table
#dataset = dd.dummygen2tuples()
###
# here comes the data insert 
#insert_stmt = 'INSERT INTO numbers (number) VALUES %s;'
###
#execute_values(cur, insert_stmt, dataset)
#conn.commit()
# check for correct insertion
#cur.execute("SELECT COUNT(*) as cnt, number FROM numbers GROUP BY number;")
cur.execute("SELECT COUNT(*) as cnt FROM numbers WHERE number = 17;")
res = cur.fetchall()
print(res)

cur.close()
conn.close()
