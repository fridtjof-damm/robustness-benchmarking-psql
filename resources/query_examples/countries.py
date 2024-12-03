import psycopg2 as pg 
import pandas as pd
import numpy as np
import random
import math
from faker import Faker
from src.utils.db_connector import get_db_connection

# init faker
fake = Faker()

conn = get_db_connection('countries')
cur = conn.cursor()

cur.execute('''
    SELECT country, count(country) FROM users GROUP BY country ORDER BY count(country) DESC;
''')


cur.execute("SELECT * FROM users;")
current_data = cur.fetchall()


cur.execute('''
    CREATE TABLE users_extended (
        id SERIAL PRIMARY KEY,
        country VARCHAR(100),
        capital VARCHAR(100),
        president VARCHAR(100),
        population INTEGER,
        area_km2 INTEGER,
        official_language VARCHAR(100),
        currency VARCHAR(50),
        random_text VARCHAR(100)
    );
''')
print('table created')
print('creating data...')

batch_data = [(user_id, 
        country, 
        fake.city(),
        fake.name(),
        random.randint(1000, 1000000),
        random.randint(1000, 1000000),
        fake.language_name(),
        fake.currency_code(),
        fake.text(max_nb_chars=100)) for user_id, country in current_data]

print('data created')

print('starting to insert data...')

cur.executemany('''
    INSERT INTO users_extended (
        id, country, capital, president, population, area_km2, official_language, currency, random_text)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    ''', batch_data)
  
conn.commit()
cur.close()
conn.close()
print("Done!")