import datetime
import psycopg2 as pg
from faker import Faker
from src.utils.db_connector import get_db_connection
from itertools import islice
import random

# init faker
fake = Faker()

conn = get_db_connection('countries')
cur = conn.cursor()

# Get current data
cur.execute("SELECT * FROM users;")
current_data = cur.fetchall()
total_records = len(current_data)
print(f'Total records to process: {total_records}')

# Extract unique countries and assign IDs
unique_countries = list(set([country for _, country in current_data]))
country_ids = {country: idx + 1 for idx,
               country in enumerate(unique_countries)}

cur.execute('DROP TABLE IF EXISTS nation;')

# Create nation table
cur.execute('''
    CREATE TABLE nation (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100)
    );
''')
print('Table created')

# Load country indices into the table
cur.executemany('''
    INSERT INTO nation (id, name)
    VALUES (%s, %s)
''', [(idx, country) for country, idx in country_ids.items()])

# Drop users_extended table if exists
cur.execute('DROP TABLE IF EXISTS users_extended;')

# Create users_extended table with registration_dates column
cur.execute('''
    CREATE TABLE users_extended (
        id SERIAL PRIMARY KEY,
        country INTEGER,
        capital VARCHAR(100),
        president VARCHAR(100),
        population INTEGER,
        area_km2 INTEGER,
        official_language VARCHAR(100),
        currency VARCHAR(50),
        random_text VARCHAR(100),
        registration_date DATE
    );
''')
print('Table created')

# Process in batches of 1000
BATCH_SIZE = 1000
processed = 0


def chunks(data, size):
    it = iter(data)
    return iter(lambda: list(islice(it, size)), [])


# Generate and insert data in batches
for chunk in chunks(current_data, BATCH_SIZE):
    batch_data = [(
        user_id,
        country_ids[country],
        fake.city(),
        fake.name(),
        random.randint(1000, 1000000),
        random.randint(1000, 1000000),
        fake.language_name(),
        fake.currency_code(),
        fake.text(max_nb_chars=100),
        fake.date_between(start_date=datetime.datetime(
            1997, 10, 28), end_date=datetime.datetime(2024, 12, 5))
    ) for user_id, country in chunk]

    cur.executemany('''
        INSERT INTO users_extended (
            id, country, capital, president, population, area_km2, 
            official_language, currency, random_text, registration_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', batch_data)

    processed += len(chunk)
    print(
        f'Processed {processed}/{total_records} records ({(processed/total_records)*100:.2f}%)')
    conn.commit()

cur.close()
conn.close()
print("Done!")
