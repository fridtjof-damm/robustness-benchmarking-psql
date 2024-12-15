import os
import pandas as pd
import psycopg2
from io import StringIO
from datetime import datetime
from src.utils.db_connector import get_db_connection

# Database connection
conn = get_db_connection('bench_stats')
cur = conn.cursor()

# Directory containing the CSV files
csv_directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch'

# Check if the directory exists
if not os.path.exists(csv_directory):
    print(f"Error: Directory '{csv_directory}' does not exist.")
    exit(1)

# Create datasets table
datasets = [
    (1, 'countries'),
    (2, 'skewdata'),
    (3, 'tpch'),
    (4, 'job')
]

cur.execute("""
CREATE TABLE IF NOT EXISTS datasets (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
)
""")

# Insert dataset entries
cur.executemany(
    "INSERT INTO datasets (id, name) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING", datasets)


def create_table(table: str, df: pd.DataFrame) -> None:
    # Generate SQL query to create table
    columns = ", ".join([f'"{col}" TEXT' for col in df.columns] +
                        ['"load_date" TIMESTAMP', '"dataset" INTEGER'])
    create_query = f"""
    CREATE TABLE IF NOT EXISTS "{table}" (
        {columns},
        FOREIGN KEY (dataset) REFERENCES datasets(id)
    )
    """
    cur.execute(create_query)


def copy_from_stringio(df: pd.DataFrame, table: str, dataset_id: int) -> None:
    # Add load_date and dataset columns
    df['load_date'] = datetime.now()
    df['dataset'] = dataset_id

    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t')
    buffer.seek(0)

    # Debug print statements
    print(f"Copying data to table {table}")
    # Print only the first 1000 characters of the buffer
    print(buffer.getvalue()[:1000])

    # Copy data into the table
    cur.copy_from(buffer, table, sep='\t', null='')


# Process each CSV file
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_directory, filename)
        table_name = os.path.splitext(filename)[0]

        # Determine dataset_id based on the directory name
        if 'countries' in csv_directory:
            dataset_id = 1
        elif 'skewdata' in csv_directory:
            dataset_id = 2
        elif 'tpch' in csv_directory:
            dataset_id = 3
        elif 'job' in csv_directory:
            dataset_id = 4
        else:
            raise ValueError("Unknown dataset directory")

        # Read CSV file
        df = pd.read_csv(file_path)

        # Create table if not exists
        create_table(table_name, df)

        # Debug print statements
        print(f"DataFrame columns: {df.columns}")
        print(f"DataFrame head:\n{df.head()}")

        # Copy data to the table
        copy_from_stringio(df, table_name, dataset_id)

        print(f"Loaded {filename} into table {table_name}")

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()

print("All files processed successfully.")
