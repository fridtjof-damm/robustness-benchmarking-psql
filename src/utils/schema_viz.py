import psycopg2
from src.utils.db_connector import get_db_connection


def get_schema_details():
    conn = get_db_connection('countries')
    cursor = conn.cursor()

    # Query to get tables and their columns with data types
    query = """
    SELECT 
        t.table_name,
        c.column_name,
        c.data_type,
        c.is_nullable
    FROM 
        information_schema.tables t
        JOIN information_schema.columns c 
            ON t.table_name = c.table_name
    WHERE 
        t.table_schema = 'public'
        AND t.table_type = 'BASE TABLE'
    ORDER BY 
        t.table_name,
        c.ordinal_position;
    """

    cursor.execute(query)
    results = cursor.fetchall()

    # Group by table
    schema_dict = {}
    for table_name, column_name, data_type, is_nullable in results:
        if table_name not in schema_dict:
            schema_dict[table_name] = []
        schema_dict[table_name].append({
            'column': column_name,
            'type': data_type,
            'nullable': is_nullable
        })

    # Print results
    for table, columns in schema_dict.items():
        print(f"\n## Table: {table}")
        for col in columns:
            print(f"{col['column']} | {col['type'] }")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    get_schema_details()
