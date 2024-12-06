import psycopg2 as pg
from matplotlib import pyplot as plt
from src.utils.db_connector import get_db_connection



def shipdata_stats():
    # Execute the query
    conn = get_db_connection('dummydb')
    cur = conn.cursor()
    cur.execute('''
        SELECT l_shipdate, count(l_shipdate) FROM lineitem GROUP BY l_shipdate ORDER BY l_shipdate;
    ''')

    # Fetch the results
    data = cur.fetchall()

    # Close the connection
    cur.close()
    conn.close()

    # Extract the x and y values
    x = [row[0] for row in data]
    y = [row[1] for row in data]

    # Create a new figure
    # only for the year 1992 and 1998 separately
    #data_1992 = [row for row in data if row[0].year == 1992]
    data_1998 = [row for row in data if row[0].year == 1998]

    #x_1992 = [row[0] for row in data_1992]
    #y_1992 = [row[1] for row in data_1992]

    x_1998 = [row[0] for row in data_1998]
    y_1998 = [row[1] for row in data_1998]

    plt.figure(figsize=(15, 8))
    #plt.plot(x_1992, y_1992, label='1992')
    plt.plot(x_1998, y_1998, label='1998')
    plt.xlabel('l_shipdate')
    plt.ylabel('count(l_shipdate)')
    plt.legend()
    plt.show()


    """# Plot the data
    plt.plot(x, y)
    plt.xlabel('l_shipdate')
    plt.ylabel('count(l_shipdate)')

    # Show the plot
    plt.show()"""

def partkey_stats():
    conn = get_db_connection('dummydb')
    cur = conn.cursor()
    # Execute the query
    cur.execute('''
        SELECT p_partkey, count(p_partkey) FROM part GROUP BY p_partkey ORDER BY p_partkey;
    ''')

    # Fetch the results
    data = cur.fetchall()

    # Close the connection
    cur.close()
    conn.close()


    # Extract the x and y values
    x = [row[0] for row in data]
    y = [row[1] for row in data]

    # Plot the data
    plt.plot(x, y)
    plt.xlabel('p_partkey')
    plt.ylabel('count(p_partkey)')

    # Show the plot
    plt.show()


def commitdate_stats():
    # Execute the query
    conn = get_db_connection('dummydb')
    cur = conn.cursor()
    cur.execute('''
        SELECT l_commitdate, count(l_commitdate) FROM lineitem GROUP BY l_commitdate ORDER BY l_commitdate;
    ''')

    # Fetch the results
    data = cur.fetchall()

    # Close the connection
    cur.close()
    conn.close()

    # Extract the x and y values
    x = [row[0] for row in data]
    y = [row[1] for row in data]

    # Plot the data
    plt.plot(x, y)
    plt.xlabel('l_commitdate')
    plt.ylabel('count(l_commitdate)')

    # Show the plot
    plt.show()

def receiptdate_stats():
    conn = get_db_connection('dummydb')
    cur = conn.cursor()
    # Execute the query
    cur.execute('''
        SELECT l_receiptdate, count(l_receiptdate) FROM lineitem GROUP BY l_receiptdate ORDER BY l_receiptdate;
    ''')

    # Fetch the results
    data = cur.fetchall()

    # Close the connection
    cur.close()

    # Extract the x and y values
    x = [row[0] for row in data]
    y = [row[1] for row in data]

    # Plot the data
    plt.plot(x, y)
    plt.xlabel('l_receiptdate')
    plt.ylabel('count(l_receiptdate)')

    # Show the plot
    plt.show()

shipdata_stats()