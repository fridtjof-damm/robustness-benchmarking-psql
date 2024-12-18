import pandas as pd
import matplotlib.pyplot as plt
import ast
import re
import numpy as np
import psycopg2 as pg
from src.utils import db_connector

# Load the CSV data
file_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified/country_extended_example_index_seq.csv'
df = pd.read_csv(file_path)

# Parse the Cardinalities and calculate cumulative actual cardinalities


def get_country_names() -> dict:
    conn = db_connector.get_db_connection('countries')
    cur = conn.cursor()
    cur.execute('SELECT * FROM nation')
    ids_nations = cur.fetchall()
    cur.close()
    conn.close()
    return {str(id): name for id, name in ids_nations}


def parse_cardinality(cardinality_str):
    try:
        # Convert the string representation of tuples to a list of tuples
        tuples = ast.literal_eval(f"[{cardinality_str}]")
        # Extract the actual cardinalities
        actual_cardinalities = [act for est, act in tuples]
        # Return the cumulative actual cardinality
        return sum(actual_cardinalities)
    except:
        return 0


df['cumulative_actual_cardinality'] = df['Cardinality e/a'].apply(
    parse_cardinality)

# Debug print statement to inspect the Node Types column
print("Node Types column:")
print(df['Node Types'].unique())

# Create the plot
plt.figure(figsize=(10, 6))

# Scatter plot for data points with color based on node type
colors = df['Node Types'].apply(
    lambda x: 'green' if 'Seq Scan' in x else 'blue')
scatter = plt.scatter(df['cumulative_actual_cardinality'],
                      df['Execution Time'], color=colors, alpha=0.6)
plt.xscale('log')
plt.xlabel('Cumulative Actual Cardinality (log scale)')
plt.ylabel('Execution Time (ms)')
plt.title('Execution Time vs. Cumulative Actual Cardinality')
plt.grid(True)

# Get the country names dictionary
country_names = get_country_names()

# Annotate the points


def annotate_point(index, df, ax, country_names, color='black', xytext=(0, 15)):
    row = df.iloc[index]
    # Extract the country ID from the Filters column using regex
    match = re.search(r'\(country,(\d+)\)', row['Filters'])
    country_id = match.group(1) if match else '<Country>'
    country_name = country_names.get(country_id, '<Country>')
    annotation = (f"{row['Node Types']}\n{country_name}\n"
                  f"Cumulated Card: {row['cumulative_actual_cardinality']}\n"
                  f"Exec Time: {row['Execution Time']:.2f} s")
    ax.annotate(annotation, (row['cumulative_actual_cardinality'], row['Execution Time']),
                textcoords="offset points", xytext=xytext, ha='center', fontsize=8, color=color, bbox=dict(facecolor='white', alpha=0.5))
    print(
        f"Annotated point: {annotation} at ({row['cumulative_actual_cardinality']}, {row['Execution Time']})")
    # Highlight the annotated point with its original color
    plt.scatter(row['cumulative_actual_cardinality'],
                row['Execution Time'], s=100, edgecolor='black', color=scatter.get_facecolors()[index], zorder=5)


# Annotate the lowest point
lowest_index = df.nsmallest(1, 'cumulative_actual_cardinality').index
for index in lowest_index:
    print(
        f"Lowest point index: {index}, Cumulative Actual Cardinality: {df.loc[index, 'cumulative_actual_cardinality']}")
    annotate_point(index, df, plt.gca(), country_names, xytext=(0, 15))

# Annotate the second last point
second_last_index = df.nlargest(2, 'cumulative_actual_cardinality').index[-1]
print(
    f"Second last point index: {second_last_index}, Cumulative Actual Cardinality: {df.loc[second_last_index, 'cumulative_actual_cardinality']}")
annotate_point(second_last_index, df, plt.gca(), country_names, xytext=(0, 30))

# Annotate the lowest point with a Seq Scan
lowest_seq_scan_index = df[df['Node Types'].str.contains(
    'Seq Scan')]['cumulative_actual_cardinality'].idxmin()
print(
    f"Lowest Seq Scan point index: {lowest_seq_scan_index}, Cumulative Actual Cardinality: {df.loc[lowest_seq_scan_index, 'cumulative_actual_cardinality']}")
annotate_point(lowest_seq_scan_index, df, plt.gca(),
               country_names, xytext=(0, 45))

# Print the top 5 points with the highest cardinalities where Index Only Scan applies
top_5_index_scan = df[df['Node Types'].str.contains(
    'Index Only Scan')].nlargest(5, 'cumulative_actual_cardinality')
print("Top 5 Index Only Scan points with the highest cardinalities:")
print(top_5_index_scan[['cumulative_actual_cardinality',
      'Execution Time', 'Node Types']])

# Annotate the highest point with an Index Only Scan within the range 10^5 and 10^6
index_scan_within_range = df[(df['Node Types'].str.contains('Index Only Scan')) &
                             (df['cumulative_actual_cardinality'] > 10**5) &
                             (df['cumulative_actual_cardinality'] < 10**6)]
print("Index Only Scan points within range:")
print(index_scan_within_range[[
      'cumulative_actual_cardinality', 'Execution Time', 'Node Types']])

if not index_scan_within_range.empty:
    highest_index_scan_index = index_scan_within_range['cumulative_actual_cardinality'].idxmax(
    )
    print(
        f"Highest Index Only Scan point within range index: {highest_index_scan_index}, Cumulative Actual Cardinality: {df.loc[highest_index_scan_index, 'cumulative_actual_cardinality']}")
    annotate_point(highest_index_scan_index, df, plt.gca(),
                   country_names, xytext=(-30, 0))  # Shifted label to the left
else:
    print("No Index Only Scan points found within the specified range.")

plt.tight_layout()
plt.show()
