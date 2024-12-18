import pandas as pd
import matplotlib.pyplot as plt
import ast
import numpy as np
import re

# Load the CSV data
file_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/all_job.csv'
df = pd.read_csv(file_path)

# Convert the Query ID column to numeric values
df['Query ID'] = df['Query ID'].apply(
    lambda x: int(re.search(r'\d+', x).group()))

# Parse the Cardinalities and calculate cumulative actual cardinalities


def parse_cardinality(cardinality_str):
    try:
        # Convert the string representation of tuples to a list of tuples
        tuples = ast.literal_eval(f"[{cardinality_str}]")
        # Extract the actual cardinalities
        actual_cardinalities = [act for est, act in tuples]
        return actual_cardinalities
    except:
        return []


df['actual_cardinalities'] = df['Cardinality e/a'].apply(parse_cardinality)

# Split the combined node types into individual types
df['Node Types'] = df['Node Types'].apply(lambda x: x.split(', '))

# Extract country names from the Filters column


def extract_country_name(filters_str):
    match = re.search(r'\(country,(\[.*?\])\)', filters_str)
    return match.group(1) if match else '<Country>'


df['Country'] = df['Filters'].apply(extract_country_name)

# Create a DataFrame where each row represents a query and each column represents a node type
node_types = set([node for sublist in df['Node Types'] for node in sublist])
query_ids = df['Query ID'].unique()
data = {node_type: [0] * len(query_ids) for node_type in node_types}

# Populate the data dictionary with the cumulative cardinalities for each node type per query
for i, row in df.iterrows():
    for node_type, cardinality in zip(row['Node Types'], row['actual_cardinalities']):
        data[node_type][row['Query ID'] - 1] += cardinality

# Convert the data dictionary to a DataFrame
data_df = pd.DataFrame(data, index=query_ids)

# Plot the stacked bar chart
ax = data_df.plot(kind='bar', stacked=True, figsize=(12, 8))

# Customize the plot
plt.xlabel('Query ID')
plt.ylabel('Cumulative Cardinalities')
plt.title('Cumulative Cardinalities of Node Types Across Queries')
plt.legend(title='Node Types', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

# Show the plot
plt.show()
