import pandas as pd
import matplotlib.pyplot as plt
import ast
import numpy as np
import re

# Load the CSV data
file_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified/country_extended_example_update.csv'
df = pd.read_csv(file_path)

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
    match = re.search(r'\(name,([^)]+)\)', filters_str)
    return match.group(1) if match else '<Country>'


df['Country'] = df['Filters'].apply(extract_country_name)

# Create a DataFrame where each row represents a query and each column represents a node type
node_types = set([node for sublist in df['Node Types'] for node in sublist])
query_ids = df['Query ID'].unique()
data = {node_type: [0] * len(query_ids) for node_type in node_types}

# Populate the DataFrame with the cumulative cardinalities for each node type within each query
for i, row in df.iterrows():
    for node_type, cardinality in zip(row['Node Types'], row['actual_cardinalities']):
        data[node_type][row['Query ID'] - 1] += cardinality

stacked_df = pd.DataFrame(data, index=query_ids)

# Add a small constant to avoid log(0) issues
stacked_df += 1e-6

# Select 10 bins including query IDs 1 and 236
selected_queries = [1, 236]
step = max(1, (len(stacked_df) - 2) // 8)
selected_queries += list(range(1, len(stacked_df) + 1, step))
selected_queries = sorted(set(selected_queries))[:10]

# Ensure query IDs 1 and 236 are included
if 1 not in selected_queries:
    selected_queries[0] = 1
if 236 not in selected_queries:
    selected_queries[-1] = 236

# Print selected queries and DataFrame index for debugging
print("Selected Queries:", selected_queries)
print("DataFrame Index:", stacked_df.index)

# Print country names for the selected queries
print("Country Names for Selected Queries:")
for query_id in selected_queries:
    print(
        f"Query ID {query_id}: {df.loc[df['Query ID'] == query_id, 'Country'].values[0]}")

# Filter the DataFrame to include only the selected queries
stacked_df = stacked_df.loc[selected_queries]

# Plot the stacked bar chart
ax = stacked_df.plot(kind='bar', stacked=True, figsize=(10, 6))

# Set log scale on the y-axis
ax.set_yscale('log')

# Set y-axis limits to start at 10^0
ax.set_ylim(1, stacked_df.max().max() * 10)

# Set x-axis labels to country names and make them horizontal
ax.set_xticks(range(len(stacked_df)))
ax.set_xticklabels(df.loc[stacked_df.index, 'Country'], rotation=0)

plt.xlabel('Country')
plt.ylabel('Cumulative Actual Cardinality (Log Scale)')
plt.title('Stacked Bar Chart of Cumulative Cardinalities by Node Types per Query')
plt.legend(title='Node Types', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
