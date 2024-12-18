import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# Read the CSV data
# df_raw = pd.read_csv('/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified/country_extended_example_update.csv')
df_raw = pd.read_csv(
    '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified/country_extended_example_index_seq.csv')
# Function to calculate q-error


def calc_qerror(estimated, actual):
    if actual == 0 or estimated == 0:
        return 0
    return max(estimated/actual, actual/estimated)


# Process the data
data = []
for idx, row in df_raw.iterrows():
    node_types = row['Node Types'].split(', ')
    cardinalities_str = row['Cardinality e/a'].strip('[]').split('), (')
    cardinalities = []
    for card in cardinalities_str:
        card = card.strip('()').split(',')
        cardinalities.append((int(card[0]), int(card[1])))

    # Create separate entries for each node type with its corresponding cardinality
    for node_idx, node_type in enumerate(node_types):
        if node_idx < len(cardinalities):
            est, act = cardinalities[node_idx]
            qerror = calc_qerror(est, act)
            data.append({
                'query_id': idx + 1,
                'node_type': node_type.strip(),
                'qerror': qerror
            })

# Create DataFrame
df = pd.DataFrame(data)

# Create the strip plot with log scale
plt.figure(figsize=(12, 6))
ax = plt.gca()
sns.stripplot(data=df, x='node_type', y='qerror',
              size=3, alpha=0.5, jitter=0.2)
ax.set_yscale('log')

# Customize the plot
plt.xticks(rotation=45, ha='right')
plt.xlabel('Node Type')
plt.ylabel('Q-Error (log scale)')
plt.title('Q-Error Distribution by Node Type')

# Adjust layout to prevent label cutoff
plt.tight_layout()

plt.show()
