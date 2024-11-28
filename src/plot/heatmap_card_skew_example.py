import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize, LinearSegmentedColormap
import os
from src.analysis.qanalyze import query_nodes_info, calc_qerror

directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
query_info = query_nodes_info(directory)

# Extract filters and cardinalities from query_info
data = []
for query_id, info in query_info.items():
    for node_type, filters, execution_time, cardinality in zip(info[0], info[1], info[2], info[3]):
        filter_dict = {k: int(v) for k, v in filters}
        a_value = filter_dict.get('a', 0)
        b_value = filter_dict.get('b', 0)
        data.append((a_value, b_value, cardinality[1]))  # Use the actual rows from cardinality

# Find the minimum and maximum cardinalities and their corresponding a, b values
min_cardinality = float('inf')
max_cardinality = float('-inf')
min_a, min_b = None, None
max_a, max_b = None, None

for a_value, b_value, cardinality in data:
    if cardinality < min_cardinality:
        min_cardinality = cardinality
        min_a, min_b = a_value, b_value
    if cardinality > max_cardinality:
        max_cardinality = cardinality
        max_a, max_b = a_value, b_value

print(f"Minimum cardinality: {min_cardinality} at a={min_a}, b={min_b}")
print(f"Maximum cardinality: {max_cardinality} at a={max_a}, b={max_b}")

# Get unique values for a and b and sort them numerically
a_values = sorted(set(item[0] for item in data))
b_values = sorted(set(item[1] for item in data))

# Create a grid for the heatmap
heatmap_data = np.zeros((len(b_values), len(a_values)))

# Calculate the 95th percentile cardinality
cardinality_values = [item[2] for item in data]
percentile_95_cardinality = np.percentile(cardinality_values, 95)

# Populate the grid with cardinality values, clipping at the 95th percentile
for a_value, b_value, cardinality in data:
    a_index = a_values.index(a_value)
    b_index = b_values.index(b_value)
    heatmap_data[b_index, a_index] = min(cardinality, percentile_95_cardinality)  # Clip cardinality values

# Define the green-to-red color map
cmap = LinearSegmentedColormap.from_list('GreenRed', ['green', 'yellow', 'red'])

# Plot the heatmap with linear normalization
fig, ax = plt.subplots(figsize=(10, 8))
norm = Normalize(vmin=np.min(heatmap_data[heatmap_data > 0]), vmax=percentile_95_cardinality)
cax = ax.matshow(heatmap_data, cmap=cmap, norm=norm, aspect='auto')

# Invert the y-axis to have the lowest values at the bottom
ax.invert_yaxis()

# Set axis labels
ax.set_xticks(np.arange(len(a_values)))
ax.set_xticklabels(a_values, rotation=0)  # Set rotation to 0 for horizontal alignment
ax.xaxis.set_ticks_position('bottom')  # Move x-axis labels to the bottom

# Set y-axis labels to show only 10 values over the range, divisible by 100
y_step = max(1, len(b_values) // 10)
y_ticks = np.arange(0, len(b_values), y_step)
y_labels = [b_values[i] for i in y_ticks]
y_labels = [label if label % 100 == 0 else '' for label in y_labels]  # Ensure labels are divisible by 100
ax.set_yticks(y_ticks)
ax.set_yticklabels(y_labels)

ax.set_xlabel('a')
ax.set_ylabel('b')

# Add color bar
cbar = fig.colorbar(cax)

plt.tight_layout()

# Customize grid
plt.grid(True, which='major', linestyle='-', alpha=0.3)
plt.grid(True, which='minor', linestyle='-', alpha=0.1)

output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/pdf'
os.makedirs(output_dir, exist_ok=True)

# Save the plot as a PDF
output_path = os.path.join(output_dir, 'skew_res_cardinalities_heatmap_95_clip.pdf')
plt.savefig(output_path, format='pdf', bbox_inches='tight')

# Show the plot
plt.show()
plt.close()

