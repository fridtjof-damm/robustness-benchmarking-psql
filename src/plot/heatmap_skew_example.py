import os
from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
from scipy.stats import mstats
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from src.analysis.qanalyze import query_nodes_info

# Example usage
directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
query_info = query_nodes_info(directory)

# Extract a, b, and execution time values from query_info
data = []
for query_id, (node_types, filters, execution_times, cardinalities) in query_info.items():
    for filter_list in filters:
        filter_dict = {k: int(v) for k, v in filter_list}
        if 'a' in filter_dict and 'b' in filter_dict:
            a_value = filter_dict['a']
            b_value = filter_dict['b']
            execution_time = sum(execution_times) / len(execution_times)  # Average execution time
            data.append((a_value, b_value, execution_time))

# Find the minimum and maximum execution times and their corresponding a, b values
min_execution_time = float('inf')
max_execution_time = float('-inf')
min_a, min_b = None, None
max_a, max_b = None, None

for a_value, b_value, execution_time in data:
    if execution_time < min_execution_time:
        min_execution_time = execution_time
        min_a, min_b = a_value, b_value
    if execution_time > max_execution_time:
        max_execution_time = execution_time
        max_a, max_b = a_value, b_value

print(f"Minimum execution time: {min_execution_time} at a={min_a}, b={min_b}")
print(f"Maximum execution time: {max_execution_time} at a={max_a}, b={max_b}")

# Get unique values for a and b and sort them numerically
a_values = sorted(set(item[0] for item in data))
b_values = sorted(set(item[1] for item in data))

# Create a grid for the heatmap
heatmap_data = np.zeros((len(b_values), len(a_values)))

# Calculate the 68th percentile execution time
execution_time_values = [item[2] for item in data]
percentile_68_execution_time = np.percentile(execution_time_values, 68)

# Populate the grid with execution time values, clipping at the 68th percentile
for a_value, b_value, execution_time in data:
    a_index = a_values.index(a_value)
    b_index = b_values.index(b_value)
    heatmap_data[b_index, a_index] = min(execution_time, percentile_68_execution_time)  # Clip execution time values

# Define the green-to-red color map
cmap = LinearSegmentedColormap.from_list('GreenRed', ['green', 'yellow', 'red'])

# Plot the heatmap with linear normalization
fig, ax = plt.subplots(figsize=(10, 8))
norm = mpl.colors.Normalize(vmin=np.min(heatmap_data[heatmap_data > 0]), vmax=percentile_68_execution_time)
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
cbar.set_label('Execution Time')


plt.tight_layout()

# Customize grid
plt.grid(True, which='major', linestyle='-', alpha=0.3)
plt.grid(True, which='minor', linestyle='-', alpha=0.1)

# Save the plot
# Ensure the output directory exists
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/pdf'
os.makedirs(output_dir, exist_ok=True)

# Save the plot as a PDF
output_path = os.path.join(output_dir, 'execution_time_heatmap_68_clip.pdf')
plt.savefig(output_path, format='pdf', bbox_inches='tight')


plt.close()

