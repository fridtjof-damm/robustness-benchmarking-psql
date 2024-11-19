# based on https://git.tu-berlin.de/halfpap/heatmap
from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from matplotlib.colors import ListedColormap
import csv
import os
from scipy.ndimage import zoom

# Source file
FILE = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example/skew_example.csv'

# Read the CSV file and extract filters, execution times, and node types
data = []
with open(FILE, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        filters = row['Filters']
        node_type = row['Node Type']
        filter_dict = dict(item.strip('()').split(',') for item in filters.split('),('))
        a_value = int(filter_dict['a'])
        b_value = int(filter_dict['b'])
        data.append((a_value, b_value, node_type))

# Get unique values for a and b and sort them numerically
a_values = sorted(set(item[0] for item in data))
b_values = sorted(set(item[1] for item in data))

# Create a grid for the node type heatmap
node_type_data = np.zeros((len(b_values), len(a_values)), dtype=object)

# Populate the grid with node types
for item in data:
    a_index = a_values.index(item[0])
    b_index = b_values.index(item[1])
    node_type_data[b_index, a_index] = item[2]


# Create a mapping from node types to numerical values
node_type_to_num = {
    'Index Scan': 0,
    'Bitmap Heap Scan': 1
}
num_to_node_type = {v: k for k, v in node_type_to_num.items()}

# Convert the node_type_data array to numerical values
node_type_data_num = np.vectorize(node_type_to_num.get)(node_type_data)

# Downsample the grid by a factor of 5 (reduce the number of data points by 25)
downsample_factor = 1
node_type_data_downsampled_num = zoom(node_type_data_num, (1/downsample_factor, 1/downsample_factor), order=0)

# Normalize the node type data for color mapping
node_type_cmap = ListedColormap(['green', 'yellow', 'red'])

# Plot the node type heatmap
fig, ax = plt.subplots(figsize=(10, 8))
cax = ax.matshow(node_type_data_downsampled_num, cmap=node_type_cmap, aspect='auto')

# Invert the y-axis to have the lowest values at the bottom
ax.invert_yaxis()

# Set axis labels
ax.set_xticks(np.arange(len(a_values) // downsample_factor))
ax.set_xticklabels(np.linspace(min(a_values), max(a_values), len(a_values) // downsample_factor, dtype=int), rotation=90)
ax.xaxis.set_ticks_position('bottom')  # Move x-axis labels to the bottom

# Set y-axis labels to show only 5 numbers over the whole range
y_ticks = np.linspace(0, len(b_values) // downsample_factor - 1, 5, dtype=int)
ax.set_yticks(y_ticks)
ax.set_yticklabels(np.linspace(min(b_values), max(b_values), 5, dtype=int))

ax.set_xlabel('a')
ax.set_ylabel('b')

# Add color bar
cbar = fig.colorbar(cax, ticks=[0, 1, 2])
cbar.ax.set_yticklabels(['Index Scan', 'Bitmap Heap Scan', 'Gather'])



plt.tight_layout()

# Ensure the output directory exists
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots'
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, 'heatmap_nodes_skew_example.png')
plt.savefig(output_path, dpi=600, bbox_inches='tight')
plt.show()