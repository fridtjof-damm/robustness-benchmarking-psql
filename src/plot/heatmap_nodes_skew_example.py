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
        node_type = row['Node Types']
        if node_type:  # Filter out rows where node type is empty
            filter_dict = dict(item.strip('()').split(',') for item in filters.split('),('))
            a_value = int(filter_dict['a'])
            b_value = int(filter_dict['b'])
            data.append((a_value, b_value, node_type))

# Get unique values for a and b and sort them numerically
a_values = sorted(set(item[0] for item in data))
b_values = sorted(set(item[1] for item in data))

# Create a grid for the node type heatmap
node_type_data = np.full((len(b_values), len(a_values)), '', dtype=object)

# Populate the grid with node types
for item in data:
    a_index = a_values.index(item[0])
    b_index = b_values.index(item[1])
    node_type_data[b_index, a_index] = item[2]

# Create a mapping from node types to numerical values
node_type_to_num = {
    'Index Scan': 0,
    'Bitmap Heap Scan, Bitmap Index Scan': 1,
    'Seq Scan': 2
}
num_to_node_type = {v: k for k, v in node_type_to_num.items()}

# Check for any node types not in the mapping
for node_type in np.unique(node_type_data):
    if node_type not in node_type_to_num:
        print(f"Warning: Node type '{node_type}' not found in node_type_to_num mapping")

# Convert the node_type_data array to numerical values, using -1 for missing node types
node_type_data_num = np.vectorize(lambda x: node_type_to_num.get(x, -1))(node_type_data)

# Downsample the grid by a factor of 1 (no downsampling)
downsample_factor = 1
node_type_data_downsampled_num = zoom(node_type_data_num, (1/downsample_factor, 1/downsample_factor), order=0)

# Normalize the node type data for color mapping
node_type_cmap = ListedColormap(['green', 'yellow', 'red', 'gray'])

# Plot the node type heatmap
fig, ax = plt.subplots(figsize=(10, 8))
cax = ax.matshow(node_type_data_downsampled_num, cmap=node_type_cmap, aspect='auto')

# Invert the y-axis to have the lowest values at the bottom
ax.invert_yaxis()

# Set y-tick labels
ax.set_yticklabels(np.linspace(min(b_values), max(b_values), 5, dtype=int))

ax.set_xlabel('a')
ax.set_ylabel('b')

# Add color bar
cbar = fig.colorbar(cax, ticks=[0, 1, 2, -1])
cbar.ax.set_yticklabels(['Index Scan', 'Bitmap Heap Scan', 'Seq Scan', 'Missing'])

plt.tight_layout()

# Ensure the output directory exists
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots'
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, 'heatmap_nodes_skew_example.png')
plt.savefig(output_path, dpi=600, bbox_inches='tight')
plt.show()