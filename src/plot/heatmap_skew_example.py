# based on https://git.tu-berlin.de/halfpap/heatmap
from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
import csv
import os
from scipy.ndimage import zoom

# Source file
FILE = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example/skew_example.csv'

# Read the CSV file and extract filters and execution times
data = []
with open(FILE, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        filters = row['Filters']
        execution_time = float(row['Execution Time'])
        filter_dict = dict(item.strip('()').split(',') for item in filters.split('),('))
        a_value = int(filter_dict['a'])
        b_value = int(filter_dict['b'])
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

# Populate the grid with execution times
for item in data:
    a_index = a_values.index(item[0])
    b_index = b_values.index(item[1])
    heatmap_data[b_index, a_index] = item[2]



# Downsample the grid by a factor of 5 (reduce the number of data points by 25)
downsample_factor = 1
heatmap_data_downsampled = zoom(heatmap_data, (1/downsample_factor, 1/downsample_factor))

# Normalize the execution times for color mapping
norm = mpl.colors.Normalize(vmin=np.min(heatmap_data_downsampled), vmax=np.max(heatmap_data_downsampled))
cmap = LinearSegmentedColormap.from_list('My color Map', colors=['green', 'yellow', 'red'])

# Plot the heatmap
fig, ax = plt.subplots(figsize=(10, 8))
cax = ax.matshow(heatmap_data_downsampled, cmap=cmap, norm=norm, aspect='auto')

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
cbar = fig.colorbar(cax)
cbar.set_label('Execution Time in ms')

plt.title('Execution Time Heatmap for a and b values')
plt.tight_layout()

# Ensure the output directory exists
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots'
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, 'heatmap_skew_example.png')
plt.savefig(output_path, dpi=600, bbox_inches='tight')
plt.show()

