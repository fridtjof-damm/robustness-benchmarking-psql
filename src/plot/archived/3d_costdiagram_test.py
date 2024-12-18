import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import csv
import os

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
        data.append((b_value, a_value, execution_time))

# Get unique values for a and b and sort them in reverse order
b_values = sorted(set(item[0] for item in data), reverse=True)
a_values = sorted(set(item[1] for item in data), reverse=True)

# Create a 3D grid for the heatmap
b_grid, a_grid = np.meshgrid(b_values, a_values)
heatmap_data = np.zeros((len(a_values), len(b_values)))

# Populate the 3D grid with execution times
for item in data:
    b_index = b_values.index(item[0])
    a_index = a_values.index(item[1])
    heatmap_data[a_index, b_index] = item[2]

# Normalize the execution times for color mapping
min_exec_time = np.min(heatmap_data)
max_exec_time = np.max(heatmap_data)
norm = plt.Normalize(vmin=min_exec_time, vmax=max_exec_time)

# Create a custom colormap with the colors reversed
cmap = plt.get_cmap('RdYlGn_r')

# Set up the 3D figure and axis
fig = plt.figure(figsize=(12, 10))
ax = fig.add_subplot(111, projection='3d')

# Plot the 3D heatmap
ax.plot_surface(b_grid, a_grid, heatmap_data, rstride=1, cstride=1, cmap=cmap, norm=norm)

# Set axis labels and title
ax.set_xlabel('b')
ax.set_ylabel('a')
ax.set_zlabel('Execution Time in ms')
ax.set_title('Execution Time Heatmap for a and b values (Switched Axes)')

# Find min, max, and median execution times
min_exec_time = np.min(heatmap_data)
max_exec_time = np.max(heatmap_data)
median_exec_time = np.median(heatmap_data)

# Annotate min, max, and median execution times on the heatmap
#ax.text(b_values[-1], a_values[0], min_exec_time, f'min\n{min_exec_time:.3f} ms', ha='left', va='top', color='black', fontsize=10)
#ax.text(b_values[0], a_values[-1], max_exec_time, f'max\n{max_exec_time:.3f} ms', ha='right', va='bottom', color='black', fontsize=10)
#ax.text(b_values[len(b_values)//2], a_values[len(a_values)//2], median_exec_time, f'median\n{median_exec_time:.3f} ms', ha='center', va='center', color='black', fontsize=10)

# Save the plot
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots'
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, '3d_heatmap_skew_example_switched_color_reversed.png')
plt.savefig(output_path, dpi=600, bbox_inches='tight')
plt.show()