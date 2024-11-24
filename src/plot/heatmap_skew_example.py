from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
from scipy.stats import mstats
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
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
print(f"Maximum execution time: {max_execution_time} at a={max_a}, b={max_b}")# Inspect specific data points and queries associated with the green outliers




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

# Calculate the 10th percentile of execution times
percentile_10 = np.percentile([item[2] for item in data], 10)

# Calculate Winsorized mean, trimming top 10% of values
winsorized_mean = mstats.winsorize(np.array([item[2] for item in data]), limits=(0, 0.10)).mean()

outliers = [item for item in data if item[2] < percentile_10 or item[2] > winsorized_mean]
print("Outliers:")
for outlier in outliers:
    if outlier:
        print("otliers found:")
        print(outlier)
if not outliers:
    print("none found")

# Normalize the execution times for color mapping, cutting out the lower 10% and using Winsorized mean for upper bound
norm = mpl.colors.Normalize(vmin=percentile_10, vmax=winsorized_mean)
cmap = LinearSegmentedColormap.from_list('My color Map', colors=['green', 'yellow', 'orange', 'red'])

# Plot the heatmap
fig, ax = plt.subplots(figsize=(10, 8))
cax = ax.matshow(heatmap_data, cmap=cmap, norm=norm, aspect='auto')

# Invert the y-axis to have the lowest values at the bottom
ax.invert_yaxis()

# Set axis labels
ax.set_xticks(np.arange(len(a_values)))
ax.set_xticklabels(a_values, rotation=0)  # Set rotation to 0 for horizontal alignment
ax.xaxis.set_ticks_position('bottom')  # Move x-axis labels to the bottom

# Set y-axis labels to show values in steps of 100 and only show 10 values
y_step = max(1, len(b_values) // 10)
y_ticks = np.arange(0, len(b_values), y_step)
ax.set_yticks(y_ticks)
ax.set_yticklabels([b_values[i] for i in y_ticks])

ax.set_xlabel('a')
ax.set_ylabel('b')

# Add color bar
cbar = fig.colorbar(cax)
cbar.set_label('Execution Time in ms')
cbar.ax.tick_params(size=0)  # Remove tick marks from color bar

plt.title('Execution Time Heatmap for a and b values')
plt.tight_layout()

# Ensure the output directory exists
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots'
os.makedirs(output_dir, exist_ok=True)

# Save the plot
output_path = os.path.join(output_dir, 'heatmap_skew_example.png')
plt.savefig(output_path, dpi=600, bbox_inches='tight')
plt.show()

