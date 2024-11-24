import csv
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import os

# Source file
FILE = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example/skew_example.csv'

# Read the CSV file and extract filters and cardinalities
data = []
with open(FILE, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        filters = row['Filters']
        cardinality = row['Cardinality e/a']
        # Correctly parse the filters string
        filter_dict = dict(item.strip('()').split(',') for item in filters.split('), ('))
        a_value = int(filter_dict['a'])
        b_value = int(filter_dict['b'])
        # Extract the second position in the first tuple of the cardinality column
        first_tuple_second_position = int(cardinality.split('), (')[0].strip('()').split(',')[1])
        data.append((a_value, b_value, first_tuple_second_position))

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

# Calculate the 85th percentile cardinality
cardinality_values = [item[2] for item in data]
percentile_85_cardinality = np.percentile(cardinality_values, 85)

# Populate the grid with cardinality values, clipping at the 85th percentile
for a_value, b_value, cardinality in data:
    a_index = a_values.index(a_value)
    b_index = b_values.index(b_value)
    heatmap_data[b_index, a_index] = min(cardinality, percentile_85_cardinality)  # Clip cardinality values

# Plot the heatmap with linear normalization
fig, ax = plt.subplots(figsize=(10, 8))
norm = Normalize(vmin=np.min(heatmap_data[heatmap_data > 0]), vmax=percentile_85_cardinality)
cax = ax.matshow(heatmap_data, cmap='viridis', norm=norm, aspect='auto')

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
cbar.set_label('Cardinality')

plt.title('Cardinality Heatmap for a and b values')
plt.tight_layout()

# Customize grid
plt.grid(True, which='major', linestyle='-', alpha=0.3)
plt.grid(True, which='minor', linestyle='-', alpha=0.1)

# Show the plot
plt.show()

# Save the plot
# plt.savefig('cardinality_heatmap.png', dpi=300, bbox_inches='tight')
plt.close()

