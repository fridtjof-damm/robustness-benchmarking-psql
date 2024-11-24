import os
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize, LinearSegmentedColormap
from src.analysis.qanalyze import query_nodes_info, calc_qerror

# Example usage
directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/skew_example_plans_simplified'
query_info = query_nodes_info(directory)
qerrors = calc_qerror(query_info)

# Extract a and b values from query_info
a_values = []
b_values = []
for query_id, (node_types, filters, execution_times, cardinalities) in query_info.items():
    for filter_list in filters:
        for k, v in filter_list:
            if k == 'a':
                a_values.append(int(v))
            elif k == 'b':
                b_values.append(int(v))

# Get unique and sorted a and b values
a_values = sorted(set(a_values))
b_values = sorted(set(b_values))

# Create a grid for the heatmap
heatmap_data = np.zeros((len(b_values), len(a_values)))

# Calculate the 95th percentile q-error
qerror_values = list(qerrors.values())
percentile_95_qerror = np.percentile(qerror_values, 95)

# Populate the grid with qerror values
for query_id, qerror in qerrors.items():
    filters = query_info[query_id][1]
    for filter_list in filters:
        a_value = None
        b_value = None
        for k, v in filter_list:
            if k == 'a':
                a_value = int(v)
            elif k == 'b':
                b_value = int(v)
        if a_value is not None and b_value is not None:
            a_index = a_values.index(a_value)
            b_index = b_values.index(b_value)
            heatmap_data[b_index, a_index] = min(qerror, percentile_95_qerror)  # Clip q-error values to the 95th percentile

# Define the green-to-red color map
cmap = LinearSegmentedColormap.from_list('GreenRed', ['green', 'yellow', 'red'])

# Plot the heatmap with linear normalization
fig, ax = plt.subplots(figsize=(10, 8))
norm = Normalize(vmin=np.min(heatmap_data[heatmap_data > 0]), vmax=percentile_95_qerror)
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

# Add color bar with percentage labels
cbar = fig.colorbar(cax)
cbar.set_label('Q-Error (%)')
cbar_ticks = np.linspace(np.min(heatmap_data[heatmap_data > 0]), percentile_95_qerror, num=5)
cbar.set_ticks(cbar_ticks)
cbar.set_ticklabels([f'{int(tick * 100)}%' for tick in cbar_ticks])

plt.tight_layout()

# Customize grid
plt.grid(True, which='major', linestyle='-', alpha=0.3)
plt.grid(True, which='minor', linestyle='-', alpha=0.1)

# Ensure the output directory exists
output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/pdf'
os.makedirs(output_dir, exist_ok=True)

# Save the plot as a PDF
output_path = os.path.join(output_dir, 'qerror_heatmap_rg.pdf')
plt.savefig(output_path, format='pdf', bbox_inches='tight')

# Show the plot
plt.show()
plt.close()