# based on https://git.tu-berlin.de/halfpap/heatmap
from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from src.qgen import dates_05, modes
from src.utils.utils import csv_to_data_list
import os

# source file
FILE = 'results/parameters_sf10/12.csv'

axis_len = csv_to_data_list(FILE)[1]
# data to plot
data = csv_to_data_list(FILE)[0]

print(len(data))

# Extract the third column values for clipping
values = [i[2] for i in data]

# Calculate the 95th percentile value
percentile_95_value = np.percentile(values, 95)

# Clip the values at the 95th percentile
clipped_data = [(item[0], item[1], min(item[2], percentile_95_value)) for item in data]

# get min and max to normalize color coding
print(min([i[2] for i in clipped_data]), max([i[2] for i in clipped_data]))
norm = mpl.colors.Normalize(min([i[2] for i in clipped_data]), max([i[2] for i in clipped_data]))

cmap = LinearSegmentedColormap.from_list('My color Map', colors=['green', 'yellow', 'red'])

fig, ax = plt.subplots(1,1)

for item in clipped_data:
    y = item[0]
    x = item[1]
    
    color = cmap(norm(item[2]))
    polygon = Polygon([(x, y), (x+1, y), (x+1, y+1), (x, y+1), (x, y)], color=color)
    ax.add_patch(polygon)
    
plt.ylim(0,axis_len)
plt.xlim(0,axis_len)


# define explicit labels for the x and y axis
""" values_x = dates_05
values_y = modes
step_y = round(axis_len / len(values_y))
print(values_y)
ax.set_xticks(list(range(1, axis_len, 3)))
ax.set_xticklabels(values_x[0:axis_len:3]) """
ax.set_xlabel('region')
""" plt.xticks(rotation=45, ha='right')

ax.set_yticks(list(range(2,axis_len,2)))
ax.set_yticklabels(values_y) """ 
ax.set_ylabel('country')

plt.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)

# Ensure the output directory exists
output_dir = 'plots'
os.makedirs(output_dir, exist_ok=True)

# Save the plot as a PNG
output_path = os.path.join(output_dir, '12_clip_95.png')
plt.savefig(output_path, format='png', bbox_inches='tight')

plt.show()
plt.close()
