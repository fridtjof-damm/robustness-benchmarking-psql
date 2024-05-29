from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from utils import csv_to_data_list, csv_to_values_list
# source file
FILE = 'results/3.csv'

axis_len = csv_to_data_list(FILE)[1]
# data to plot
data = csv_to_data_list(FILE)[0]

print(len(data))

# get min and max to normalize color coding
print(min([i[2] for i in data]), max([i[2] for i in data]))
norm = mpl.colors.Normalize(min([i[2] for i in data]), max([i[2] for i in data]))


cmap = LinearSegmentedColormap.from_list('My color Map', colors=['green', 'yellow', 'red'])

fig, ax = plt.subplots(1,1)

for item in data:
    y = item[0]
    x = item[1]
    
    color = cmap(norm(item[2]))
    polygon = Polygon([(x, y), (x+1, y), (x+1, y+1), (x, y+1), (x, y)], color=color)
    ax.add_patch(polygon)
    
plt.ylim(0,axis_len)
plt.xlim(0,axis_len)


# define explicit labels for the x and y axis
values_x = csv_to_values_list(FILE)[0]
values_y = csv_to_values_list(FILE)[1]

ax.set_xticks(list(range(0, axis_len, 4)))
ax.set_xticklabels(values_x[0:axis_len:4])
ax.set_xlabel('order_date')
plt.xticks(rotation=45, ha='right')

ax.set_yticks(list(range(0, axis_len, 4)))
ax.set_yticklabels(values_y[0:axis_len:4])
ax.set_ylabel('mktsegment')

plt.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)
plt.savefig('plots/3.png')
plt.show()
