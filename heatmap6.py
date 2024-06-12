# based on https://git.tu-berlin.de/halfpap/heatmap
from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from qgen import dates_05, modes
from utils import csv_to_data_list
# source file
FILE = 'results/6.csv'

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
""" values_x = modes
values_y = [(dates_05)]
step_y = round(axis_len / len(values_y))
print(values_y)
ax.set_xticks(list(range(1, axis_len, 3)))
ax.set_xticklabels(values_x[0:axis_len:3]) """
ax.set_xlabel('date')
""" plt.xticks(rotation=45, ha='right')

ax.set_yticks(list(range(2,axis_len,2)))
ax.set_yticklabels(values_y) """ 
ax.set_ylabel('')

plt.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)
plt.savefig('plots/6.png')
plt.show()
