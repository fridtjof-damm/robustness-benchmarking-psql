from matplotlib.patches import Polygon
import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from utils import csv_to_data_list
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


# define explicit labels for the x axis
#values = ['1992-04-01', '1992-07-01', '1992-10-01', '1993-01-01', '1993-04-01', '1993-07-01', '1993-10-01', '1994-01-01', '1994-04-01', '1994-07-01', '1994-10-01', '1995-01-01', '1995-04-01', '1995-07-01', '1995-10-01', '1996-01-01', '1996-04-01', '1996-07-01', '1996-10-01', '1997-01-01', '1997-04-01', '1997-07-01', '1997-10-01', '1998-01-01', '1998-04-01', '1998-07-01', '1998-10-01', '1999-01-01']

ax.set_xticks(list(range(0, axis_len, 4)))
#ax.set_xticklabels(values[0:axis_len:4])
ax.set_xlabel('order_date')
plt.xticks(rotation=45, ha='right')

ax.set_yticks(list(range(0, axis_len, 4)))
#ax.set_yticklabels(values[0:axis_len:4])
ax.set_ylabel('mktsegment')

plt.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)
plt.savefig('plots/3.png')
plt.show()
