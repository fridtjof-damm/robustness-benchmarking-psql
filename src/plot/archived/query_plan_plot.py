import csv
import numpy as np
# only query plan analysis plot so far

#from src.utils.utils import csv_to_data_list
import matplotlib.pyplot as plt
#import matplotlib as mpl
# source file
FILE = 'results/dummyresults/queryplans.csv'
exec_times = []
scan_types = []
with open(FILE, encoding='UTF8') as file:
    csv_data = list(csv.reader(file, delimiter=';'))
    file.close()

for row in csv_data:
    exec_times.append(float(row[0]))
    scan_types.append(row[1])
assert len(exec_times) == len(scan_types)
# plotting 
fig, ax = plt.subplots(1,1)
# numbers = [x for x in range(0,20)]
colors = ['darkseagreen','lightskyblue']
bar_labels = ['index scan', 'seq scan']
bar_colors = [colors[0] if scan == 'index_scan' else colors[1] for scan in scan_types]


x_ticks = np.arange(len(exec_times))
ax.set_xticks(x_ticks)
ax.set_xticklabels(x_ticks)
ax.set_xlabel('queries w/ selection on attribute with number i')
ax.set_yticks(list(range(2,len(exec_times),4)))
print(x_ticks)
print(exec_times)
ax.set_yticklabels([0.18, 0.30, 0.45, 0.60, 0.80])
ax.set_ylabel('exection time in ms')
ax.bar(x_ticks, exec_times, color=bar_colors)
ax.legend(['index scan', 'seq scan'])
plt.show()