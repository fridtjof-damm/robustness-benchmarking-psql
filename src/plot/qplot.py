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
    exec_times.append(row[0])
    scan_types.append(row[1])
assert len(exec_times) == len(scan_types)
# plotting 
fig, ax = plt.subplots(1,1)

numbers = [x for x in range(0,20)]
colors = ['darkseagreen','lightskyblue']
bar_labels = ['index scan', 'seq scan']
bar_colors = []
for scan in scan_types:
    if scan == 'index_scan':
        bar_colors.append(colors[0])
    if scan == 'seq_scan':
        bar_colors.append(colors[1])

ax.bar(numbers, exec_times,color=bar_colors)
ax.set_xticks(np.arange(len(exec_times)))
ax.set_ylabel('execution time')

plt.show()



