# import pandas as pd 
import csv
import math

data: list[list] = []
with open('results/3.csv', encoding='UTF8') as file:
    csv_data = list(csv.reader(file, delimiter=';'))
   # for row in csv_data_qm:
    #    csv_data = ', '.join(row) 
    sqr_len = round(math.sqrt(len(csv_data)))
    #print(csv_data)

for i in range(sqr_len):
    for j in range(sqr_len):
        data.append([i,j,float(csv_data[i][0])])

print(len(data))
