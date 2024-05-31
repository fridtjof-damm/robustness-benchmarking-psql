# import pandas as pd 
import csv
import math

data: list[list] = []
with open('results/3.csv', encoding='UTF8') as file:
    csv_data = list(csv.reader(file, delimiter=';'))
    sqr_len = round(math.sqrt(len(csv_data)))
    #for row in csv_data:
       # csv_data = ', '.join(row) 
    # print(csv_data)

    k = 0
    for i in range(sqr_len):
        for j in range(sqr_len):
                data.append([i,j,float(csv_data[k][0])])
                k += 1

print(data)

