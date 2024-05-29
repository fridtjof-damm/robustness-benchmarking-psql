# import pandas as pd 
""" import csv

values_x: list[str] = []
values_y: list[str] = []

with open('results/3.csv', encoding='UTF8') as file:
    csv_data = list(csv.reader(file, delimiter=';'))

for i in range(len(csv_data)):
        values_y.append(str(csv_data[i][1]))
        values_x.append(str(csv_data[i][2]))

print(values_y) """

values = ['1992-04-01', '1992-07-01', '1992-10-01', '1993-01-01', '1993-04-01', '1993-07-01', '1993-10-01', '1994-01-01', '1994-04-01', '1994-07-01', '1994-10-01', '1995-01-01', '1995-04-01', '1995-07-01', '1995-10-01', '1996-01-01', '1996-04-01', '1996-07-01', '1996-10-01', '1997-01-01', '1997-04-01', '1997-07-01', '1997-10-01', '1998-01-01', '1998-04-01', '1998-07-01', '1998-10-01', '1999-01-01']

print(len(values))