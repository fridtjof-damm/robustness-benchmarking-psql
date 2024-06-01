import csv

values_x: list[str] = []
values_y: list[str] = []

with open('results/3.csv', encoding='UTF8') as file:
    csv_data = list(csv.reader(file, delimiter=';'))

for i, row in enumerate(csv_data):
    #values_y.append(str(csv_data[i][1]))
    values_y.append(row[1])
    values_x.append(row[2])

# step_list = len(values_x) // axis_len
#values_x = [values_x[i] for i in range(0,len(values_x), step_list)][:axis_len]
#values_y = [values_y[i] for i in range(0,len(values_y), step_list)][:axis_len]
print(len(values_y))
