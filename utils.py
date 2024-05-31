import csv
import math

def format_tuple(t: tuple) -> str:
    # (1, "algeria", 10) -> 1; algeria; 10
    result = ''
    for i in t:
        result += str(i) + ';'
    return result[:-1]

def csv_to_data_list(file) -> tuple[list[list],int]:
    data: list[list] = []
    with open(file, encoding='UTF8') as file:
        csv_data = list(csv.reader(file, delimiter=';'))
        sqr_len = round(math.sqrt(len(csv_data)))
        axis_len = sqr_len
    
    k = 1
    for i in range(sqr_len):
        for j in range(sqr_len):
            data.append([i,j,float(csv_data[k][0])])
            k += 1
    
    return data,axis_len

def csv_to_values_list(file) -> tuple[list,list]:
    axis_len = csv_to_data_list(file)[1]
    values_x: list[str] = []
    values_y: list[str] = []

    with open(file, encoding='UTF8') as file:
        csv_data = list(csv.reader(file, delimiter=';'))

    for i in range(len(csv_data)):
        values_y.append(str(csv_data[i][1]))
        values_x.append(str(csv_data[i][2]))

    step_list = len(values_x) // axis_len
    values_x = [values_x[i] for i in range(0,len(values_x), step_list)][:axis_len]
    values_y = [values_y[i] for i in range(0,len(values_y), step_list)][:axis_len]

    return values_x,values_y
