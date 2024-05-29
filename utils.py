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
    
    for i in range(sqr_len):
        for j in range(sqr_len):
            data.append([i,j,float(csv_data[i][0])])
    
    return data,axis_len
