import csv
import math
import re

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
    
    k = 0
    for i in range(sqr_len-1):
        for j in range(sqr_len-1):
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

# from picasso templates retrieve all parameters to prepare PICASSO qgen section

def get_picasso_gen_list():
    query_ids = [1,2,3,4,5,6,7,8,9,10,11,12,13,16,17]
    query_templates = []
    extract_vals = []
    parameters = []
    for query_id in query_ids:
        with open(f"resources/queries_picasso/qt{query_id}.sql", encoding='UTF-8', mode='r') as f:
            query_template = f.read()
            query_templates.append(query_template)
            f.close()
    
    for query in query_templates:
        matches = re.findall(r'\{(.*?)\}', query)
        for param in matches:
            if param not in parameters:
                parameters.append(param)
    print(parameters)

#get_picasso_gen_list()

# function to extract number and suffix (letters for variations) from query filenames
# used in qanalyze.py for example in the function job_profiling()
def extract_number(filename) -> tuple[int,str]:
    match = re.match(r'(\d+)([a-zA-Z]*)', filename)
    if match:
        number = int(match.group(1))
        suffix = match.group(2)
        return (number, suffix)
    return (float('inf'), '')
