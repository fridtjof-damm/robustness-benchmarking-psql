import csv
import math
import re
import pandas as pd
import os


def format_tuple(t: tuple) -> str:
    # (1, "algeria", 10) -> 1; algeria; 10
    result = ''
    for i in t:
        result += str(i) + ';'
    return result[:-1]


def csv_to_data_list(file) -> tuple[list[list], int]:
    data: list[list] = []
    with open(file, encoding='UTF8') as file:
        csv_data = list(csv.reader(file, delimiter=';'))
        sqr_len = round(math.sqrt(len(csv_data)))
        axis_len = sqr_len

    k = 0
    for i in range(sqr_len-1):
        for j in range(sqr_len-1):
            data.append([i, j, float(csv_data[k][0])])
            k += 1
    return data, axis_len


def csv_to_values_list(file) -> tuple[list, list]:
    axis_len = csv_to_data_list(file)[1]
    values_x: list[str] = []
    values_y: list[str] = []

    with open(file, encoding='UTF8') as file:
        csv_data = list(csv.reader(file, delimiter=';'))

    for i in range(len(csv_data)):
        values_y.append(str(csv_data[i][1]))
        values_x.append(str(csv_data[i][2]))

    step_list = len(values_x) // axis_len
    values_x = [values_x[i]
                for i in range(0, len(values_x), step_list)][:axis_len]
    values_y = [values_y[i]
                for i in range(0, len(values_y), step_list)][:axis_len]

    return values_x, values_y

# from picasso templates retrieve all parameters to prepare PICASSO qgen section


def get_picasso_gen_list():
    query_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16, 17]
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

# get_picasso_gen_list()

# function to extract number and suffix (letters for variations) from query filenames
# used in qanalyze.py for example in the function job_profiling()


def extract_number(filename) -> tuple[int, str]:
    match = re.match(r'(\d+)([a-zA-Z]*)', filename)
    if match:
        number = int(match.group(1))
        suffix = match.group(2)
        return (number, suffix)
    return (float('inf'), '')

# correctly split node filters of query plans into key value pairs


def process_filters(filters: str) -> dict:
    filters = filters.strip('"')  # Remove the leading and trailing quotes
    # Remove the leading and trailing parentheses
    filters = filters.strip('()')
    # Split the string into individual filters
    filter_list = filters.split('), (')
    filter_dict = {}
    for item in filter_list:
        key, value = item.split(',')
        filter_dict[key.strip()] = value.strip()
    return filter_dict


###
# stack plot help functions
###


def extract_relevant_filters(filter_str, param1_name, param2_name):
    pattern = r'\(([\w_]+),\s*([^)]+)\)'
    matches = re.findall(pattern, filter_str)
    param_dict = dict((name.strip(), value.strip()) for name, value in matches)
    return param_dict.get(param1_name, ''), param_dict.get(param2_name, '')


def sample_data(data, method='none', target_size=150):
    if len(data) <= 100:
        return data
    if method == 'none':
        return data
    elif method == 'stratified':
        param_groups = data.groupby(['param1', 'param2'])
        sampling_rate = target_size / len(param_groups)
        sampled_data = param_groups.apply(
            lambda x: x.sample(max(1, int(len(x) * sampling_rate))),
            include_groups=False
        )
        return sampled_data.reset_index(drop=True)
    elif method == 'systematic':
        n = max(1, len(data) // target_size)
        return data.iloc[::n].reset_index(drop=True)
    else:
        raise ValueError(
            "Invalid sampling method. Use 'none', 'stratified', or 'systematic'")


def extract_params(filter_str):
    pattern = r'\(([\w_]+),\s*([^)]+)\)'
    matches = re.findall(pattern, filter_str)
    param_dict = dict((name.strip(), value.strip())
                      for name, value in matches)
    return param_dict.get(param1_name, ''), param_dict.get(param2_name, '')


def extract_cardinalities(cardinality_str):
    matches = re.findall(r'\([\d,]+,\s*(\d+)\)', cardinality_str)
    return [int(x) for x in matches]


def process_csv_and_discard_equals(input_file, output_file, query_parameters):
    # Read the CSV file
    df = pd.read_csv(input_file)

    # Function to filter out parameters with '='
    def filter_out_equals(filters):
        return ', '.join([f for f in filters.split('), (') if '=' not in f])

    # Discard filters that are not relevant to the plot
    # extract query id
    query_id = os.path.basename(input_file).split('.')[0]
    param1_name, param2_name = query_parameters[query_id]
    print(query_id, param1_name, param2_name)

    # Ensure Filters column contains string values
    df['Filters'] = df['Filters'].astype(str)

    # Extract relevant filters
    df['param1'], df['param2'] = zip(
        *[extract_relevant_filters(x, param1_name, param2_name) for x in df['Filters']])

    # Apply the filter function to the 'Filters' column
    df['Filters'] = df['Filters'].apply(filter_out_equals)

    # Debug print statements
    print(f"Filtered DataFrame columns: {df.columns}")
    print(f"Filtered DataFrame head:\n{df.head()}")

    # Check if output file exists and prompt for overwrite
    if os.path.exists(output_file) and input_file != output_file:
        overwrite = input(
            f"The file {output_file} already exists. Do you want to overwrite it? (y/n): ")
        if overwrite.lower() != 'y':
            print("Operation cancelled.")
            return None

    # Save the modified DataFrame to the output CSV file
    df.to_csv(output_file, index=False)

    print(f"Processed CSV saved to {output_file}")

    # Return the processed DataFrame
    return df


def process_node_types(node_types_str):
    node_types = node_types_str.split(',')
    processed_types = []
    type_counts = {}
    for node_type in node_types:
        node_type = node_type.strip()
        if node_type in type_counts:
            type_counts[node_type] += 1
            processed_types.append(f"{node_type}_{type_counts[node_type]}")
        else:
            type_counts[node_type] = 1
            processed_types.append(f"{node_type}_1")
    return processed_types
