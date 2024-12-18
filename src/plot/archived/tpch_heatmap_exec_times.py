import csv
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# Example usage
file_path = f"/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/q3.csv"


def extract_data_from_csv(file_path):
    query_info = {}

    with open(file_path, mode='r', encoding='UTF-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            query_id = int(row['Query ID'])
            node_types = row['Node Types'].split(', ')
            filters = row['Filters'].split(', ')
            execution_time = float(row['Execution Time'])
            cardinalities = [tuple(map(int, pair.strip('()').split(',')))
                             for pair in row['Cardinality e/a'].split(', ')]

            query_info[query_id] = (node_types, filters, [
                                    execution_time], cardinalities)

    return query_info


query_info = extract_data_from_csv(file_path)

# Extract p_size, r_name, and execution time values from query_info
data = []
for query_id, (node_types, filters, execution_times, cardinalities) in query_info.items():
    filter_dict = {}
    for filter_str in filters:
        if ',' in filter_str:
            k, v = filter_str.split(',')
            filter_dict[k.strip()] = v.strip().strip(')')
        print(f"Parsed filter: {filter_dict}")  # Debug print
    if '(p_size' in filter_dict and '(r_name' in filter_dict:
        a_value = int(filter_dict['(p_size'])
        b_value = filter_dict['(r_name']
        execution_time = sum(execution_times) / \
            len(execution_times)  # Average execution time
        data.append((a_value, b_value, execution_time))

# Check if data is populated
if not data:
    print("No data available for plotting.")
else:
    # Find the minimum and maximum execution times and their corresponding a, b values
    min_execution_time = float('inf')
    max_execution_time = float('-inf')
    min_a, min_b = None, None
    max_a, max_b = None, None

    for a_value, b_value, execution_time in data:
        if execution_time < min_execution_time:
            min_execution_time = execution_time
            min_a, min_b = a_value, b_value
        if execution_time > max_execution_time:
            max_execution_time = execution_time
            max_a, max_b = a_value, b_value

    print(
        f"Minimum execution time: {min_execution_time} at a={min_a}, b={min_b}")
    print(
        f"Maximum execution time: {max_execution_time} at a={max_a}, b={max_b}")

    # Create heatmap
    a_values, b_values, execution_times = zip(*data)
    heatmap_data = np.zeros((max(a_values) + 1, len(set(b_values))))

    b_value_to_index = {b: idx for idx, b in enumerate(sorted(set(b_values)))}

    for a, b, execution_time in data:
        heatmap_data[a, b_value_to_index[b]] = execution_time

    plt.figure(figsize=(10, 8))
    plt.imshow(heatmap_data, cmap='viridis', aspect='auto')
    plt.colorbar(label='Execution Time')
    plt.xlabel('p_size')
    plt.ylabel('r_name')
    plt.title('Heatmap of Execution Times')
    plt.xticks(ticks=range(len(set(b_values))), labels=sorted(set(b_values)))
    plt.show()
