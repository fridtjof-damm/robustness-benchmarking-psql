import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
from matplotlib.colors import LinearSegmentedColormap
import matplotlib as mpl
import re


def extract_params(filter_str, param1_name, param2_name):
    pattern = r'\(([\w_]+),\s*([^)]+)\)'
    matches = re.findall(pattern, filter_str)
    date_values = {name: value.strip() for name, value in matches}
    return date_values.get(param1_name, ''), date_values.get(param2_name, '')


def csv_to_data_list(file_path, param1_name, param2_name):
    data = pd.read_csv(file_path)
    data[['param1', 'param2']] = data['Filters'].apply(
        lambda x: pd.Series(extract_params(x, param1_name, param2_name))
    )
    param1_categories = data['param1'].astype('category')
    param2_categories = data['param2'].astype('category')
    data['param1'] = param1_categories.cat.codes
    data['param2'] = param2_categories.cat.codes
    axis_len = max(data['param1'].nunique(), data['param2'].nunique())
    data_list = list(
        zip(data['param1'], data['param2'], data['Execution Time']))
    return data_list, axis_len, param1_categories.cat.categories, param2_categories.cat.categories


def create_execution_time_heatmap(data, axis_len, param1_labels, param2_labels, output_file):
    values = [i[2] for i in data]
    percentile_95_value = np.percentile(values, 95)
    clipped_data = [(item[0], item[1], min(item[2], percentile_95_value))
                    for item in data]

    norm = mpl.colors.Normalize(
        min([i[2] for i in clipped_data]), max([i[2] for i in clipped_data]))
    cmap = LinearSegmentedColormap.from_list(
        'My color Map', colors=['green', 'yellow', 'red'])

    fig, ax = plt.subplots(1, 1)
    for item in clipped_data:
        y = item[0]
        x = item[1]
        color = cmap(norm(item[2]))
        polygon = Polygon([(x, y), (x + 1, y), (x + 1, y + 1),
                          (x, y + 1), (x, y)], color=color)
        ax.add_patch(polygon)

    plt.ylim(0, axis_len)
    plt.xlim(0, axis_len)
    ax.set_xticks(np.arange(len(param1_labels)) + 0.5)
    ax.set_xticklabels(param1_labels, rotation=45, ha='right')
    ax.set_yticks(np.arange(len(param2_labels)) + 0.5)
    ax.set_yticklabels(param2_labels)
    ax.set_xlabel('param1')
    ax.set_ylabel('param2')
    plt.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)
    plt.savefig(output_file, format='pdf', bbox_inches='tight')
    plt.close()


def process_benchmark(benchmark):
    if benchmark == "tpch":
        csv_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/csvs/plottable'
        output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/execution_times/tpch'
        query_params = {
            'q2': ('r_name', 'p_type'),
            'q3': ('o_orderdate', 'l_shipdate'),
            'q5': ('o_orderdate', 'r_name'),
            'q7': ('n_name', 'n_name'),
            'q8': ('r_name', 'p_type'),
            'q11': ('n_name', 'n_name'),
            'q12': ('l_shipmode', 'l_receiptdate'),
            'q13': ('o_comment', 'o_comment'),
            'q14': ('l_shipdate', 'l_shipdate'),
            'q17': ('p_container', 'p_brand')
        }
    elif benchmark == "job":
        csv_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/csvs/plottable'
        output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/execution_times/job'
        query_params = {
            '12c': ('production_year', 'rating'),
            '15a': ('production_year', 'country_code'),
            '14b': ('production_year', 'rating')
        }
    else:
        print("Error: Invalid benchmark type. Please use 'tpch' or 'job'.")
        return

    os.makedirs(output_dir, exist_ok=True)

    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            csv_path = os.path.join(csv_dir, filename)
            qid = filename.split('.')[0]
            param1_name, param2_name = query_params.get(qid, (None, None))
            if param1_name is None or param2_name is None:
                print(f"Query ID {qid} not found in query_params mapping.")
                continue
            data, axis_len, param1_labels, param2_labels = csv_to_data_list(
                csv_path, param1_name, param2_name)
            output_file = os.path.join(output_dir, f'{qid}.pdf')
            create_execution_time_heatmap(
                data, axis_len, param1_labels, param2_labels, output_file)


if __name__ == "__main__":
    benchmark = input("Enter the benchmark type (tpch/job): ").strip().lower()
    process_benchmark(benchmark)
