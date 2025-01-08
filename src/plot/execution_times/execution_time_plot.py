import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
from matplotlib.colors import LinearSegmentedColormap
import matplotlib as mpl
import re
import sys


def extract_params(filter_str, param1_name, param2_name):
    pattern = r'\(([\w_]+),\s*([^)]+)\)'
    matches = re.findall(pattern, filter_str)
    date_values = {name: value.strip() for name, value in matches}
    return date_values.get(param1_name, ''), date_values.get(param2_name, '')


def csv_to_data_list(file_path, param1_name, param2_name, target_size=1000):
    print(f"Processing file: {file_path}")
    data = pd.read_csv(file_path)
    data[['param1', 'param2']] = data['Filters'].apply(
        lambda x: pd.Series(extract_params(x, param1_name, param2_name))
    )

    if param2_name == 'rating':
        data['param2'] = pd.to_numeric(data['param2'])
        data = data[data['param2'] <= 10]
        data = data.sort_values('param2')
    else:
        try:
            data['param2'] = pd.to_numeric(data['param2'])
            data = data.sort_values('param2')
        except ValueError:
            data = data.sort_values('param2')

    param1_categories = data['param1'].astype('category')
    param2_categories = data['param2'].astype('category')
    data['param1'] = param1_categories.cat.codes
    data['param2'] = param2_categories.cat.codes
    axis_len = max(data['param1'].nunique(), data['param2'].nunique())
    data_list = list(
        zip(data['param1'], data['param2'], data['Execution Time'])
    )

    print("Data list created:")
    print(data_list[:10])  # Print the first 10 entries for brevity

    return data_list, axis_len, param1_categories.cat.categories, param2_categories.cat.categories


def create_execution_time_heatmap(data, axis_len, param1_labels, param2_labels, param1_name, param2_name, output_file):
    # Debug print statement to check the data being used for plotting
    print("Data being used for heatmap plotting:")
    print(data[:10])  # Print the first 10 entries for brevity

    # Check the range of param1 and param2 values
    param1_values = [item[0] for item in data]
    param2_values = [item[1] for item in data]
    print(
        f"Range of param1 values: {min(param1_values)} to {max(param1_values)}")
    print(
        f"Range of param2 values: {min(param2_values)} to {max(param2_values)}")

    norm = mpl.colors.Normalize(
        min([i[2] for i in data]), max([i[2] for i in data]))
    cmap = LinearSegmentedColormap.from_list(
        'My color Map', colors=['green', 'yellow', 'red'])

    fig, ax = plt.subplots(1, 1)
    for item in data:
        x = item[0]
        y = item[1]
        color = cmap(norm(item[2]))
        polygon = Polygon([(x, y), (x + 1, y), (x + 1, y + 1),
                          (x, y + 1), (x, y)], color=color)
        ax.add_patch(polygon)

    plt.ylim(0, len(param2_labels))
    plt.xlim(0, len(param1_labels))

    if len(param1_labels) > 20:
        step = (len(param1_labels)-2) // 18
        selected_indices = [
            0] + list(range(step, len(param1_labels)-1, step)) + [len(param1_labels)-1]
        selected_indices = sorted(list(set(selected_indices)))
        xticks = np.array(selected_indices)
        xticklabels = [param1_labels[i] for i in selected_indices]
    else:
        xticks = np.arange(len(param1_labels))
        xticklabels = param1_labels

    if len(param2_labels) > 20:
        step = (len(param2_labels)-2) // 18
        selected_indices = [
            0] + list(range(step, len(param2_labels)-1, step)) + [len(param2_labels)-1]
        selected_indices = sorted(list(set(selected_indices)))
        yticks = np.array(selected_indices)
        yticklabels = [param2_labels[i] for i in selected_indices]
    else:
        yticks = np.arange(len(param2_labels))
        yticklabels = param2_labels

    ax.set_xticks(xticks + 0.5)
    ax.set_xticklabels(xticklabels, rotation=45, ha='right')
    ax.set_yticks(yticks + 0.5)
    ax.set_yticklabels(yticklabels)
    ax.set_xlabel(param1_name)
    ax.set_ylabel(param2_name)
    plt.colorbar(mpl.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax)
    plt.savefig(output_file, format='pdf', bbox_inches='tight')
    plt.close()


def plot_execution_times(data_list, axis_len, param1_categories, param2_categories):

    # Existing plotting logic
    fig, ax = plt.subplots(figsize=(15, 10))
    cmap = LinearSegmentedColormap.from_list("mycmap", ["blue", "red"])
    norm = mpl.colors.Normalize(vmin=0, vmax=axis_len)
    for param1, param2, exec_time in data_list:
        ax.scatter(param1, param2, s=exec_time, c=cmap(
            norm(param1)), alpha=0.6, edgecolors='w', linewidth=0.5)

    ax.set_xticks(np.arange(len(param1_categories)))
    ax.set_xticklabels(param1_categories, rotation=90)
    ax.set_yticks(np.arange(len(param2_categories)))
    ax.set_yticklabels(param2_categories)
    ax.set_xlabel('Param1')
    ax.set_ylabel('Param2')
    ax.set_title('Execution Times')
    plt.colorbar(mpl.cm.ScalarMappable(
        norm=norm, cmap=cmap), ax=ax, label='Param1')
    plt.tight_layout()
    plt.show()


def process_benchmark(benchmark):
    print(f"Processing benchmark: {benchmark}")
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
            print(f"Creating heatmap for {qid}")
            create_execution_time_heatmap(
                data, axis_len, param1_labels, param2_labels, param1_name, param2_name, output_file)


if __name__ == "__main__":
    benchmark = input("Enter the benchmark type (tpch/job): ").strip().lower()
    process_benchmark(benchmark)
