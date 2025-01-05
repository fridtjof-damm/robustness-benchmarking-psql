import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import re
import numpy as np
import os
from src.qgen import tpch_query_parameters, job_query_parameters


def extract_cardinalities(cardinality_str):
    matches = re.findall(r'\([\d,]+,\s*(\d+)\)', cardinality_str)
    return [int(x) for x in matches]


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


def sample_data(data, method='none', target_size=150):
    if method == 'none':
        return data
    elif method == 'systematic':
        n = max(1, len(data) // target_size)
        return data.iloc[::n].reset_index(drop=True)
    else:
        raise ValueError(
            "Invalid sampling method. Use 'none','systematic'")


def extract_params(filter_str, param1_name, param2_name):
    pattern = r'\(([\w_]+),\s*([^)]+)\)'
    matches = re.findall(pattern, filter_str)
    date_values = {name: value.strip() for name, value in matches}
    return date_values[param1_name], date_values[param2_name]


def create_stacked_bar_chart(data, param1_name, param2_name, sampling_method='none',
                             target_sample_size=150, output_file=None, whitespace_width=1):
    try:
        # Debug print statement
        print("Filters column before extraction:")
        print(data['Filters'].head())

        # Process the data
        data[['param1', 'param2']] = data['Filters'].apply(
            lambda x: pd.Series(extract_params(x, param1_name, param2_name))
        )

        data = sample_data(data, sampling_method, target_sample_size)

        data = data[['Query ID', 'Node Types', 'Filters', 'Execution Time',
                     'Cardinality e/a', 'param1', 'param2']]

        data['Processed_Node_Types'] = data['Node Types'].apply(
            process_node_types)
        data['cardinalities'] = data['Cardinality e/a'].apply(
            extract_cardinalities)

        param_combinations = data[['param1', 'param2']].drop_duplicates()

        # Add whitespace between groups
        param_combinations_with_whitespace = []
        for i, row in param_combinations.iterrows():
            param_combinations_with_whitespace.append(row)
            if i < len(param_combinations) - 1:
                next_row = param_combinations.iloc[i + 1]
                if row['param2'] != next_row['param2']:
                    for _ in range(whitespace_width):
                        param_combinations_with_whitespace.append(
                            pd.Series({'param1': '', 'param2': ''}))

        param_combinations_with_whitespace = pd.DataFrame(
            param_combinations_with_whitespace)

        # plot setup
        plt.figure(figsize=(18, 10))
        plt.subplots_adjust(bottom=0.2)
        # unique node types for coloring
        all_node_types = [node for sublist in data['Processed_Node_Types']
                          for node in sublist]
        unique_node_types = list(dict.fromkeys(all_node_types))
        colors = plt.cm.tab20(np.linspace(0, 1, len(unique_node_types)))
        color_map = dict(zip(unique_node_types, colors))

        x = np.arange(len(param_combinations_with_whitespace))
        bottom = np.zeros(len(param_combinations_with_whitespace))

        # Plot each node type's cardinalities
        for idx, node_type in enumerate(unique_node_types):
            print(
                f"Plotting node type {idx + 1}/{len(unique_node_types)}: {node_type}")
            values = [row[i] for row, nodes in zip(data['cardinalities'], data['Processed_Node_Types'])
                      for i, n in enumerate(nodes) if n == node_type]
            values_with_whitespace = np.zeros(
                len(param_combinations_with_whitespace))
            values_with_whitespace[:len(values)] = values
            plt.bar(x, values_with_whitespace, bottom=bottom,
                    color=color_map[node_type], label=node_type)
            bottom += values_with_whitespace

        plt.ylabel('Cardinality', fontsize=20)
        plt.yticks(fontsize=16)

        unique_param1 = param_combinations['param1'].unique()
        label_indices = []
        for param1 in unique_param1:
            indices = param_combinations.index[param_combinations['param1'] == param1].tolist(
            )
            if indices:
                first_occurrence = indices[0]
                last_occurrence = indices[-1]
                label_indices.append(first_occurrence)  # First occurrence
                label_indices.append(last_occurrence)  # Last occurrence

                # Perform additional sampling if needed
                sampled_indices = indices[1:-1]  # Exclude first and last
                if len(sampled_indices) > 0:
                    sampled_indices = np.random.choice(sampled_indices, min(
                        len(sampled_indices), target_sample_size - 2), replace=False).tolist()

                label_indices.extend(sampled_indices)

        # Ensure unique indices
        label_indices = list(set(label_indices))

        # set x-axis labels at the identified indices
        plt.xticks(x[label_indices],
                   [f'1 {param_combinations["param2"][i]} - 2 {param_combinations["param1"][i]}' for i in label_indices],
                   rotation=45, ha='right', fontsize=14)

        # set the x-axis limit to zoom in on the left if stack bar count is over 30
        if len(param_combinations) > 30:
            x_max = x.max() / 7.55
            plt.xlim(-0.5, x_max)

        plt.grid(True, axis='y', linestyle='--', alpha=0.7)

        # remove underscores from legend labels
        handles, labels = plt.gca().get_legend_handles_labels()
        labels = [label.replace('_', ' ') for label in labels]
        plt.legend(handles, labels, title='Operators', bbox_to_anchor=(
            1.05, 1), loc='upper left', borderaxespad=0., fontsize=18)

        # format y-axis with scientific notation
        plt.gca().yaxis.set_major_formatter(ticker.ScalarFormatter(useMathText=True))
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
        plt.gca().yaxis.get_offset_text().set_fontsize(14)
        # adjust layout
        plt.tight_layout()

        # save or show the plot
        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            plt.close()
        else:
            plt.show()

    except Exception as e:
        print(f"Error creating plot: {str(e)}")


def process_benchmark(benchmark):
    if benchmark == "tpch":
        csv_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/csvs/plottable'
        output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/cardinality/stack_bar/tpch/pdf/new'
        query_params = {
            'q2': ('r_name', 'p_type'),  # Switched values
            'q3': ('o_orderdate', 'l_shipdate'),
            'q5': ('o_orderdate', 'r_name'),  # Switched values
            'q7': ('n_name', 'n_name'),
            'q8': ('r_name', 'p_type'),
            'q11': ('n_name', 'n_name'),
            'q12': ('l_shipmode', 'l_receiptdate'),
            'q13': ('o_comment', 'o_comment'),
            'q14': ('l_shipdate', 'l_shipdate'),
            'q17': ('p_container', 'p_brand')  # Switched values
        }
    elif benchmark == "job":
        csv_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/csvs/plottable'
        output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/cardinality/stack_bar/job/pdf'
        query_params = job_query_parameters
    else:
        print("Error: Invalid benchmark type. Please use 'tpch' or 'job'.")
        return

    # Iterate over each CSV file in the directory
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            csv_path = os.path.join(csv_dir, filename)
            data = pd.read_csv(csv_path)
            qid = filename.split('.')[0]  # Extract query ID from filename

            # Print the number of rows in the CSV
            print(f"Number of rows in the CSV ({filename}): {len(data)}")

            # Get the parameter names for the current query ID
            param1_name, param2_name = query_params.get(qid, (None, None))
            if param1_name is None or param2_name is None:
                print(f"Query ID {qid} not found in query_params mapping.")
                continue

            # Extract parameters from filters
            print("Extracting parameters from filters...")
            data[['param1', 'param2']] = data['Filters'].apply(
                lambda x: pd.Series(extract_params(
                    x, param1_name, param2_name))
            )

            # Debug print statement
            print("Extracted param1 and param2 values:")
            print(data[['param1', 'param2']].head())

            # Get unique parameter combinations
            param_combinations = data[['param1', 'param2']].drop_duplicates()
            num_stacks = len(param_combinations)

            # Only create the systematic sampled plot if there are more than 1000 stacks/bars
            if num_stacks > 1000:
                print(
                    f"Number of stacks ({num_stacks}) exceeds 1000, creating only the systematic sampled plot.")
                create_stacked_bar_chart(
                    data,
                    param1_name,
                    param2_name,
                    sampling_method='systematic',
                    target_sample_size=150,
                    output_file=f'{output_dir}/{qid}_sampled.pdf',
                    whitespace_width=1  # Adjust the whitespace width as needed
                )
            else:
                # Create plots with different sampling methods
                create_stacked_bar_chart(
                    data,
                    param1_name,
                    param2_name,
                    sampling_method='none',
                    target_sample_size=150,
                    output_file=f'{output_dir}/{qid}.pdf',
                    whitespace_width=1  # Adjust the whitespace width as needed
                )

                create_stacked_bar_chart(
                    data,
                    param1_name,
                    param2_name,
                    sampling_method='systematic',
                    target_sample_size=150,
                    output_file=f'{output_dir}/{qid}_sampled.pdf',
                    whitespace_width=1  # Adjust the whitespace width as needed
                )


if __name__ == "__main__":
    benchmark = input("Enter the benchmark type (tpch/job): ").strip().lower()
    process_benchmark(benchmark)
