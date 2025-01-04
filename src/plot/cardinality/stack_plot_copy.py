import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import re
import numpy as np


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
    elif method == 'stratified':
        # Group by parameter combinations and sample proportionally
        param_groups = data.groupby(['param1', 'param2'])
        sampling_rate = target_size / len(param_groups)
        sampled_data = param_groups.apply(
            lambda x: x.sample(max(1, int(len(x) * sampling_rate))),
            include_groups=False
        ).reset_index(drop=True)
        return sampled_data
    elif method == 'systematic':
        # Systematic sampling with fixed interval
        n = max(1, len(data) // target_size)
        return data.iloc[::n].reset_index(drop=True)
    else:
        raise ValueError(
            "Invalid sampling method. Use 'none', 'stratified', or 'systematic'")


def create_stacked_bar_chart(data, param1_name, param2_name, sampling_method='none',
                             target_sample_size=150, output_file=None, whitespace_width=1):
    try:
        # Extract parameters from filters
        def extract_params(filter_str):
            pattern = r'\(([\w_]+),\s*([^)]+)\)'
            matches = re.findall(pattern, filter_str)
            date_values = {name: value.strip() for name, value in matches}
            return date_values[param1_name], date_values[param2_name]

        # Debug print statement
        print("Filters column before extraction:")
        print(data['Filters'].head())

        # Process the data
        data[['param1', 'param2']] = data['Filters'].apply(
            lambda x: pd.Series(extract_params(x))
        )

        # Debug print statement
        print("Extracted param1 and param2 values:")
        print(data[['param1', 'param2']].head())

        # Debug print statement
        print("DataFrame columns before sampling:")
        print(data.columns)

        # Apply sampling if requested
        data = sample_data(data, sampling_method, target_sample_size)

        # Ensure param1 and param2 columns are retained
        data = data[['Query ID', 'Node Types', 'Filters', 'Execution Time',
                     'Cardinality e/a', 'param1', 'param2']]

        # Debug print statement
        print("DataFrame columns after sampling:")
        print(data.columns)

        # Process node types and cardinalities
        data['Processed_Node_Types'] = data['Node Types'].apply(
            process_node_types)
        data['cardinalities'] = data['Cardinality e/a'].apply(
            extract_cardinalities)

        # Get unique parameter combinations
        param_combinations = data[['param1', 'param2']].drop_duplicates()

        # Debug print statement
        print("Unique parameter combinations:")
        print(param_combinations.head())

        # Print the number of stacks
        print(
            f"Number of stacks for {sampling_method} sampling: {len(param_combinations)}")

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

        # Setup the plot
        plt.figure(figsize=(20, 12))
        plt.subplots_adjust(bottom=0.2)
        # Get unique node types for coloring
        all_node_types = [node for sublist in data['Processed_Node_Types']
                          for node in sublist]
        unique_node_types = list(dict.fromkeys(all_node_types))
        colors = plt.cm.tab20(np.linspace(0, 1, len(unique_node_types)))
        color_map = dict(zip(unique_node_types, colors))

        # Create bars
        x = np.arange(len(param_combinations_with_whitespace))
        bottom = np.zeros(len(param_combinations_with_whitespace))

        # Plot each node type's cardinalities
        first_query_nodes = data['Processed_Node_Types'].iloc[0]
        for node_type in first_query_nodes:
            values = [row[i] for row, nodes in zip(data['cardinalities'], data['Processed_Node_Types'])
                      for i, n in enumerate(nodes) if n == node_type]
            values_with_whitespace = np.zeros(
                len(param_combinations_with_whitespace))
            values_with_whitespace[:len(values)] = values
            plt.bar(x, values_with_whitespace, bottom=bottom,
                    color=color_map[node_type], label=node_type)
            bottom += values_with_whitespace

        # Customize the plot
        # plt.xlabel(f'Parameters 1 {param1_name}, 2 {param2_name}', fontsize=20)
        plt.ylabel('Cardinality', fontsize=20)
        plt.yticks(fontsize=16)

        # Identify the indices of the first and last occurrences of each unique param1 value
        unique_param2 = param_combinations['param2'].unique()
        label_indices = []
        for param2 in unique_param2:
            indices = param_combinations.index[param_combinations['param2'] == param2].tolist(
            )
            if indices:
                label_indices.append(indices[0])  # First occurrence
                label_indices.append(indices[-1])  # Last occurrence

        # Set x-axis labels at the identified indices
        plt.xticks(x[label_indices],
                   [f'2 {param_combinations["param1"][i]}' for i in label_indices],
                   rotation=45, ha='right', fontsize=16)
        # Set the x-axis limit to zoom in on the left
        x_max = x.max() / 4.55
        plt.xlim(-0.5, x_max)

        # Add grid and legend
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)

        # Remove underscores from legend labels
        handles, labels = plt.gca().get_legend_handles_labels()
        labels = [label.replace('_', ' ') for label in labels]
        plt.legend(handles, labels, title='Operators', bbox_to_anchor=(
            1.05, 1), loc='upper left', borderaxespad=0., fontsize=18)

        # Format y-axis with scientific notation
        plt.gca().yaxis.set_major_formatter(ticker.ScalarFormatter(useMathText=True))
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
        plt.gca().yaxis.get_offset_text().set_fontsize(14)
        # Adjust layout
        plt.tight_layout()

        # Save or show the plot
        if output_file:
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            plt.close()
        else:
            plt.show()

    except Exception as e:
        print(f"Error creating plot: {str(e)}")


if __name__ == "__main__":
    try:
        # Update with your path
        csv_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/csvs/plottable/q3.csv'
        data = pd.read_csv(csv_path)
        qid = 'q3'

        # Print the number of rows in the CSV
        print(f"Number of rows in the CSV: {len(data)}")

        # Create plots with different sampling methods
        create_stacked_bar_chart(
            data,
            'l_shipdate',
            'o_orderdate',
            sampling_method='none',
            target_sample_size=150,
            output_file=f'/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/cardinality/stack_bar/tpch/pdf/new/{qid}.pdf',
            whitespace_width=1  # Adjust the whitespace width as needed
        )

        create_stacked_bar_chart(
            data,
            'l_shipdate',
            'o_orderdate',
            sampling_method='systematic',
            target_sample_size=150,
            output_file=f'/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/cardinality/stack_bar/tpch/pdf/new/{qid}_sampled.pdf',
            whitespace_width=1  # Adjust the whitespace width as needed
        )
    except Exception as e:
        print(f"Error loading data: {str(e)}")
