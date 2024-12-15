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


def create_stacked_bar_chart(data, param1_name, param2_name, output_file=None):
    try:
        # Process node types
        data['Processed_Node_Types'] = data['Node Types'].apply(
            process_node_types)

        # Extract parameters from filters
        def extract_params(filter_str):
            pattern = r'\(([\w_]+),\s*([^)]+)\)'
            matches = re.findall(pattern, filter_str)
            date_values = [value.strip() for name, value in matches
                           if name in [param1_name, param2_name]]
            return date_values[0], date_values[1]

        # Process the data
        data[['param1', 'param2']] = data['Filters'].apply(
            lambda x: pd.Series(extract_params(x))
        )

        # Get cardinalities for each query
        data['cardinalities'] = data['Cardinality e/a'].apply(
            extract_cardinalities)

        # Get unique parameter combinations
        param_combinations = data[['param1', 'param2']].drop_duplicates()

        # Setup the plot
        plt.figure(figsize=(15, 10))

        # Get unique node types for coloring
        unique_node_types = list(set([item for sublist in data['Processed_Node_Types']
                                      for item in sublist]))
        colors = plt.cm.tab20(np.linspace(0, 1, len(unique_node_types)))
        color_map = dict(zip(unique_node_types, colors))

        # Create bars for each parameter combination
        x = np.arange(len(param_combinations))
        bottom = np.zeros(len(param_combinations))

        # Get cardinalities for each layer while maintaining order
        first_query_nodes = data['Processed_Node_Types'].iloc[0]
        for node_type in first_query_nodes:
            values = [row[i] for row, nodes in zip(data['cardinalities'],
                                                   data['Processed_Node_Types'])
                      for i, n in enumerate(nodes) if n == node_type]
            plt.bar(x, values, bottom=bottom,
                    color=color_map[node_type],
                    label=node_type)
            bottom += values

        # Customize the plot
        plt.title('Query Plan Cardinalities', fontsize=16, pad=20)
        plt.xlabel(f'Parameters ({param1_name}, {param2_name})', fontsize=12)
        plt.ylabel('Cardinality', fontsize=12)

        # Set x-axis labels with reduced frequency
        num_labels = min(8, len(param_combinations))
        step = max(1, len(param_combinations) // num_labels)
        plt.xticks(x[::step],
                   [f'{p1}\n{p2}' for p1, p2 in
                   zip(param_combinations['param1'][::step],
                       param_combinations['param2'][::step])],
                   rotation=45, ha='right')

        # Add grid and legend
        plt.grid(True, axis='y', linestyle='--', alpha=0.7)
        plt.legend(title='Operators', bbox_to_anchor=(1.05, 1),
                   loc='upper left', borderaxespad=0.)

        # Format y-axis with scientific notation
        plt.gca().yaxis.set_major_formatter(
            ticker.ScalarFormatter(useMathText=True)
        )
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))

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


# Example usage
if __name__ == "__main__":
    try:
        # Update with your path
        csv_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/q3.csv'
        data = pd.read_csv(csv_path)
        create_stacked_bar_chart(
            data,
            'l_shipdate',
            'o_orderdate',
            'query_cardinalities.png'
        )
    except Exception as e:
        print(f"Error loading data: {str(e)}")
