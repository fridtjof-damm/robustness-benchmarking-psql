import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import re
import numpy as np
import os
import glob


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
    if len(data) <= 100:  # Don't sample if less than 100 rows
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


def create_stacked_bar_chart(data, param1_name, param2_name, query_id, sampling_method='none',
                             target_sample_size=150, output_dir='results/plots/'):
    try:
        # Extract parameters from filters
        def extract_params(filter_str):
            pattern = r'\(([\w_]+),\s*([^)]+)\)'
            matches = re.findall(pattern, filter_str)
            date_values = [value.strip() for name, value in matches
                           if name in [param1_name, param2_name]]
            return date_values[0], date_values[1]

        # Process the data
        data['param1'], data['param2'] = zip(
            *data['Filters'].apply(extract_params))

        # Apply sampling if needed
        data = sample_data(data, sampling_method, target_sample_size)

        # Process node types and cardinalities
        data['Processed_Node_Types'] = data['Node Types'].apply(
            process_node_types)
        data['cardinalities'] = data['Cardinality e/a'].apply(
            extract_cardinalities)

        # Setup the plot
        plt.figure(figsize=(15, 10))

        # Get all node types while preserving order
        all_node_types = []
        for nodes in data['Processed_Node_Types']:
            for node in nodes:
                if node not in all_node_types:
                    all_node_types.append(node)

        # Create color map
        colors = plt.cm.tab20(np.linspace(0, 1, len(all_node_types)))
        color_map = dict(zip(all_node_types, colors))

        # Create bars
        param_combinations = data[['param1', 'param2']].drop_duplicates()
        x = np.arange(len(param_combinations))
        bottom = np.zeros(len(param_combinations))

        # Plot each node type's cardinalities
        for node_type in all_node_types:
            values = []
            for idx, row in data.iterrows():
                if node_type in row['Processed_Node_Types']:
                    node_idx = row['Processed_Node_Types'].index(node_type)
                    values.append(row['cardinalities'][node_idx])
                else:
                    values.append(0)

            plt.bar(x, values, bottom=bottom,
                    color=color_map[node_type],
                    label=node_type)
            bottom += np.array(values)

        # Customize the plot
        sampling_text = f" ({sampling_method} sampling)" if len(
            data) > 100 else ""
        plt.title(f'Query {query_id} Plan Cardinalities{sampling_text}',
                  fontsize=16, pad=20)
        plt.xlabel(f'Parameters ({param1_name}, {param2_name})', fontsize=12)
        plt.ylabel('Cardinality', fontsize=12)

        # Set x-axis labels
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

        # Format y-axis
        plt.gca().yaxis.set_major_formatter(
            ticker.ScalarFormatter(useMathText=True)
        )
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))

        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save the plot
        output_file = os.path.join(
            output_dir, f'{query_id}_cardinalities_{sampling_method}.png')
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()

    except Exception as e:
        print(f"Error creating plot for {query_id}: {str(e)}")


if __name__ == "__main__":
    try:
        # Directory containing CSV files
        csv_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/'
        output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/cardinality/stack_bar/tpch'

        # Process all CSV files
        for csv_file in glob.glob(os.path.join(csv_dir, '*.csv')):
            # Extract query ID from filename
            query_id = os.path.basename(csv_file).split('.')[0]

            # Read data
            data = pd.read_csv(csv_file)

            # Create plots with different sampling methods
            create_stacked_bar_chart(
                data.copy(),
                'l_shipdate',
                'o_orderdate',
                query_id,
                sampling_method='none',
                output_dir=output_dir
            )

            if len(data) > 100:
                create_stacked_bar_chart(
                    data.copy(),
                    'l_shipdate',
                    'o_orderdate',
                    query_id,
                    sampling_method='systematic',
                    target_sample_size=100,
                    output_dir=output_dir
                )

    except Exception as e:
        print(f"Error processing files: {str(e)}")
