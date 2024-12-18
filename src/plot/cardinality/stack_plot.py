import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import re
import numpy as np
import os
from src.utils.utils import extract_params, extract_relevant_filters, sample_data, extract_cardinalities, process_csv_and_discard_equals, process_node_types


def create_stacked_bar_chart(data, param1_name, param2_name, query_id, sampling_method='none',
                             target_sample_size=150, output_dir='results/plots/'):
    try:
        # Process the data
        data['param1'], data['param2'] = zip(
            *[extract_relevant_filters(x, param1_name, param2_name) for x in data['Filters']])

        # Apply sampling if needed, function in utils.py
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

        # Plot each node type's cardinalities in order
        for node_type in all_node_types:
            values = np.zeros(len(param_combinations))
            for i, (_, row) in enumerate(param_combinations.iterrows()):
                matching_rows = data[
                    (data['param1'] == row['param1']) &
                    (data['param2'] == row['param2'])
                ]
                if not matching_rows.empty:
                    first_match = matching_rows.iloc[0]
                    if node_type in first_match['Processed_Node_Types']:
                        node_idx = first_match['Processed_Node_Types'].index(
                            node_type)
                        values[i] = first_match['cardinalities'][node_idx]

            plt.bar(x, values, bottom=bottom,
                    color=color_map[node_type],
                    label=node_type)
            bottom += values

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
                   zip(param_combinations['param1'].iloc[::step],
                       param_combinations['param2'].iloc[::step])],
                   rotation=45, ha='right')

        # Add grid and legend
        # plt.grid(True, axis='y', linestyle='--', alpha=0.7)
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
