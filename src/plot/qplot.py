import os
import glob
import pandas as pd
import src.plot.cardinality.stack_plotv2 as sp
from src.qgen import tpch_query_parameters
from src.utils.utils import process_csv_and_discard_equals
# module to manage the plotting

# CARDINALITIES

# 1. stack plot
# 1.1 tpch
if __name__ == "__main__":
    try:
        # Directory containing CSV files
        csv_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/'
        output_dir = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/plots/cardinality/stack_bar/tpch/pdf/new'
        # Process all CSV files
        for csv_file in glob.glob(os.path.join(csv_dir, '*.csv')):
            query_id = os.path.basename(csv_file).split('.')[0]

            if query_id not in tpch_query_parameters:
                print(f"Skipping {query_id}: parameters not defined")
                continue
            process_csv_and_discard_equals(
                csv_file, csv_file, tpch_query_parameters)
            data = pd.read_csv(csv_file)
            param1_name, param2_name = tpch_query_parameters[query_id]

            sp.create_stacked_bar_chart(
                data.copy(),
                param1_name,
                param2_name,
                query_id,
                sampling_method='none',
                output_dir=output_dir
            )
            if len(data) > 100:
                sp.create_stacked_bar_chart(
                    data.copy(),
                    param1_name,
                    param2_name,
                    query_id,
                    sampling_method='systematic',
                    target_sample_size=100,
                    output_dir=output_dir
                )

    except Exception as e:
        print(f"Error processing files: {str(e)}")

# job


# country


# EXECUTION TIMES

# tpch


# job


# country


# PLAN DIAGRAMS
