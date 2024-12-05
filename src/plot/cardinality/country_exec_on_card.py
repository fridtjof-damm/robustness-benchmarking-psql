import pandas as pd
import matplotlib.pyplot as plt
import ast

# Load the CSV data
file_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified/country_extended_example.csv'
df = pd.read_csv(file_path)

# Parse the Cardinalities and calculate cumulative actual cardinalities


def parse_cardinality(cardinality_str):
    try:
        # Convert the string representation of tuples to a list of tuples
        tuples = ast.literal_eval(f"[{cardinality_str}]")
        # Extract the actual cardinalities
        actual_cardinalities = [act for est, act in tuples]
        # Return the cumulative actual cardinality
        return sum(actual_cardinalities)
    except:
        return 0


df['cumulative_actual_cardinality'] = df['Cardinality e/a'].apply(
    parse_cardinality)

# Calculate the 95th percentile for cumulative actual cardinalities and execution times
cardinality_95th_percentile = df['cumulative_actual_cardinality'].quantile(
    0.95)
execution_time_95th_percentile = df['Execution Time'].quantile(0.95)

# Filter the DataFrame to include only data points below the 95th percentile
df_clipped = df[(df['cumulative_actual_cardinality'] <= cardinality_95th_percentile) &
                (df['Execution Time'] <= execution_time_95th_percentile)]

# Create the scatter plot
plt.figure(figsize=(10, 6))
plt.scatter(df_clipped['cumulative_actual_cardinality'],
            df_clipped['Execution Time'], color='blue', alpha=0.6)

# Customize the plot
plt.xlabel('Cumulative Actual Cardinalities')
plt.ylabel('Execution Time')
plt.title(
    'Execution Time vs. Cumulative Actual Cardinalities')
plt.grid(True)
plt.tight_layout()

# Show the plot
plt.show()
