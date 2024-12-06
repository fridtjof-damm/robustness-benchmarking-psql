import pandas as pd
import matplotlib.pyplot as plt
import ast
import re
import numpy as np

# Load the CSV data
file_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified/country_extended_example_update.csv'
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

# Clip data to the 99th percentile
percentile_99 = df['cumulative_actual_cardinality'].quantile(0.99)
df_clipped = df[df['cumulative_actual_cardinality'] <= percentile_99]

# Create the plot
plt.figure(figsize=(10, 6))

# Scatter plot for clipped data points
plt.scatter(df_clipped['cumulative_actual_cardinality'],
            df_clipped['Execution Time'], color='blue', alpha=0.6)
plt.xlabel('Cumulative Actual Cardinality')
plt.ylabel('Execution Time (s)')
plt.title('Scatter Plot of Execution Time vs. Cumulative Actual Cardinality (Clipped to 99th Percentile)')
plt.grid(True)

# Annotate the points


def annotate_point(index, df, ax, color='black', xytext=(0, 15)):
    row = df.iloc[index]
    # Extract the country name from the Filters column using regex
    match = re.search(r'\(name,([^)]+)\)', row['Filters'])
    country = match.group(1) if match else '<Country>'
    annotation = (f"{row['Node Types']}\n{country}\n"
                  f"Cumulated Card: {row['cumulative_actual_cardinality']}\n"
                  f"Exec Time: {row['Execution Time']:.2f} s")
    ax.annotate(annotation, (row['cumulative_actual_cardinality'], row['Execution Time']),
                textcoords="offset points", xytext=xytext, ha='center', fontsize=8, color=color, bbox=dict(facecolor='white', alpha=0.5))
    print(
        f"Annotated point: {annotation} at ({row['cumulative_actual_cardinality']}, {row['Execution Time']})")


# Annotate the lowest value
lowest_index = df_clipped['Execution Time'].idxmin()
print(
    f"Lowest value index: {lowest_index}, Execution Time: {df_clipped.loc[lowest_index, 'Execution Time']}")
annotate_point(lowest_index, df_clipped, plt.gca(), xytext=(0, 15))

# Annotate the first big peak
peak_index = df_clipped['Execution Time'].idxmax()
print(
    f"First big peak index: {peak_index}, Execution Time: {df_clipped.loc[peak_index, 'Execution Time']}")
annotate_point(peak_index, df_clipped, plt.gca(), xytext=(0, 30))

# Annotate the local minimum after the first peak
if peak_index + 1 < len(df_clipped):
    local_min_index = df_clipped.iloc[peak_index +
                                      1:].idxmin()['Execution Time']
    print(
        f"Local minimum index after peak: {local_min_index}, Execution Time: {df_clipped.loc[local_min_index, 'Execution Time']}")
    annotate_point(local_min_index, df_clipped, plt.gca(), xytext=(0, 45))

# Annotate the two highest cumulative cardinalities with more spacing
highest_cardinality_indices = df_clipped['cumulative_actual_cardinality'].nlargest(
    2).index
for i, index in enumerate(highest_cardinality_indices):
    print(
        f"High cardinality index: {index}, Cumulative Actual Cardinality: {df_clipped.loc[index, 'cumulative_actual_cardinality']}")
    annotate_point(index, df_clipped, plt.gca(), xytext=(0, 5 + i * 30))

plt.tight_layout()
plt.show()
