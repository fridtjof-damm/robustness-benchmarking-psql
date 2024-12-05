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

# Create the scatter plot
plt.figure(figsize=(10, 6))
plt.scatter(df['cumulative_actual_cardinality'],
            df['Execution Time'], color='blue', alpha=0.6)

# Annotate the points


def annotate_point(index, df, ax, color='black'):
    row = df.iloc[index]
    # Extract the country name from the Filters column
    filters = row['Filters'].replace(')', '').split(',')[1].strip()
    annotation = (f"{row['Node Types']}\n{filters}\n"
                  f"Cumulated Card: {row['cumulative_actual_cardinality']}\n"
                  f"Exec Time: {row['Execution Time']:.2f} s")
    ax.annotate(annotation, (row['cumulative_actual_cardinality'], row['Execution Time']),
                textcoords="offset points", xytext=(0, 15), ha='center', fontsize=8, color=color, bbox=dict(facecolor='white', alpha=0.5))
    print(
        f"Annotated point: {annotation} at ({row['cumulative_actual_cardinality']}, {row['Execution Time']})")


# Annotate the first three points
for i in range(1):
    annotate_point(i, df, plt.gca())

# Annotate the first big peak
peak_index = df['Execution Time'].idxmax()
print(
    f"First big peak index: {peak_index}, Execution Time: {df.loc[peak_index, 'Execution Time']}")
annotate_point(peak_index, df, plt.gca())

# Annotate the local minimum after the peak
if peak_index + 1 < len(df):
    local_min_index = df.iloc[peak_index + 1:].idxmin()['Execution Time']
    print(
        f"Local minimum index after peak: {local_min_index}, Execution Time: {df.loc[local_min_index, 'Execution Time']}")
    annotate_point(local_min_index, df, plt.gca())

# Annotate the last three points
for i in range(-3, 0):
    annotate_point(i, df, plt.gca())

# Annotate points where filters contain exactly "India" or "China"
for index, row in df.iterrows():
    if '(country,India)' in row['Filters'] or '(country,China)' in row['Filters']:
        annotate_point(index, df, plt.gca())

# Customize the plot
plt.xlabel('Cumulative Actual Cardinalities')
plt.ylabel('Execution Time')
plt.title('Execution Time vs. Cumulative Actual Cardinalities')
plt.grid(True)
plt.tight_layout()

# Show the plot
plt.show()
