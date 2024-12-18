import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import ast

# Load and prepare the data


def prepare_data(file_path):
    # Read CSV
    df = pd.read_csv(file_path)

    # Debug print statement
    print("Initial DataFrame head:")
    print(df.head())

    # Rename columns for easier handling
    df.columns = ['query_id', 'node_type',
                  'filters', 'exec_time', 'card_ratio']

    # Debug print statement
    print("Unique values in card_ratio before conversion:")
    print(df['card_ratio'].unique())

    # Parse the card_ratio column and calculate the ratio
    def parse_card_ratio(card_ratio_str):
        try:
            # Convert the string representation of tuples to a list of tuples
            tuples = ast.literal_eval(f"[{card_ratio_str}]")
            # Calculate the ratio for each tuple and return the list of ratios
            ratios = [est / act if act != 0 else np.nan for est, act in tuples]
            return np.mean(ratios)  # Return the mean ratio
        except:
            return np.nan

    df['card_ratio'] = df['card_ratio'].apply(parse_card_ratio)

    # Debug print statement
    print("DataFrame after parsing card_ratio:")
    print(df.head())

    # Calculate cardinality error (log of ratio for better visualization)
    df['card_error'] = np.log2(df['card_ratio'])

    # Debug print statement
    print("DataFrame after calculating card_error:")
    print(df.head())

    # Clean node types (assuming they're in a string format)
    df['node_type'] = df['node_type'].str.strip()

    # Drop rows with NaN values in card_error
    df = df.dropna(subset=['card_error'])

    # Debug print statements
    print("DataFrame head after dropping NaN values in card_error:")
    print(df.head())
    print("DataFrame info after dropping NaN values in card_error:")
    print(df.info())
    print("Unique node types after dropping NaN values in card_error:")
    print(df['node_type'].unique())

    return df

# Create cumulative line plot


def plot_cumulative_cardinality_errors(df):
    # Split node types into individual rows
    df = df.assign(node_type=df['node_type'].str.split(
        ', ')).explode('node_type')

    # Sort by execution time
    df = df.sort_values(by='exec_time')

    # Calculate cumulative sum of cardinality errors
    df['cumulative_card_error'] = df.groupby(
        'node_type')['card_error'].cumsum()

    plt.figure(figsize=(15, 8))

    # Create line plot
    sns.lineplot(x='cumulative_card_error', y='exec_time',
                 hue='node_type', data=df, marker='o')

    # Customize plot
    plt.xlabel('Cumulative Cardinality Error (log2)')
    plt.ylabel('Execution Time')
    plt.title('Execution Time by Cumulative Cardinality Errors')
    plt.legend(title='Node Type', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()


# Usage
file_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/all_job.csv'
df = prepare_data(file_path)
plot_cumulative_cardinality_errors(df)
