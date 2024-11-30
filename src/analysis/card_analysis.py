import pandas as pd
import matplotlib.pyplot as plt
from ast import literal_eval
import numpy as np
from collections import Counter

def process_job_data(csv_path):
    df = pd.read_csv(csv_path)
    processed_data = []
    
    for _, row in df.iterrows():
        query_id = literal_eval(row['Query ID'])
        node_types = row['Node Types'].split(', ')
        cardinalities = []
        for pair in row['Cardinality e/a'].split('), ('):
            pair = pair.strip('(').strip(')')
            est, act = map(int, pair.split(','))
            cardinalities.append((est, act))
            
        processed_data.append({
            'query_id': query_id[0],
            'variant': query_id[1],
            'node_types': node_types,
            'cardinalities': cardinalities,
            'execution_time': float(row['Execution Time']) / 1000 if not pd.isna(row['Execution Time']) else 0  # Convert to seconds
        })
    
    return pd.DataFrame(processed_data)

def calc_qerror(query_info) -> dict[int, float]:
    qerrors = {}
    for query_id, (node_types, filters, execution_times, cardinalities) in query_info.items():
        qerror_sum = 0
        valid_cardinalities = 0
        for estimated, actual in cardinalities:
            if actual == 0 or estimated == 0:
                continue
            if actual >= estimated:
                qerror = actual / estimated
            else:
                qerror = estimated / actual
            qerror_sum += qerror
            valid_cardinalities += 1
        if valid_cardinalities > 0:
            qerrors[query_id] = qerror_sum / len(cardinalities)
        else: 
            qerrors[query_id] = 0
    return qerrors

def plot_qerror_steps(df):
    plt.figure(figsize=(15, 8))
    
    all_node_types = []
    all_qerrors = []
    for _, row in df.iterrows():
        cardinalities = row['cardinalities']
        node_types = row['node_types']
        qerrors = []
        for est, act in cardinalities:
            if act == 0 or est == 0:
                qerrors.append(0)
            elif act >= est:
                qerrors.append(act / est)
            else:
                qerrors.append(est / act)
        
        # Calculate the 90th percentile of q-errors
        percentile_90 = np.percentile(qerrors, 90)
        
        # Clip the q-errors at the 90th percentile
        qerrors = np.clip(qerrors, None, percentile_90)
        
        plt.plot(qerrors, label=f"Query {row['query_id']}-{row['variant']}", alpha=0.8)
        all_node_types.append(node_types)
        all_qerrors.append(qerrors)
    
    # Determine the node type of the tuple that caused the highest q-error for each step
    highest_qerror_node_types = []
    for i in range(len(all_node_types[0])):
        max_qerror = 0
        max_node_type = 'N/A'
        for j in range(len(all_qerrors)):
            if i < len(all_qerrors[j]) and all_qerrors[j][i] > max_qerror:
                max_qerror = all_qerrors[j][i]
                max_node_type = all_node_types[j][i]
        highest_qerror_node_types.append(max_node_type)
    
    # Debug print statements
    print("Node types causing the highest q-errors for each step:")
    print(highest_qerror_node_types)
    
    # Customize plot
    plt.xlabel('Node Type')
    plt.ylabel('Q-Error')
    plt.title('Q-Error over Steps (Clipped at 90th Percentile)')
    plt.xticks(ticks=range(len(highest_qerror_node_types)), labels=highest_qerror_node_types, rotation=45)
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15), ncol=3, fontsize='small')
    plt.grid(True, axis='both', linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    return plt.gcf()

# Usage
csv_path = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/all_job.csv'
df = process_job_data(csv_path)
print(df)  # Print the DataFrame to verify the data

# Plot q-error over steps
fig_qerror_steps = plot_qerror_steps(df)
plt.show()