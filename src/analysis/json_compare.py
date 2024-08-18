import json 
import os
from deepdiff import DeepDiff

# dir of query plan json files
dir = 'results/tpch/qplans'

def compare_json_files(json_file1, json_file2):
    diff = DeepDiff(json_file1, json_file2, verbose_level=1)
    return diff if diff else None

def summ_diff(diff):
    # for summarizing verbose outputs
    summary = {}
    return summary

def group_files_by_query(dir, target_query):
    query_groups = {}

    for filename in os.listdir(dir):
        if filename.endswith('.json') and filename.startswith(target_query):
            query_id = filename.split('p')[0]

            if query_id not in query_groups:
                query_groups[query_id] = []
            query_groups[query_id].append(filename)
    
    return query_groups 

def process_json_files(dir, target_query):
    query_groups = group_files_by_query(dir, target_query)
    results = {}
    json_contents = {}

    if target_query in query_groups:
        files = query_groups[target_query]
        for filename in files:
            with open(os.path.join(dir, filename), encoding='UTF-8') as f:
                json_contents[filename]=json.load(f)

    for i in range(len(files)):
        for j in range(i + 1, len(files)):
            file1 = files[i] 
            file2 = files[j]
            diff = compare_json_files(json_contents[file1], json_contents[file2])

            if diff:
                results[(target_query, file1, file2)] = diff
    return results

# usage 
# state target query
target_query = 'q2'
comparison_results = process_json_files(dir, target_query)

for (query_id, file1, file2), diff in comparison_results.items():
    print(f'Comparison for {query_id} between {file1} and {file2}:')
    print(diff)
    print('\n')