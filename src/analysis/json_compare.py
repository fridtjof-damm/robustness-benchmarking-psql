import json 
import os
from deepdiff import DeepDiff

# state target query, example Query 2: 'q2'
target_query = 'q2'
query_ids = [i for i in range(1,23) if i != 15]

# dir of query plan json files
base_dir = 'results/tpch/qplans/'
dir = os.path.join(base_dir, target_query)

def compare_json_files(json_file1, json_file2):
    diff = DeepDiff(json_file1, json_file2, verbose_level=1)
    return diff if diff else None

def summ_diff(diff):
    # for summarizing verbose outputs
    summary = {}
    return summary

def process_json_files(dir):
    results = {}
    json_contents = {}

    # list all filenames 
    files = []
    for filename in os.listdir(dir):
        if filename.endswith('.json'):
            files.append(filename)
    # load json files 
    for filename in files:
        filepath = os.path.join(dir, filename)
        with open(filepath, encoding='UTF-8', mode='r') as f:
            json_contents[filename] = json.load(f)
            print("Success loading JSON file: ", filename)
    # compare two files 
    for i in range(0, len(files) - 1, 2):
            file1 = files[i] 
            file2 = files[i + 1]
            print("Comparing", file1, "with", file2)
            diff = compare_json_files(json_contents[file1], json_contents[file2])
            if diff:
                results[(target_query, file1, file2)] = diff
    return results

def print_results(comparison_results):
    total_differences = 0
    difference_types = {}

    for (file1,file2), diff in comparison_results.items():
        print(f'Comparison between {file1} and {file2}:')

        for change_type, changes in diff.items():
            for change in changes:
                total_differences += 1
                key_path = change['path']
                key_type = key_path.split('.')[-1].strip("[]'")
                value = change['value']

                print(f"  -  {key_type}: {value}")

                if key_type not in difference_types:
                    difference_types[key_type] = 0
                difference_types[key_type] += 1
        print('\n')

    print(f'Total differences: {total_differences}')
    print('Difference types:')
    for key_type, count in difference_types.items():
        print(f'  - {key_type}: {count}')

def process_all_queries(base_dir, query_ids):
    for query_id in query_ids:
        print(f"Processing query ID: q{query_id}")
        dir = os.path.join(base_dir)
        comparison_results = process_json_files(dir)

        print_results(comparison_results)
        print("\n" + "="*50 + "\n")

# mock data 
mock_comparison_results = {
    ('file1.json', 'file2.json'): {
        'type_changes': [
            {'path': "root['Plans'][0]['Filter']", 'value': 'old_value1'},
            {'path': "root['Plans'][1]['JoinType']", 'value': 'old_value2'}
        ],
        'value_changes': [
            {'path': "root['Plans'][0]['Filter']", 'value': 'new_value1'},
            {'path': "root['Plans'][1]['JoinType']", 'value': 'new_value2'}
        ]
    },
    ('file3.json', 'file4.json'): {
        'type_changes': [
            {'path': "root['Plans'][2]['Node']", 'value': 'old_value3'}
        ],
        'value_changes': [
            {'path': "root['Plans'][2]['Node']", 'value': 'new_value3'}
        ]
    }
}    
# usage 
process_all_queries(base_dir,)


