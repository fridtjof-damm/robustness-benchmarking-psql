import json 
import os
import time
from deepdiff import DeepDiff

query_ids = [i for i in range(1,23) if i != 15]

# for single query comparison , change func call!
# target query format: 'q1' ,etc...
target_query = 'q9'
base_dir = 'results/tpch/qplans/'
dir = os.path.join(base_dir, target_query)



def compare_json_files(json_file1, json_file2):
    diff = DeepDiff(json_file1, json_file2, verbose_level=1)
    return diff if diff else None

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
                results[(file1, file2)] = diff
    return results

def print_results(comparison_results):
    count = 0
    if comparison_results:
        for (file1,file2), diff in comparison_results.items():
            count += 1
            print(f'Comparison between {file1} and {file2}:')
            print(f"Found differences: {diff}")
            print("Comparison success")
            print(f"="*50)
            print(f"Count: {count}")

  
    else: 
        print(f"="*50) 
        print(f"No differences found for Query {target_query[1:]}.")
        print(f"="*50) 
        

print_results(process_json_files(dir))

def compare_2_plans():
    # paths
    path1 = 'results/tpch/qplans/q9/q9_p27.json'
    path2 = 'results/tpch/qplans/q9/q9_p31.json'
    with open(path1, encoding='UTF-8', mode='r') as f1, open(path2, encoding='UTF-8', mode='r') as f2:   
        json_file1 = json.load(f1)
        json_file2 = json.load(f2)
        diff = DeepDiff(json_file1, json_file2, verbose_level=1)
        print(diff)

#compare_2_plans()

