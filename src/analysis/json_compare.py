import json 
import os
from deepdiff import DeepDiff

# dir of query plan json files
dir = 'results/tpc/qplans'

def compare_json_files(file1_path, file2_path):
    with open(file1_path, encoding='UTF-8', mode='r') as file1, open(file1_path,
            encoding='UTF-8', mode='r') as file2:
        json_file1 =json.load(file1)
        json_file2 =json.load(file2)
    
    diff = DeepDiff(json_file1, json_file2, verbose_level=1)
    return diff if diff else None

def summ_diff(diff):
    # for summarizing verbose outputs
    summary = {}
    return summary

def group_files_by_query(dir):
    query_groups = {}

    for filename in os.listdir(dir):
        if filename.endswith('.json'):
            query_id = filename.split('p')[0]

            if query_id not in query_groups:
                query_groups[query_id] = []
            query_groups[query_id].append(filename)
    
    return query_groups 



# usage 
