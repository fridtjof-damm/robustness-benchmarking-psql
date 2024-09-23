import json 
import os
import sys
from itertools import combinations
from deepdiff import DeepDiff
from src.utils.utils import extract_number
from datetime import datetime

def load_json(file_path) -> dict:
    """
    Load a JSON file from the given path.
    """
    with open(file_path, encoding='UTF-8', mode='r') as f:
        return json.load(f)

def compare_plans(plan1, plan2) -> dict:
    """
    Compare two query plans and return the differences.
    """
    diff = DeepDiff(plan1, plan2, verbose_level=2)
    return diff

def extract_key(path):
    """
    Extract the key from a path.
    """
    return path.split("']")[-2].split("['")[-1]

def summarize_diff(diff, show_full_paths=True) -> str:
    """
    Summarize the differences between two query plans.
    """
    summary = []
    if 'values_changed' in diff:
        for path, change in diff['values_changed'].items():
            key = path if show_full_paths else extract_key(path)
            summary.append((path,f"Changed: {key}\nOld: {change['old_value']}\nNew: {change['new_value']}\n"))
    if 'type_changes' in diff:
        for path, change in diff['type_changes'].items():
            key = path if show_full_paths else extract_key(path)
            summary.append((path,f"Type changed: {key}\nOld: {change['old_type']} {change['old_value']}\nNew: {change['new_type']} {change['new_value']}\n"))
    if 'dictionary_item_added' in diff:
        for item in diff['dictionary_item_added']:
            key = item if show_full_paths else extract_key(item)
            summary.append((item,f"Added: {key}\n"))
    if 'dictionary_item_removed' in diff:
        for item in diff['dictionary_item_removed']:
            key = item if show_full_paths else extract_key(item)
            summary.append((item,f"Removed: {key}\n"))
    summary.sort(key=lambda x: x[0])

    return "\n".join(change for _, change in summary)

def main() -> None:
    """ 
    Compare query plans for the same query from different runs.
    """
    # exact query number or None to compare all queries
    query_number = None
    # show the full paths 
    show_full_paths = True
    # using the job query plans
    folder = 'results/job/qplans'
    # create a logs folder if it does not exist
    logs_folder = 'results/job/logs'
    os.makedirs(logs_folder, exist_ok=True)

    # get timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") 


    files = [f for f in os.listdir(folder) if f.endswith('.json')]

    # comparing plans in groups of their prefix
    # group files by prefix
    groups = {}
    for file in files:
        number, suffix = extract_number(file)
        if number not in groups:
            groups[number] = []
        groups[number].append(file)

    sorted_groups = sorted(groups.items())

    if query_number is not None:
        sorted_groups = [(num, group) for num, group in sorted_groups if num == query_number]
        if not sorted_groups:
            print(f"No plans found for query {query_number}.")
            return  

    for number, group in sorted_groups:
        print(f"\nComparing plans for groups {number}:")
        for file1, file2 in  combinations(group, 2):
            plan1 = load_json(os.path.join(folder, file1))
            plan2 = load_json(os.path.join(folder, file2))
            diff = compare_plans(plan1, plan2)
            summary = summarize_diff(diff, show_full_paths)

            # write to log file
            log_filename = f"comparison_{timestamp}_{number}_{file1.split('.')[0]}_{file2.split('.')[0]}.log"
            log_path = os.path.join(logs_folder, log_filename)

            with open(log_path, 'w', encoding='UTF-8') as log_file:
                log_file.write(f"Comparison performed on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_file.write(f"Comparison between {file1} and {file2}:\n")
                log_file.write(summary if summary else "No significant differences found.")

            print(f"Comparison between {file1} and {file2}:")
            print(summary if summary else "No significant differences found.")

if __name__ == '__main__':
    main()
 