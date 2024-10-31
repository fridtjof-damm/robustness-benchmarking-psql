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

def summarize_diff(diff, show_full_paths=True, include_added_removed=True) -> str:
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
    if include_added_removed:
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
    # set scope of comparison to only changed values
    include_added_removed = False
    # using the job query plans
    folder = 'results/job/qplans_focus_filter'
    # create a logs folder if it does not exist
    logs_folder = 'results/job/logs_focus_filter'
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
            exit()

    for number, group in sorted_groups:
        print(f"\nComparing plans for groups {number}:")
        group_summaries = []
        signif = 0
        not_signif = 0 
        for file1, file2 in combinations(group, 2):
            plan1 = load_json(os.path.join(folder, file1))
            plan2 = load_json(os.path.join(folder, file2))
            diff = compare_plans(plan1, plan2)
            summary = summarize_diff(diff, show_full_paths, include_added_removed)
            
            # Accumulate summaries if there are significant differences
            if summary and summary.strip() != "No significant differences found.":
                signif += 1
                group_summaries.append(f"Comparison between {file1} and {file2}:\n{summary}\n")
            else:
                not_signif += 1

        # Write to log file only if there are significant differences
        if group_summaries:
            log_filename = f"comparison_{timestamp}_{number}.log"
            log_path = os.path.join(logs_folder, log_filename)

            with open(log_path, 'w', encoding='UTF-8') as log_file:
                log_file.write(f"Comparison performed on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_file.write(f"Show full paths: {show_full_paths}\n")
                log_file.write(f"Include added/removed items: {include_added_removed}\n")
                log_file.write("--- Begin Summary ---\n")
                for summary in group_summaries:
                    log_file.write(summary)
                log_file.write("--- End Summary ---\n")

        print(f"Successfully saved query plan difference logs for group {number}.")
        print(f"With {not_signif} insignificant and {signif} significant differences out of {not_signif + signif}.")

if __name__ == '__main__':
    main()