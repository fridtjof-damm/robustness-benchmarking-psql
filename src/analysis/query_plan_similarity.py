import os
import json
from typing import Dict, Union, Tuple, List
import re
import csv


def extract_number(filename) -> Tuple[int, str]:
    match = re.match(r'(\d+)([a-zA-Z]*)', filename)
    if match:
        number = int(match.group(1))
        suffix = match.group(2)
        return (number, suffix)
    return (float('inf'), '')


def extract_node_types_and_depth_from_plan(plan: Union[Dict, List]) -> Tuple[List[str], int]:
    node_types = []
    # Use a list to allow modification within the traverse function
    max_depth = [0]

    def traverse(plan: Union[Dict, List], depth: int) -> None:
        if isinstance(plan, list):
            for item in plan:
                traverse(item, depth)
        elif isinstance(plan, dict):
            if 'Node Type' in plan:
                node_types.append(plan['Node Type'])
                max_depth[0] = max(max_depth[0], depth)
            if 'Plans' in plan:
                for subplan in plan['Plans']:
                    traverse(subplan, depth + 1)
            # Only traverse the 'Plan' key if it exists and ignore other keys
            if 'Plan' in plan:
                traverse(plan['Plan'], depth + 1)
            # Skip increasing depth for certain keys
            if 'Triggers' in plan or 'Execution Time' in plan or 'Planning Time' in plan:
                return

    # Start traversal from the top level of the plan
    traverse(plan, 1)
    return node_types, max_depth[0]-1


def info_qplan(directory: str) -> Dict[Union[int, Tuple[int, str]], Tuple[List[str], int]]:
    query_info = {}
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            query_id, suffix = extract_number(filename)
            if query_id == float('inf'):
                try:
                    query_id = int(filename.split('.')[0])
                    suffix = ''
                except ValueError:
                    continue
            with open(os.path.join(directory, filename), 'r', encoding='UTF-8') as file:
                plan = json.load(file)
                node_types, depth = extract_node_types_and_depth_from_plan(
                    plan)
                query_info[(query_id, suffix) if suffix else query_id] = (
                    node_types, depth)
    return query_info


def query_plan_info_to_csv(data: Dict[Union[int, Tuple[int, str]], Tuple[List[str], int]], directory: str, filename: str) -> None:
    filepath = os.path.join(directory, filename)
    with open(filepath, 'w', newline='') as csvfile:
        fieldnames = ['query id', 'node types', 'plan depth']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for query_id, (node_types, depth) in data.items():
            writer.writerow({
                'query id': query_id,
                'node types': ', '.join(node_types),
                'plan depth': depth
            })


def categorize_plans(query_info: Dict[Union[int, Tuple[int, str]], Tuple[List[str], int]]) -> List[List[Union[int, Tuple[int, str]]]]:
    categories = []

    for query_id, (node_types, depth) in query_info.items():
        found = False
        for category in categories:
            # Compare with the first plan in the category
            if query_info[category[0]] == (node_types, depth):
                category.append(query_id)
                found = True
                break
        if not found:
            categories.append([query_id])

    return categories


# Directory containing the query plans
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q2'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q3'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q5'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q7'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q8'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q12'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q13'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q14'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q17'
# directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/qplans'
directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/15a'

# Collect the node types and depth of the plans
query_info = info_qplan(directory)

# Sort the dictionary by depth
sorted_query_info = dict(
    sorted(query_info.items(), key=lambda item: item[1][1]))

# Write the sorted dictionary to a CSV file in the specified directory
csv_filename = 'query_plan_info.csv'
query_plan_info_to_csv(sorted_query_info, directory, csv_filename)

# Categorize the plans
categories = categorize_plans(sorted_query_info)

# print(query_info)
for i, category in enumerate(categories):
    print(f"Category {i + 1}: {category}")
