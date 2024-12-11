import os
import json
from typing import Dict, Union, Tuple, List
import re


def extract_number(filename) -> Tuple[int, str]:
    match = re.match(r'(\d+)([a-zA-Z]*)', filename)
    if match:
        number = int(match.group(1))
        suffix = match.group(2)
        return (number, suffix)
    return (float('inf'), '')


def extract_node_types_and_depth_from_plan(plan: Dict) -> Tuple[List[str], int]:
    node_types = []
    # Use a list to allow modification within the traverse function
    max_depth = [0]

    def traverse(plan: Dict, depth: int) -> None:
        if isinstance(plan, list):
            for item in plan:
                traverse(item, depth)
        elif isinstance(plan, dict):
            if 'Node Type' in plan:
                if plan['Node Type'] not in ['Limit', 'Gather']:
                    node_types.append(plan['Node Type'])
            if 'Plans' in plan:
                for subplan in plan['Plans']:
                    traverse(subplan, depth + 1)
            if 'Plan' in plan:
                traverse(plan['Plan'], depth + 1)
            max_depth[0] = max(max_depth[0], depth)

    traverse(plan, 1)
    return node_types, max_depth[0]


def info_qplan_compare(directory: str) -> Dict[Union[int, Tuple[int, str]], Tuple[List[str], int]]:
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


# Directory containing the query plans
directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/fd/country_example_plans_simplified'

# Collect the node types and depth of the plans
query_info = info_qplan_compare(directory)

# Print the results
print(query_info)
for query_id, (node_types, depth) in query_info.items():
    print(f"Query ID: {query_id}")
    print(f"Node Types: {node_types}")
    print(f"Depth: {depth}")
    print()
