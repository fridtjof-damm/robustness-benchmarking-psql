import os
import json
import re
import matplotlib.pyplot as plt
import numpy as np
from typing import Dict, Union, Tuple, List
from matplotlib.colors import ListedColormap
from matplotlib.patches import Polygon


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
                if plan['Node Type'] not in ['Limit', 'Gather']:
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

    # Sort each category by query ID
    for category in categories:
        category.sort()

    # Sort categories by the query ID of the first plan in each category
    categories.sort(key=lambda x: x[0])

    return categories


def extract_filters(plan: Dict) -> Tuple[str, str]:
    param1 = None
    param2 = None
    if 'Filters' in plan:
        filters = plan['Filters']
        for filter_str in filters:
            if 'param1' in filter_str:
                match = re.search(r'param1,(\w+)', filter_str)
                if match:
                    param1 = match.group(1)
            if 'param2' in filter_str:
                match = re.search(r'param2,(\w+)', filter_str)
                if match:
                    param2 = match.group(1)
    return param1, param2


def plot_heatmap(query_info: Dict[Union[int, Tuple[int, str]], Tuple[List[str], int]], plan_categories: List[List[Union[int, Tuple[int, str]]]], directory: str) -> None:
    # Extract parameters and categories
    param1_values = set()
    param2_values = set()
    query_filters = {}

    for query_id in query_info.keys():
        with open(os.path.join(directory, f"{query_id}.json"), 'r', encoding='UTF-8') as file:
            plan = json.load(file)
            param1, param2 = extract_filters(plan)
            if param1 is not None and param2 is not None:
                param1_values.add(param1)
                param2_values.add(param2)
                query_filters[query_id] = (param1, param2)
            else:
                # Debug print statement
                print(f"Filters not found for query ID {query_id}: {plan}")

    # Sort parameter values
    param1_values = sorted(param1_values)
    param2_values = sorted(param2_values)

    # Debug print statements
    print(f"Param1 values: {param1_values}")
    print(f"Param2 values: {param2_values}")
    print(f"Query filters: {query_filters}")

    # Create a matrix for the heatmap
    heatmap_data = np.zeros((len(param2_values), len(param1_values)))

    # Fill the matrix with the category index
    for category_index, category in enumerate(plan_categories):
        for query_id in category:
            if query_id in query_filters:
                param1, param2 = query_filters[query_id]
                row = param2_values.index(param2)
                col = param1_values.index(param1)
                heatmap_data[row, col] = category_index + \
                    1  # Use category index + 1 for coloring

    # Debug print statement
    print(f"Heatmap data:\n{heatmap_data}")

    # Create a custom colormap
    num_categories = len(plan_categories)
    colors = plt.colormaps['tab20'](np.linspace(0, 1, num_categories))
    cmap = ListedColormap(colors)

    # Plot the heatmap with polygons
    fig, ax = plt.subplots(figsize=(12, 8))
    for row in range(len(param2_values)):
        for col in range(len(param1_values)):
            category_index = int(heatmap_data[row, col]) - 1
            if category_index >= 0:
                color = colors[category_index]
                polygon = Polygon([(col, row), (col + 1, row), (col + 1,
                                  row + 1), (col, row + 1)], closed=True, color=color)
                ax.add_patch(polygon)

    ax.set_xlim(0, len(param1_values))
    ax.set_ylim(0, len(param2_values))
    ax.set_xticks(np.arange(len(param1_values)) + 0.5)
    ax.set_xticklabels(param1_values, rotation=45, ha='right')
    ax.set_yticks(np.arange(len(param2_values)) + 0.5)
    ax.set_yticklabels(param2_values)
    ax.set_xlabel('Param1')
    ax.set_ylabel('Param2')
    ax.set_title('Query Plan Categories Heatmap')

    # Create a colorbar
    cbar = plt.colorbar(plt.cm.ScalarMappable(cmap=cmap),
                        ax=ax, ticks=range(1, num_categories + 1))
    cbar.set_label('Category')

    plt.show()


# Directory containing the query plans
directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/15a'

# Collect the node types and depth of the plans
query_info = info_qplan(directory)

# Debug print statements
print(f"Collected query info: {query_info}")

# Categorize the plans
plan_categories = categorize_plans(query_info)

# Debug print statements
print(f"Categorized plans: {plan_categories}")

# Plot the heatmap
plot_heatmap(query_info, plan_categories, directory)
