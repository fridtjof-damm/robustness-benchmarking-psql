import os
import json
import re
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from typing import Dict, Union, Tuple, List


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


def extract_filters(plan: Dict) -> Tuple[str, int]:
    country_code = None
    production_year = None
    if 'Filters' in plan:
        filters = plan['Filters']
        for filter_str in filters:
            if 'production_year' in filter_str:
                match = re.search(r'production_year,(\d+)', filter_str)
                if match:
                    production_year = int(match.group(1))
            if 'country' in filter_str:
                match = re.search(r'country,\[(.*?)\]', filter_str)
                if match:
                    country_code = match.group(1)
    return country_code, production_year


def plot_heatmap(query_info: Dict[Union[int, Tuple[int, str]], Tuple[List[str], int]], plan_categories: List[List[Union[int, Tuple[int, str]]]], directory: str) -> None:
    # Extract country codes and production years
    country_codes = set()
    production_years = set()
    query_filters = {}

    for query_id in query_info.keys():
        with open(os.path.join(directory, f"{query_id}.json"), 'r', encoding='UTF-8') as file:
            plan = json.load(file)
            country_code, production_year = extract_filters(plan)
            if country_code and production_year:
                country_codes.add(country_code)
                production_years.add(production_year)
                query_filters[query_id] = (country_code, production_year)
            else:
                # Debug print statement
                print(f"Filters not found for query ID {query_id}: {plan}")

    # Sort country codes and production years
    country_codes = sorted(country_codes)
    production_years = sorted(production_years)

    # Create a matrix for the heatmap
    heatmap_data = np.zeros((len(production_years), len(country_codes)))

    # Fill the matrix with the number of queries in each category
    for category_index, category in enumerate(plan_categories):
        for query_id in category:
            if query_id in query_filters:
                country_code, production_year = query_filters[query_id]
                row = production_years.index(production_year)
                col = country_codes.index(country_code)
                heatmap_data[row, col] += 1

    # Plot the heatmap
    plt.figure(figsize=(12, 8))
    sns.heatmap(heatmap_data, annot=True, fmt="d", xticklabels=country_codes,
                yticklabels=production_years, cmap="YlGnBu")
    plt.xlabel('Country Code')
    plt.ylabel('Production Year')
    plt.title('Query Plan Categories Heatmap')
    plt.show()


# Directory containing the query plans
directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/15a'

# Collect the node types and depth of the plans
query_info = info_qplan(directory)

# Categorize the plans
plan_categories = categorize_plans(query_info)

# Plot the heatmap
plot_heatmap(query_info, plan_categories, directory)
