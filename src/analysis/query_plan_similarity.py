import csv
import os
import json
from typing import Union, Dict, List, Tuple

node_types = []
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
        if 'Plan' in plan:
            traverse(plan['Plan'], depth + 1)
        if 'Triggers' in plan or 'Execution Time' in plan or 'Planning Time' in plan:
            return


def extract_node_types_and_depth_from_plan(plan: Union[Dict, List]) -> Tuple[List[str], int]:
    node_types.clear()
    max_depth[0] = 0
    traverse(plan, 1)
    return node_types.copy(), max_depth[0] - 1


def extract_number(filename: str) -> Tuple[int, str]:
    parts = filename.split('_')
    try:
        query_id = int(parts[0])
        suffix = parts[1] if len(parts) > 1 else ''
    except ValueError:
        query_id = float('inf')
        suffix = ''
    return query_id, suffix


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
            if query_info[category[0]] == (node_types, depth):
                category.append(query_id)
                found = True
                break
        if not found:
            categories.append([query_id])
    return categories


def save_categories_to_csv(categories: List[List[Union[int, Tuple[int, str]]]], directory: str, filename: str) -> None:
    total_plans = sum(len(category) for category in categories)
    csv_filename = os.path.join(directory, f'{filename}_plan_categories.csv')

    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Category', 'Number of Plans', 'Percentage',
                        'Last Node Type', 'Second Last Node Type'])

        for i, category in enumerate(categories, start=1):
            num_plans = len(category)
            percentage = (num_plans / total_plans) * 100
            last_node_type = node_types[-1] if node_types else 'N/A'
            second_last_node_type = node_types[-2] if len(
                node_types) > 1 else 'N/A'
            writer.writerow(
                [f'Category {i}', num_plans, percentage, last_node_type, second_last_node_type])


def process_directory(directory: str, output_directory: str) -> None:
    query_info = info_qplan(directory)
    sorted_query_info = dict(
        sorted(query_info.items(), key=lambda item: item[1][1]))
    categories = categorize_plans(sorted_query_info)
    directory_name = os.path.basename(directory)
    save_categories_to_csv(categories, output_directory, directory_name)


def process_benchmark(benchmark: str) -> None:
    if benchmark == 'job':
        job_directories = [
            '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/qplans/12c',
            '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/qplans/14b',
            '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/qplans/15a'
        ]
        job_output_directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/plan_categories'

        for directory in job_directories:
            process_directory(directory, job_output_directory)
    elif benchmark == 'tpch':
        tpch_queries = [2, 3, 5, 7, 8, 12, 13, 14, 17]
        tpch_output_directory = '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/plan_categories'

        summary_needed = input(
            "Do you want a summary CSV for tpch? (yes/no): ").strip().lower() == 'yes'
        summary_data = set()

        for query_id in tpch_queries:
            directory = f'/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/tpch/qplans/q{query_id}'
            process_directory(directory, tpch_output_directory)
            if summary_needed:
                query_info = info_qplan(directory)
                sorted_query_info = dict(
                    sorted(query_info.items(), key=lambda item: item[1][1]))
                categories = categorize_plans(sorted_query_info)
                for i, category in enumerate(categories, start=1):
                    num_plans = len(category)
                    percentage = (num_plans / sum(len(cat)
                                                  for cat in categories)) * 100
                    last_node_type = node_types[-1] if node_types else 'N/A'
                    second_last_node_type = node_types[-2] if len(
                        node_types) > 1 else 'N/A'
                    for query in category:
                        summary_data.add(
                            (query_id, f'Category {i}', num_plans, percentage, last_node_type, second_last_node_type))

        if summary_needed:
            summary_csv_filename = os.path.join(
                tpch_output_directory, 'summary_plan_categories.csv')
            summary_data = list(summary_data)
            with open(summary_csv_filename, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Query ID', 'Category', 'Number of Plans',
                                'Percentage', 'Last Node Type', 'Second Last Node Type'])
                writer.writerows(summary_data)
    else:
        print("Error: Invalid benchmark type. Please enter 'tpch' or 'job'.")


def main():
    benchmark = input("Enter the benchmark type (tpch/job): ").strip().lower()
    process_benchmark(benchmark)


if __name__ == "__main__":
    main()
