import os
import json
import requests
import webbrowser

# DiffChecker API
API_URL = "https://api.diffchecker.com/public/text"
API_EMAIL = "fridtjof.damm@t-online.de"

# FOLDERS
LOGS_FOLDER = "results/job/logs"
PLANS_FOLDER = "results/job/qplans"

# read logs
def read_log_files() -> list[str]:
    log_files = [f for f in os.listdir(LOGS_FOLDER) if f.endswith(".log")]
    return log_files

# extract log comparisons
def extract_comparions(log_content) -> list[tuple[str, str]]:
    comparisons = []
    logs = log_content.split("Comparison performed on:")
    for log in logs[1:]:
        match = re.search(r'Comparison bewteen (.*?) and (.*?):', log)
        if match and "No significant differences found" not in log:
            left_file, right_file = match.group(1), match.group(2)
            comparisons.append((left_file, right_file))
    return comparisons

# main
def main() -> None:
    # main logic
    pass 

if __name__ == "__main__":
    main()