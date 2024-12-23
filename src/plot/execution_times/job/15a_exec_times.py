import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the CSV file
data = pd.read_csv(
    '/Users/fridtjofdamm/Documents/thesis-robustness-benchmarking/results/job/15a/15a_job.csv')

# Extract Production Year and Country Code
data['Production Year'] = data['Filters'].str.extract(r'production_year,(\d+)')
data['Country Code'] = data['Filters'].str.extract(r'\[(\w+)\]')
data['Execution Time'] = data['Execution Time'].astype(float)

# Pivot the data to create a matrix for the heatmap
heatmap_data = data.pivot_table(index='Country Code',
                                columns='Production Year',
                                values='Execution Time',
                                aggfunc='first')

# Sort country codes in descending order
heatmap_data = heatmap_data.sort_index(ascending=False)

# Plotting the heatmap
plt.figure(figsize=(20, 10))
sns.heatmap(heatmap_data, cmap='RdYlGn_r', annot=False,
            cbar_kws={'label': 'Execution Time (s)'})

# Add labels and title
plt.title('Execution Time Heatmap by Production Year and Country Code')
plt.xlabel('Production Year')
plt.ylabel('Country Code')
plt.xticks(rotation=90)
plt.tight_layout()

plt.show()
