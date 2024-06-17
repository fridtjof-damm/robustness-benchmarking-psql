import json
# write json query plans to py dicts
query_plans = []

for i in range(0,20): 
    with open(f'results/dummyresults/plan{i}.json', mode='r', encoding='UTF-8') as a:
        a_data = json.loads(a.read())
        query_plans.append(a_data)
        a.close()

# write scan 
with open('results/dummyresults/queryplans.csv', mode='w', encoding='UTF-8') as file:
    for i, plan  in enumerate(query_plans):
        file.write(str(plan['result']) +';')
        plan = str(plan)
        if "INDEX_SCAN " in plan:
            file.write('index_scan'+'\n')
        if "SEQ_SCAN " in plan:
           file.write('seq_scan'+'\n')
    file.close()
    
