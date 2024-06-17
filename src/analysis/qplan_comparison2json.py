import json
from deepdiff import DeepDiff

for i in range(0,18): 
        with open(f'results/dummyresults/plan{i}.json', mode='r', encoding='UTF-8') as a, open(f'results/dummyresults/plan{i+1}.json', mode='r', encoding='UTF-8') as b:
            a_data = json.loads(a.read())
            b_data = json.loads(b.read())
            a.close()
            b.close()

        with open('results/dummyresults/queryplan_diff_res.py', mode='a', encoding='UTF-8') as res:  
            diff = DeepDiff(a_data,b_data)
            diff = diff.to_dict()
            res.write(f'diff{i} = '+str(diff)+ '\n')
            res.close()
        

