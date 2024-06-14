import json
from deepdiff import DeepDiff

with open('results/dummyresults/plan0.json', mode='r', encoding='UTF-8') as a, open('results/dummyresults/plan1.json', mode='r', encoding='UTF-8') as b:
    a_data = json.loads(a.read())
    b_data = json.loads(b.read())
    a.close()
    b.close()

with open('results/dummyresults/01result.json', mode='w', encoding='UTF-8') as a:  
    diff = DeepDiff(a_data,b_data, view='tree')
    res = diff.to_json
    a.write(str(diff))
    a.close()


