import random

def dummygen():
    data = []
    value = 20
    for i in range(value):
        for j in range(2**i):
            data.append(i)
    random.shuffle(data)
    return data

with open('results/dummyresults/data.csv', encoding='UTF-8', mode='w') as file:
    data = dummygen()
    for row in data:
        file.write(str(row) + '\n')
    file.close()

def dummygen2tuples():
    data_as_tuples = []  
    for val in dummygen():
        data_as_tuples.append((val,))
    return data_as_tuples


