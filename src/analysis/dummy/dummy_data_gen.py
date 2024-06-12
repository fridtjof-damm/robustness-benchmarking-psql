import random

def dummygen():
    data = []
    value = 20
    for i in range(value):
        for j in range(2**i):
            data.append(i)
    random.shuffle(data)
    return data

print(dummygen())
