import random

def dummygen():
    data = []
    value = 20
    for i in range(value):
        for j in range(2**value):
            data.append(i)
    random.shuffle(data)
    return data

data = dummygen()
print(len(data))
