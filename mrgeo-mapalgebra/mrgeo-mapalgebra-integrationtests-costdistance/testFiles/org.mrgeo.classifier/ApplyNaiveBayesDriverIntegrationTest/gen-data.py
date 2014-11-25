
import random

random.seed(0)

l = 400

fp = open("input.tsv", 'w')
for j in range(0, l):
    a = []
    if j < l / 2:
        a.append(str(random.gauss(0, .5)))
        a.append(str(random.gauss(1, .5)))
        a.append(str(random.gauss(2, 0.1)))
        a.append('apple')
    else:
        a.append(str(random.gauss(1, .5)))
        a.append(str(random.gauss(2, .5)))
        a.append(str(random.gauss(2, 0.1)))
        a.append('orange')
    
    fp.write('\t'.join(a) + '\n')      
