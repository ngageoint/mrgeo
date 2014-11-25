
import random

random.seed(0)

for i in range(1, 4):
    fp = open("input%d.tsv" % i, 'w')
    for j in range(0, 40):
        a = []
        c = random.randint(0, 1)
        if c == 0:
            a.append(str(random.uniform(0, 180)))
            a.append(str(random.uniform(-90, 90)))
            a.append(str(random.gauss(2, 0.1)))
            a.append('apple')
        else:
            a.append(str(random.uniform(-180, 0)))
            a.append(str(random.uniform(-90, 90)))
            a.append(str(random.gauss(2, 0.1)))
            a.append('orange')
        
        fp.write('\t'.join(a) + '\n')      
