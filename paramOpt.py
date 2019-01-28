#!algotrading/bin/python3

'''
Test optimal std deviation parameter
in bollinger pair johansen for the bollinger band
'''

import bpjtest as bpj
import matplotlib.pyplot as plt
import numpy as np
import multiprocessing as mp
from mpl_toolkits.mplot3d import Axes3D

class Vividict(dict):
    def __missing__(self, key):
        value = self[key] = type(self)() # retain local pointer to value
        return value                     # faster to return than dict lookup

Enter = np.linspace(0.5,3,25)
Exit = np.linspace(0,2,20)

res=Vividict()

pool = mp.Pool()

for i in Enter:
    for j in Exit:
        res[i][j] = pool.apply_async(bpj.main,args=(i,j))

pool.close()
pool.join()
results = Vividict()

for i in res:
    for j in res[i]:
        results[i][j] = res[i][j].get(timeout=1)

print(results)

x = list()
y = list()
z = list()
maximum = float()
for key,val in results.items():
    x.extend([key]*len(val))
    y.extend(val.keys())
    z.extend(val.values())

print(str(max(z))+' Enter: '+str(x[z.index(max(z))])+' Exit: '+str(y[z.index(max(z))]))

fig = plt.figure()
ax = fig.add_subplot(111,projection='3d')
ax.scatter(x,y,z)
plt.show()
