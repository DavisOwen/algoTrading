#!algotrading/bin/python

'''
converts local csv file of stock prices
into pandas data frame and stores it as
pickle file in ./data

usage:
    pickleCSV /path/to/file.csv

'''

import pandas as pd
import cPickle as pickle
import sys

csv = pd.read_csv(sys.argv[1])
unique = csv.ticker.unique()
for tick in unique:
    tick_csv = csv[:][csv.ticker == tick].set_index('date')
    tick_csv = tick_csv.drop('ticker', 1)
    pickle.dump(tick_csv, open('./data/'+tick+'.pickle', 'wb'))
