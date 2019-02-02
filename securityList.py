'''
SecurityList class that loads security data
and finds hedge ratio of portolfio, returns
time series, and other useful functions

'''

import quandl
import numpy as np
import pandas as pd
import datetime
import statsmodels.tsa.vector_ar.vecm as jh
import matplotlib.pyplot as plt
import pickle
import sys
from utils import scrape_list, listify

quandl.ApiConfig.api_key = 'AfS6bPzj1CsRFyYxCcvz'

class SecurityList():

    def __init__(self,tickers):

        self.tickers = tickers
        self.data = pd.DataFrame(columns=self.tickers)
        self.volume = pd.DataFrame(columns=self.tickers)
        self.split = pd.DataFrame(columns=self.tickers)
        self.div = pd.DataFrame(columns=self.tickers)
        self.close = pd.DataFrame(columns=self.tickers)

    def importData(self,data):

        self.data = data

    def loadData(self,start = None, end = None, minLength = 300):

        '''
        Loads data for tickers from
        start to end. If stocks don't 
        have data from start, will 
        load data from first available 
        date

        '''

        try: 
            self.data,self.volume,self.split,self.div,self.close = pickle.load(open('WIKIdata.pickle','rb'))
        except FileNotFoundError:
            print("File not found! Running downloadQuandl()")
            self.downloadQuandl()

        self.data = self.data[self.tickers][start:end]
        self.volume = self.volume[self.tickers][start:end]
        self.split = self.split[self.tickers][start:end]
        self.div = self.div[self.tickers][start:end]
        self.close = self.close[self.tickers][start:end]
        if(self.checkNaN()):
            start = self.findEarliest()
            print('No data for start time. Using %s instead' % start)
            self.data = self.data[start:].dropna(axis='columns')
            self.volume = self.volume[start:].dropna(axis='columns')
            self.split = self.split[start:].dropna(axis='columns')
            self.div = self.div[start:].dropna(axis='columns')
            self.close = self.close[start:].dropna(axis='columns')
        print('Resulting tickers: %s' % self.data.columns)

    def checkNaN(self):

        columns = self.data.columns[self.data.isna().any()].tolist()
        for col in columns:
            self.downloadSecurity(col)            
        columns = self.data.columns[self.data.isna().any()].tolist()
        if(columns):
            return True
        return False

    def findEarliest(self):
        return self.data[self.data.count().idxmin()].first_valid_index()

    def downloadSecurity(self, sec):

        try:
            self.downloadSecurityUtil(sec)
        except Exception as e:
            print(e)
            if type(e) is KeyboardInterrupt:
                sys.exit(0)

    def downloadSecurityUtil(self, sec):

        print("downloading "+sec)
        a = quandl.get('WIKI/'+sec)
        self.data[sec] = a['Adj. Close']
        self.volume[sec] = a['Volume']
        self.split[sec] = a['Split Ratio']
        self.div[sec] = a['Ex-Dividend']
        self.close[sec] = a['Close']

    def downloadQuandl(self):

        '''
        Downloads all quandl data for 
        all stocks in SP500 from all time,
        and stores in WIKIdata.pickle

        '''
        tickers = scrape_list()
        tickers = listify(tickers)
        for sec in tickers:
            self.downloadSecurity(sec)
        pickle.dump((self.data,self.volume,self.split,self.div,self.close),open('WIKIdata.pickle','wb'))

    def genTimeSeries(self):

        '''
        Generate Time Series using johansen test
        '''
        eig = self.genHedgeRatio()
        ts = np.dot(self.data,eig)
        return ts

    def genHedgeRatio(self):

        matrix = self.genMatrix()
        results = jh.coint_johansen(matrix,0,1)
        return results.evec[:,0]

    def genMatrix(self):

        ts_row,ts_col = self.data.shape
        matrix = np.zeros((ts_row,ts_col))
        for i, sec in enumerate(self.data):
            matrix[:,i] = self.data[sec]
        return matrix

    def getVolume(self):
        return self.volume

    def getSplits(self):
        return self.split

    def getDiv(self):
        return self.div

    def getAdjFactors(self):
        temp = self.div.copy()
        temp[temp != 0] = 1
        close = self.close*temp
        adj_factors = self.div+close
        close[close == 0] = 1
        adj_factors /= close
        adj_factors[adj_factors == 0] = 1
        return adj_factors

    def adjSplits(self):
        split = self.split.product()
        eig = self.genHedgeRatio()
        adj = eig/split
        return adj

    def adjDividends(self):
        adj_fact = self.getAdjFactors()
        total_fact = adj_fact.product()
        return total_fact
