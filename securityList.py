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

    def __init__(self, tickers, start = None, end = None):

        self.tickers = tickers
        self.adj_close = pd.DataFrame(columns=self.tickers)
        self.volume = pd.DataFrame(columns=self.tickers)
        self.split = pd.DataFrame(columns=self.tickers)
        self.div = pd.DataFrame(columns=self.tickers)
        self.close = pd.DataFrame(columns=self.tickers)

        # Hedge ratio start and end
        self.start = start
        self.end = end

    def loadData(self, start = None, end = None, minLength = 300):

        """
        Loads data for tickers from
        start to end. If stocks don't 
        have data from start, will 
        load data from first available 
        date

        """

        try: 
            self.adj_close,self.volume,self.split,self.div,self.close = pickle.load(open('WIKIdata.pickle','rb'))
        except FileNotFoundError:
            print("File not found! Running downloadQuandl()")
            self.downloadQuandl()
        self.adj_close = self.adj_close[self.tickers][start:end]
        self.volume = self.volume[self.tickers][start:end]
        self.split = self.split[self.tickers][start:end]
        self.div = self.div[self.tickers][start:end]
        self.close = self.close[self.tickers][start:end]
        if(self.checkNaN()):
            start = self.findEarliest()
            print('No data for start time. Using %s instead' % start)
            self.adj_close = self.adj_close[start:].dropna(axis='columns')
            self.volume = self.volume[start:].dropna(axis='columns')
            self.split = self.split[start:].dropna(axis='columns')
            self.div = self.div[start:].dropna(axis='columns')
            self.close = self.close[start:].dropna(axis='columns')
        print('Resulting tickers: %s' % self.adj_close.columns)

    def checkNaN(self):

        check_list = [self.adj_close, self.volume, self.split, self.div, self.close]
        for item in check_list:
            columns = item.columns[item.isna().all()].tolist()
            for col in columns:
                self.downloadSecurity(col)            
            columns = item.columns[item.isna().any()].tolist()
            if(columns):
                self.fillNaN(columns, item)
                columns = item.columns[item.isna().any()].tolist()
                if(columns):
                    return True
        return False

    def fillNaN(self, columns, item):

        """
        Fills stray NaN values by setting it to the previous days value

        """

        for col in columns:
            index = np.argwhere(np.isnan(item[col]))
            item[col][index] = item[col][index - 1]

    def findEarliest(self):
        return self.adj_close[self.adj_close.count().idxmin()].first_valid_index()

    def downloadSecurity(self, sec):

        try:
            self.downloadSecurityUtil(sec)
        except Exception as e:
            print(e)
            if type(e) is KeyboardInterrupt:
                sys.exit(0)

    def downloadSecurityUtil(self, sec):

        print("downloading "+sec)
        all_adj_close, all_volume, all_split, all_div, all_close = pickle.load(open('WIKIdata.pickle','rb'))
        a = quandl.get('WIKI/'+sec)
        self.adj_close[sec] = a['Adj. Close']
        self.volume[sec] = a['Volume']
        self.split[sec] = a['Split Ratio']
        self.div[sec] = a['Ex-Dividend']
        self.close[sec] = a['Close']
        all_adj_close[sec] = a['Adj. Close']
        all_volume[sec] = a['Volume']
        all_split[sec] = a['Split Ratio']
        all_div[sec] = a['Ex-Dividend']
        all_close[sec] = a['Close']
        pickle.dump((all_adj_close, all_volume, all_split, all_div, all_close),open('WIKIdata.pickle','wb'))

    def downloadQuandl(self):

        """
        Downloads all quandl data for 
        all stocks in SP500 from all time,
        and stores in WIKIdata.pickle

        """

        tickers = scrape_list()
        tickers = listify(tickers)
        for sec in tickers:
            self.downloadSecurity(sec)
        pickle.dump((self.adjClose,self.volume,self.split,self.div,self.close),open('WIKIdata.pickle','wb'))

    def genTimeSeries(self):

        """
        Generate Time Series using johansen test

        """

        eig = self.genHedgeRatio()
        ts = np.dot(self.adj_close,eig)
        return ts

    def genHedgeRatio(self):

        matrix = self.genMatrix()
        results = jh.coint_johansen(matrix,0,1)
        return results.evec[:,0]

    def genMatrix(self):

        # self.start and self.end are the time period to calculate the 
        # hedge ratio for
        adj_close_eig = self.adj_close[self.start:self.end]
        ts_row, ts_col = adj_close_eig.shape
        matrix = np.zeros((ts_row,ts_col))
        for i, sec in enumerate(adj_close_eig):
            matrix[:,i] = adj_close_eig[sec]
        return matrix

    def get_volume(self):

        return self.volume

    def getSplits(self):

        return self.split

    def getDivs(self):

        return self.div

    def get_adj_close(self):

        return self.adj_close

    def getAdjFactors(self):

        temp = self.div.copy()
        temp[temp != 0] = 1
        close = self.close * temp
        adj_factors = self.div + close
        close[close == 0] = 1
        adj_factors /= close
        adj_factors[adj_factors == 0] = 1
        return adj_factors

    def adjSplits(self):

        split = self.split[self.start:self.end].product()
        eig = self.genHedgeRatio()
        adj = eig / split
        return adj

    def adjDividends(self):

        adj_fact = self.getAdjFactors()
        total_fact = adj_fact.product()
        return total_fact
