#!/usr/bin/env python

'''
Create a pandas dataframe containing columns
with johansen test hedge ratios, augmented
dickey fuller test statistics, start and
end dates, and hurst exponents, indexed
by different combinations of s&p500
securities, 1 from each sector
'''

import datetime
import numpy as np
import pandas as pd
import statsmodels.tsa.stattools as ts
import time
import sys
import cPickle as pickle
from securityList import SecurityList
import itertools
import signal
from utils import scrape_list


def beginning_date(tickers, date):

    tick_dict = dict()
    new_tickers = [[] for i in range(len(tickers))]
    for i, arr in enumerate(tickers):
        for j, tick in enumerate(arr):
            print('Altering %s Dates' % tick)
            temp = pickle.load(open(path_to_data+tick+'.pickle', 'rb'))
            beg_date = datetime.datetime.strptime(temp.index[0], '%Y-%m-%d')
            if beg_date <= date:
                new_tickers[i].append(tick)
                tick_dict[tick] = temp
    return new_tickers, tick_dict


def safe_test_stocks(*args, **kwargs):
    try:
        return test_stocks(*args, **kwargs)
    except np.linalg.linalg.LinAlgError:
        pass


def test_stocks(params):

    results = pd.DataFrame()
    tick_dict = params[-1]
    columns = params[:-1]
    name = str(columns)
    tick_list = [tick_dict[x]['adj_close'] for x in columns]
    data = pd.DataFrame(columns=columns)
    len_tick_list = [len(x) for x in tick_list]
    if all(x == len_tick_list[0] for x in len_tick_list):
        results.loc[name, 'Start Date'] = tick_list[0].index[0]
        results.loc[name, 'End Date'] = tick_list[0].index[-1]
    else:
        minimum = min(len_tick_list)
        minimum = minimum*-1
        for x, tick in enumerate(tick_list):
            tick_list[x] = tick[minimum:]
        results.loc[name, 'Start Date'] = tick_list[0].index[0]
        results.loc[name, 'End Date'] = tick_list[0].index[-1]
    for x, col in enumerate(columns):
        data[col] = tick_list[x]
    for col in columns:
        index = np.argwhere(np.isnan(data[col]))
        data[col][index] = data[col][index-1]
    sec_list = SecurityList()
    sec_list.importData(data)
    eig = sec_list.genHedgeRatio()
    results.loc[name, 'Eigenvector'] = str(eig)
    res = np.dot(data, eig)
    cadf = ts.adfuller(res)
    results.loc[name, 'ADF Test'] = cadf[0]
    results.loc[name, 'ADF 1% Critical Value'] = cadf[4]['1%']
    results.loc[name, 'Hurst'] = hurst(res)
    return results


def hurst(ts):
    """Returns the Hurst Exponent of the time series vector ts"""
    # Create the range of lag values
    lags = range(2, 100)

    # Calculate the array of the variances of the lagged differences
    tau = [np.sqrt(np.std(np.subtract(ts[lag:], ts[:-lag]))) for lag in lags]

    # Use a linear fit to estimate the Hurst Exponent
    poly = np.polyfit(np.log(lags), np.log(tau), 1)

    # Return the Hurst exponent from the polyfit output
    return poly[0]*2.0


if __name__ == "__main__":

    def handler(signum, frame):
        pickle.dump(results, open('11_johansen_results.pickle', 'wb'))
        sys.exit()
    signal.signal(signal.SIGINT, handler)
    start_time = time.time()
    path_to_data = '/home/sowen/algorithmic_trading/algorithms/data/'
    tickers = scrape_list()
    tickers = tickers.values()
    date = datetime.datetime(1990, 1, 1)
    tickers, tick_dict = beginning_date(tickers, date)
    print('Creating paramlist')
    paramlist = [itertools.islice(x, 2) for x in tickers]
    paramlist = list(itertools.product(*paramlist))
    paramlist = [x + (tick_dict,) for x in paramlist]
    print('Starting Test Stocks')
    results = pd.DataFrame()
    len_paramlist = len(paramlist)
    done = int()
    for i in itertools.imap(safe_test_stocks, paramlist):
        done += 1
        print('%s out of %s' % (done, len_paramlist))
        results = results.append(i)
    pickle.dump(results, open('11_johansen_results.pickle', 'wb'))
    print('Ran for %s' % (time.time()-start_time))
