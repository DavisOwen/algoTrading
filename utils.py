import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.tsa.vector_ar.vecm as jh
import statsmodels.tsa.stattools as ts


def scrape_list():

    site = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    response = requests.get(site)
    soup = BeautifulSoup(response.content, 'html.parser')

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr')[1:]:
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].a.string.strip())
            for i in range(len(ticker)):
                if ticker[i] == '.':
                    new = ticker[:i]+'_'+ticker[(i+1):]
                    ticker = new
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers


def listify(dic):
    """ Turns dictionary of lists into one long list """

    arr = []
    for lis in dic.values():
        for val in lis:
            arr.append(val)
    return arr


def plot_results(results):
    """ Plots useful metrics from a zipline backtest result object """

    plt.figure()
    plt.plot(results.portfolio_value)
    plt.title('Portfolio Value')
    plt.figure()
    plt.plot(results.benchmark_period_return)
    plt.plot(results.algorithm_period_return)
    plt.title('Benchmark Returns vs. Algo Returns')
    plt.legend(['Benchmark Returns', 'Algo Returns'])
    plt.figure()
    plt.plot(results.sharpe)
    plt.title('Rolling Sharpe')
    plt.figure()
    plt.subplot(2, 2, 1)
    plt.plot(results.gross_leverage)
    plt.title('Gross Leverage')
    plt.subplot(2, 2, 2)
    plt.plot(results.net_leverage)
    plt.title('Net Leverage')
    plt.subplot(2, 2, 3)
    plt.plot(results.max_leverage)
    plt.title('Max Leverage')
    plt.show()


def generate_hedge_ratio_from_df(df):
    """
    Uses matrix generated from df
    to calcuate hedge ratio with coint_johansen
    statistical test

    Parameters:
    :param df: pd.DataFrame to generate hedge_ratio for
    :type df: pd.DataFrame

    :return: hedge ratio
    :rtype: List
    """
    ts_row, ts_col = df.shape
    matrix = np.zeros((ts_row, ts_col))
    for i, sec in enumerate(df):
        matrix[:, i] = df[sec]
    results = jh.coint_johansen(matrix, 0, 1)
    return results.evec[:, 0]


def generate_hedge_ratio(prices):
    prices = np.array(prices)
    prices = prices.T
    results = jh.coint_johansen(prices, 0, 1)
    # return results.evec[:, 0]
    return results


def is_stationary(prices, hedge_ratio):
    time_series = dot(prices, hedge_ratio)
    cadf = ts.adfuller(time_series)
    return cadf[0] <= cadf[4]['1%']


def dot(arr1, arr2):
    if len(arr1) != len(arr2):
        return 0
    return sum(i[0] * i[1] for i in zip(arr1, arr2))
