import urllib3
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import pickle


def scrape_list():

    print('Scraping tickers')
    site = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    hdr = {'User-Agent': 'Mozilla/5.0'}
    http = urllib3.PoolManager()
    response = http.request('GET',site)
    soup = BeautifulSoup(response.data, 'html.parser')

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr')[1:]:
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[1].string.strip())
            for i in range(len(ticker)):
               if ticker[i] == '.':
                  new = ticker[:i]+'_'+ticker[(i+1):]
                  ticker = new
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers

def listify(x):
    
    """ Turns dictionary of lists into one long list """

    y = list()
    for lis in x.values():
        for val in lis:
            y.append(val)
    return y

def plot_results(results):

    """ Plots useful metrics from a zipline backtest result object """

    plt.figure()
    plt.plot(results.portfolio_value)
    plt.title('Portfolio Value')
    plt.figure()
    plt.plot(results.benchmark_period_return)
    plt.plot(results.algorithm_period_return)
    plt.title('Benchmark Returns vs. Algo Returns')
    plt.legend(['Benchmark Returns','Algo Returns'])
    plt.figure()
    plt.plot(results.sharpe)
    plt.title('Rolling Sharpe')
    plt.figure()
    plt.subplot(2,2,1)
    plt.plot(results.gross_leverage)
    plt.title('Gross Leverage')
    plt.subplot(2,2,2)
    plt.plot(results.net_leverage)
    plt.title('Net Leverage')
    plt.subplot(2,2,3)
    plt.plot(results.max_leverage)
    plt.title('Max Leverage')
    plt.show()

def get_oldest():

    """ 

    Gets the oldest stocks on the S&P500

    """

    adj_close, volume, split, div, close = pickle.load(open('WIKIdata.pickle','rb'))
    return adj_close.iloc[0].dropna().index.format()