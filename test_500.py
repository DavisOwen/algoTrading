#!algotrading/bin/python3

'''
Creates a time series from all 500 stocks
in the S&P

'''

import urllib3
from bs4 import BeautifulSoup
from securityList import SecurityList
import datetime
import numpy as np
import matplotlib.pyplot as plt

def scrape_list(site):

    print('Scraping tickers')
    hdr = {'User-Agent': 'Mozilla/5.0'}
    http = urllib3.PoolManager()
    response = http.request('GET',site)
    soup = BeautifulSoup(response.data, 'html.parser')

    table = soup.find('table', {'class': 'wikitable sortable'})
    sector_tickers = dict()
    for row in table.findAll('tr'):
        col = row.findAll('td')
        if len(col) > 0:
            sector = str(col[3].string.strip()).lower().replace(' ', '_')
            ticker = str(col[0].string.strip())
            for i in range(len(ticker)):
               if ticker[i] == '.':
                  new = ticker[:i]+'_'+ticker[(i+1):]
                  ticker = new
            if sector not in sector_tickers:
                sector_tickers[sector] = list()
            sector_tickers[sector].append(ticker)
    return sector_tickers

def main():

    tickers = scrape_list('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies');
    lticks = list()
    for l in tickers.values():
        for tick in l:
            lticks.append(tick)
    sl = SecurityList(lticks)
    start = datetime.datetime(1994,9,29)
    end = datetime.datetime(2017,4,5)
    sl.downloadQuandl(start, end)
    ts = sl.genTimeSeries()
    plt.plot(np.arange(len(ts)),ts)
    plt.show()

main()
