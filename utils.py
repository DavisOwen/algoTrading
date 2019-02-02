import urllib3
from bs4 import BeautifulSoup

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
    y = list()
    for lis in x.values():
        for val in lis:
            y.append(val)
    return y
