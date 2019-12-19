import datetime
import operator
import os
import pickle
import logging
import quandl
import pandas as pd
from utils import scrape_list, listify

from abc import ABCMeta, abstractmethod

from event import MarketEvent


pickle_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "pickle_files")

logger = logging.getLogger("backtester")
quandl.ApiConfig.api_key = 'AfS6bPzj1CsRFyYxCcvz'


class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic)

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implemented update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provides an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """

    def __init__(self, events, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        :param events: the event Queue
        :type events: Queue
        :param csv_dir: absolute directory path to the CSV files
        :type csv_dir: str
        :param symbol_list: a list of symbol strings
        :type symbol_list: list(str)
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from DTN IQFeed. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on data
            self.symbol_data[s] = pd.io.parsers.read_csv(
                                        os.path.join(self.csv_dir,
                                                     '{symbol}.csv'.format(s)),
                                        header=0, index_col=0,
                                        names=['datetime', 'open', 'low',
                                               'high', 'close', 'volume', 'oi']
                                  )

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []

        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                                                        index=comb_index,
                                                        method='pad'
                                                      ).iterrows()

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed

        :param symbol: ticker symbol to get bar for
        :type symbol: str

        :return: bar data (symbol, datetime, open, low, high, close, volume)
        :rtype: tuple
        """
        for b in self.symbol_data[symbol]:
            yield tuple([symbol,
                         datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'),
                         b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.

        :param symbol: ticker symbol to get bar for
        :type symbol: str
        :param N: (optional) number of bars to get
        :type N: int

        :return: list of bars
        :rtype: list(tuple)
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            logger.error(
                "That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bars(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())


class QuandlAPIDataHandler(DataHandler):
    """
    DataHandler class that pulls data from quandl using supplied tickers.
    If data exists in pickle files, will load from pickle, if they do
    not exist, will save data to pickle file

    Data format for quandl is:
    index - (pd.Timestamp) date
    data - (float) Open, High, Low, Close, Volume, Ex-Dividend, Split Ratio,
    Adj. Open, Adj. High, Adj. Low, Adj. Close, Adj. Volume
    """
    def __init__(self, events, symbol_list, test_date, adjust):
        """
        Initialises the quandl data handler.

        :param events: events Queue object
        :type events: Queue
        :param symbol_list: list of symbols in backtest
        :type symbol_list: list(str)
        :param test_date: start date for backtest
        :type test_date: datetime
        :param adjust: whether or not to adjust previous bar data after
        split or dividend payment (helps speed if not necessary)
        :type adjust: boolean
        """
        self.events = events

        self.latest_symbol_data = {}
        self.continue_backtest = True
        self.adjust = adjust

        self.update_symbol_list(symbol_list, test_date)

    def _get_data_from_pickle(self):
        """
        Tries to load data from the pickle file, and if
        not found will run _download_quandl_ticker() to download
        the security from quandl and store it in pickle format
        """
        for sec in self.symbol_list:
            try:
                self.symbol_data[sec] = pickle.load(open(os.path.join(
                                            pickle_dir,
                                            "{sec}.pickle".format(sec=sec)),
                                                         'rb'))
            except FileNotFoundError:
                logger.info("File not found! Running download_quandl_ticker()")
                self.symbol_data[sec] = self._download_quandl_ticker(sec)
            self.latest_symbol_data[sec] = []

    def sort_oldest(self):
        """
        Sorts all s&p tickers by oldest stocks

        :return: sorted list of ticker
        :rtype: list(str)
        """
        tickers = scrape_list()
        tickers = listify(tickers)
        date_dict = {}
        for ticker in tickers:
            try:
                sec = pickle.load(open(os.path.join(
                                pickle_dir,
                                "{sec}.pickle".format(sec=ticker)),
                                   'rb'))
            except FileNotFoundError:
                pass
            else:
                date_dict[ticker] = sec.index[0]
        return [x[0] for x in sorted(date_dict.items(),
                                     key=operator.itemgetter(1))]

    def _adjust_start_date(self):
        """
        Sets the start date for generating training set
        """
        self.train_date = self._get_train_date()
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s][self.train_date:]

    def _download_quandl_ticker(self, sec):
        """
        Downloads security from quandl and saves to pickle_dir

        :param sec: security ticker
        :type sec: str

        :return: quandl ticker data
        :rtype: pd.Dataframe
        """
        logger.info("Downloading {sec}".format(sec=sec))
        data = quandl.get("WIKI/{sec}".format(sec=sec))
        pickle.dump(data, open(os.path.join(
            pickle_dir, "{sec}.pickle".format(sec=sec)), 'wb'))
        return data

    def _get_new_bars_dict(self):
        """
        Auxilliary dictionary to keep track of each symbols
        _get_new_bars() generators, so that we can continue to
        call next() and get the next bar for each symbol.

        :return: dictionary of generator functions
        :rtype: dict(generator)
        """
        get_new_bars = {}
        for s in self.symbol_list:
            get_new_bars[s] = self._get_new_bars(s)
        return get_new_bars

    def _get_new_bars(self, symbol):
        """
        Returns the latest bar from the data feed

        Parameters:

        :param symbol: ticker symbol to get bar for
        :type symbol: str

        :return: dictionary of bar data with format \
        (symbol, datetime, open, low, high, close, volume, dividend events \
        and split events)
        :rtype: dict
        """
        for index, row in self.symbol_data[symbol][self.test_date:].iterrows():
            yield {'Symbol': symbol, 'Date': index, 'Open': row['Open'],
                   'Low': row['Low'], 'High': row['High'],
                   'Close': row['Close'], 'Volume': row['Volume'],
                   'Dividend': row['Ex-Dividend'], 'Split': row['Split Ratio']}

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.

        :param symbol: bar symbol
        :type symbol: str
        :param N: number of bars to fetch
        :type N: int

        :return: list of bars
        :rtype: list(dict)
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            logger.error(
                "That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]

    def _adjust_data_test(self, bar):
        """
        Checks if there is a dividend for split event
        on the given bar, and if there is, will adjust
        all previous bars prices based on split or div

        :param bar: bar object to check if there was a split or div event
        :type bar: tuple
        """
        symbol = bar['Symbol']
        date = bar['Date']
        close = bar['Close']
        ex_div = bar['Dividend']
        split = bar['Split']
        adj_ratio = split
        adj_ratio *= (close + ex_div) / close
        if adj_ratio != 1.0:
            logger.info("Adjusting data for {symbol} on {date}. "
                        "Close: {close}, Ex-Div: {ex_div}, "
                        "Split-Ratio: {split}"
                        .format(symbol=symbol, date=date, close=close,
                                ex_div=ex_div, split=split))
            for s in self.symbol_list:
                for i, bar in enumerate(self.latest_symbol_data[s][:-1]):
                    for j in ['High', 'Low', 'Open', 'Close']:
                        self.latest_symbol_data[s][i][j] /= adj_ratio

    def _adjust_data_train(self, bars):
        """
        Adjusts all data in training set based on splits and dividend events

        :param bars: dataframe of training data
        :type bars: pd.Dataframe

        :return: adjusted training data
        :rtype: pd.Dataframe
        """
        start = None
        for index in bars[(bars['Split Ratio'] != 1.0)
                          | (bars['Ex-Dividend'] != 0.0)].index:
            adj_ratio = bars['Split Ratio'][index]
            adj_ratio *= (bars['Close'][index] + bars['Ex-Dividend'][index])\
                / bars['Close'][index]
            bars.loc[start:index, 'Open'].iloc[:-1] /= adj_ratio
            bars.loc[start:index, 'High'].iloc[:-1] /= adj_ratio
            bars.loc[start:index, 'Low'].iloc[:-1] /= adj_ratio
            bars.loc[start:index, 'Close'].iloc[:-1] /= adj_ratio
            start = index
        return bars

    def _get_train_date(self):
        """
        Returns the latest common date between all stocks in symbol_list

        :return: earliest common date of all stocks
        :rtype: datetime
        """
        latest = datetime.datetime.min
        for s in self.symbol_list:
            date = self.symbol_data[s].index[0]
            if date > latest:
                latest = date
        return latest

    def generate_train_set_all(self, price_type):
        """
        Generates training set

        :param price_type: price type to use for train set i.e. \
        open, close, etc.
        :type price_type: str
        """
        logger.info("Generating training set for {price_type} data. "
                    "Starting at {start_date} and ending at {end_date}"
                    .format(price_type=price_type,
                            start_date=self.train_date,
                            end_date=self.test_date))
        train_set = []
        for s in self.symbol_lists:
            train_set.append(self.generate_train_set(s, price_type))
        return train_set

    def generate_train_set(self, sec, price_type):
        bars = self.symbol_data[sec].iloc[:self.symbol_data[sec].index
                                          .get_loc(self.test_date,
                                                   method='backfill')]
        adjusted_bars = self._adjust_data_train(bars)
        return adjusted_bars[price_type]

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self.get_new_bars[s])
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    if self.adjust:
                        self._adjust_data_test(bar)
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())

    def update_symbol_list(self, symbols, test_date):
        """
        Updates symbol list and calls functions to get the quandl data,
        adjusts start date for train set, sets up generator functions,
        and updates the initial bar

        :param symbols: new symbol list
        :type symbols: list(str)
        :param test_date: current date of backtest
        :type test_date: datetime
        """
        self.test_date = test_date
        self.symbol_list = symbols
        self.symbol_data = {}
        self._get_data_from_pickle()
        self._adjust_start_date()
        self.get_new_bars = self._get_new_bars_dict()
        self.update_bars()

    def get_adj_close(self, start=None, end=None):
        """
        gets adj_close data for all tickers provided for
        specific time period

        :param start: (optional) start date for adj_close data
        :type start: datetime
        :param end: (optional) end date for adj_close data
        :type end: datetime

        :return: adjusted close data
        :rtype: pd.DataFrame
        """
        adj_close_df = pd.DataFrame(index=self.symbol_data.index,
                                    columns=self.symbol_list)
        for sec in self.symbol_list:
            adj_close_df[sec] = self.symbol_data[sec]['Adj. Close']
        return adj_close_df
