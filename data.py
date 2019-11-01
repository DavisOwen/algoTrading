import datetime
import os
import pickle
import logging
import quandl
import pandas as pd

from abc import ABCMeta, abstractmethod

from event import MarketEvent

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

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
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
        Returns the latest bar from the data feed as a tuple of
        (symbol, datetime, open, low, high, close, volume).
        """
        for b in self.symbol_data[symbol]:
            yield tuple([symbol,
                         datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'),
                         b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
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
    def __init__(self, events, pickle_dir, symbol_list, test_date):
        """
        Initialises the quandl data handler.

        Parameters:
        events - events queue object
        pickle_dir - directory to find and save pickle objects
        symbol_list - list of symbols in backtest
        """
        self.events = events
        self.pickle_dir = pickle_dir
        self.symbol_list = symbol_list
        self.test_date = test_date

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True

        self.get_new_bars = self._get_new_bars_dict()

        self._get_data_from_pickle()

        self.train_date = self._get_train_date()

        self._adjust_start_date()

    def _get_data_from_pickle(self):
        """
        Tries to load data from the pickle file, and if
        not found will run _download_quandl_ticker() to download
        the security from quandl and store it in pickle format
        """
        for sec in self.symbol_list:
            try:
                self.symbol_data[sec] = pickle.load(open(os.path.join(
                                            self.pickle_dir,
                                            "{sec}.pickle".format(sec=sec)),
                                                         'rb'))
            except FileNotFoundError:
                logger.info("File not found! Running download_quandl_ticker()")
                self.symbol_data[sec] = self._download_quandl_ticker(sec)
            self.latest_symbol_data[sec] = []

    def _adjust_start_date(self):
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s][self.train_date:]

    def _download_quandl_ticker(self, sec):
        """
        Downloads security from quandl and saves to pickle_dir

        Parameters:
        sec - security ticker
        """
        logger.info("Downloading {sec}".format(sec=sec))
        data = quandl.get("WIKI/{sec}".format(sec=sec))
        pickle.dump(data, open(os.path.join(
            self.pickle_dir, "{sec}.pickle".format(sec=sec)), 'wb'))
        return data

    def _get_new_bars_dict(self):
        get_new_bars = {}
        for s in self.symbol_list:
            get_new_bars[s] = self._get_new_bars(s)
        return get_new_bars

    def _get_new_bars(self, symbol):
        """
        Returns the latest bar from the data feed as a tuple of
        (symbol, datetime, open, low, high, close, volume, dividend events and
        split events).
        """
        for index, row in self.symbol_data[symbol][self.test_date:].iterrows():
            yield [symbol, index, row['Open'], row['Low'],
                   row['High'], row['Close'],
                   row['Volume'], row['Ex-Dividend'],
                   row['Split Ratio']]

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
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

        Paramters:
        :param bar: bar object to check if there was a split or div event
        :type bar: tuple
        """
        symbol = bar[0]
        date = bar[1]
        close = bar[5]
        ex_div = bar[7]
        split = bar[8]
        adj_ratio = split
        adj_ratio *= (close + ex_div) / close
        if adj_ratio != 1.0:
            logger.info("Adjusting data for {symbol} on {date}. "
                        "Close: {close}, Ex-Div: {ex_div}, "
                        "Split-Ratio: {split}"
                        .format(symbol=symbol, date=date, close=close,
                                ex_div=ex_div, split=split))
            for s in self.symbol_list:
                for i, bar in enumerate(self.latest_symbol_data[s]):
                    for j in range(2, 6):
                        self.latest_symbol_data[s][i][j] /= adj_ratio

    def _adjust_data_train(self, bars):
        """
        TODO figure this shit out
        I figured it out
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
        latest = datetime.datetime.min
        for s in self.symbol_list:
            date = self.symbol_data[s].index[0]
            if date > latest:
                latest = date
        return latest

    def generate_train_set(self, price_type):
        logger.info("Generating training set for {price_type} data. "
                    "Starting at {start_date} and ending at {end_date}"
                    .format(price_type=price_type,
                            start_date=self.train_date,
                            end_date=self.test_date))
        train_set = []
        for s in self.symbol_list:
            bars = self.symbol_data[s].iloc[:self.symbol_data[s].index
                                            .get_loc(self.test_date,
                                                     method='backfill')]
            adjusted_bars = self._adjust_data_train(bars)
            train_set.append(adjusted_bars[price_type])
        return train_set

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
                    logger.info("Generating bar for {date}"
                                .format(date=bar[1]))
                    self._adjust_data_test(bar)
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())

    def get_adj_close(self, start=None, end=None):
        """
        gets adj_close data for all tickers provided for
        specific time period

        Parameters:
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
