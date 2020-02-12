from utils import generate_hedge_ratio, dot, is_stationary
import numpy as np
import pandas as pd
import sys
import logging
from statistics import stdev
from itertools import combinations
from pykalman import KalmanFilter

from abc import ABCMeta, abstractmethod

from event import SignalEvent

logger = logging.getLogger("backtester")


class Strategy(object):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.

    The goal of a (derived) Strategy object is to generate Signal
    objects for a particular symbols based on the inputs of Bars
    (OLHCVI) generated by a DataHandler object.

    This is designed to work both with historic and live data as
    the Strategy object is agnostic to the data source,
    since it obtains the bar tuples from a queue object.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_signals(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_signals()")


class BuyAndHoldStrategy(Strategy):
    """
    This is an extremely simple strategy that goes LONG all of the
    symbols as soon as a bar is received. It will never exit a position

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """
    def __init__(self, bars, events):
        """
        Initialises the buy and hold strategy.

        :param bars: The DataHandler object that provides bar information
        :type bars: DataHandler
        :param events: The Event Queue object.
        :type events: Queue
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events

        # Once buy & hold signal is given, these are set to True
        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to False.

        :return: dictionary of booleans for whether symbol has \
        been bought or not
        :rtype: dict
        """
        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event):
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are
        constantly long the market from the date of strategy
        initialisation.

        :param event: A MarketEvent object.
        :type event: MarketEvent
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bar = self.bars.get_latest_bars(s, N=1)[0]
                if bar is not None and bar != {}:
                    if not self.bought[s]:
                        # (Symbol, Datetime, Type = LONG, SHORT or EXIT)
                        signal = SignalEvent(bar['Symbol'], bar['Date'],
                                             'LONG')
                        self.events.put(signal)
                        self.bought[s] = True


class KalmanPairTradeStrategy(Strategy):
    """
    Pairs Trading with Kalman Filters

    Author: David Edwards

    This algorithm extends the Kalman Filtering pairs trading algorithm from a
    previous lecture to support multiple pairs. In order to extend the idea,
    the previous algorithm was factored into a class so several instances can
    be created with different assets.


    This algorithm was developed by David Edwards as part of
    Quantopian's 2015 summer lecture series. Please direct any
    questions, feedback, or corrections to dedwards@quantopian.com
    """
    def __init__(self, bars, portfolio, events, pairs, leverage):
        self.bars = bars
        self.events = events
        self.pairs = []
        self.portfolio = portfolio
        self.leverage = leverage

        for pair in pairs:
            self.pairs.append(KalmanPairTrade(pair, self.bars, self.events,
                                              self.portfolio, self.leverage))

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for pair in self.pairs:
                pair.calculate_signal()


class KalmanPairTrade(object):
    def __init__(self, pair, bars, events, portfolio, leverage):
        self._x = pair[0]
        self._y = pair[1]
        self.X = KalmanMovingAverage(self._x)
        self.Y = KalmanMovingAverage(self._y)
        self.bars = bars
        self.events = events
        self.initialize_filters()
        self.entry_dt = pd.Timestamp('1900-01-01', tz='utc')
        self.portfolio = portfolio
        self.leverage = leverage

    def calculate_signal(self):
        self.update()
        spreads = self.mean_spread()

        zscore = (spreads.iloc[-1] - spreads.mean()) / spreads.std()

        reference_pos = self.portfolio.get_current_holdings()[self._y]['cost']

        x_symbol = self.x_bar['Symbol']
        x_date = self.x_bar['Date']
        y_symbol = self.y_bar['Symbol']
        y_date = self.y_bar['Date']
        now = x_date
        if reference_pos:
            pnl = self.portfolio.get_pnl([x_symbol,
                                         y_symbol])
            if ((now - self.entry_dt).days > 20) or \
                    (zscore > -0.0 and reference_pos > 0 and pnl > 0) or \
                    (zscore < 0.0 and reference_pos < 0 and pnl > 0):
                x_signal = SignalEvent(x_symbol,
                                       x_date, 'EXIT')
                y_signal = SignalEvent(y_symbol,
                                       y_date, 'EXIT')
                self.events.put(x_signal)
                self.events.put(y_signal)
        else:
            if zscore > 1.5:
                x_signal = SignalEvent(x_symbol,
                                       x_date, 'LONG',
                                       self.leverage / 2.)
                y_signal = SignalEvent(y_symbol,
                                       y_date, 'SHORT',
                                       self.leverage / 2.)
                self.entry_dt = now
                self.events.put(x_signal)
                self.events.put(y_signal)
            if zscore < -1.5:
                x_signal = SignalEvent(x_symbol,
                                       x_date, 'SHORT',
                                       self.leverage / 2.)
                y_signal = SignalEvent(y_symbol,
                                       y_date, 'LONG',
                                       self.leverage / 2.)
                self.entry_dt = now
                self.events.put(x_signal)
                self.events.put(y_signal)

    def update(self):
        self.x_bar = self.bars.get_latest_bars(self._x, N=1)[0]
        self.y_bar = self.bars.get_latest_bars(self._y, N=1)[0]
        self.X.update(self.x_bar['Close'])
        self.Y.update(self.y_bar['Close'])
        self.kf.update(self.X.state_means[-1], self.Y.state_means[-1])

    def mean_spread(self):
        means = self.means_frame()
        beta, alpha = self.kf.state_mean
        return means[self._y] - (beta * means[self._x] + alpha)

    def means_frame(self):
        mu_Y = self.Y.state_means
        mu_X = self.X.state_means
        return pd.DataFrame([mu_Y, mu_X], [self._y, self._x]).T

    def initialize_filters(self):
        prices_x = self.bars.generate_train_set(self._x, 'Close')
        prices_y = self.bars.generate_train_set(self._y, 'Close')
        self.X.update_all(prices_x)
        self.Y.update_all(prices_y)
        self.kf = KalmanRegression(self.X.state_means, self.Y.state_means)


class KalmanMovingAverage(object):
    def __init__(self, asset, observation_covariance=1.0, initial_value=0,
                 initial_state_covariance=1.0, transition_covariance=0.05):
        self.asset = asset
        self.kf = KalmanFilter(
            transition_matrices=[1],
            observation_matrices=[1],
            initial_state_mean=initial_value,
            initial_state_covariance=initial_state_covariance,
            observation_covariance=observation_covariance,
            transition_covariance=transition_covariance)
        self.state_means = [self.kf.initial_state_mean]
        self.state_vars = [self.kf.initial_state_covariance]

    def update_all(self, observations):
        for observation in observations:
            self.update(observation)

    def update(self, observation):
        mu, cov = self.kf.filter_update(self.state_means[-1],
                                        self.state_vars[-1],
                                        observation)
        self.state_means.append(mu.flatten()[0])
        self.state_vars.append(cov.flatten()[0])


class KalmanRegression(object):
    def __init__(self, x_state_mean, y_state_mean, delta=1e-5):
        trans_cov = delta / (1 - delta) * np.eye(2)
        obs_mat = np.expand_dims(
            np.vstack([[x_state_mean],
                       [np.ones(len(x_state_mean))]]).T, axis=1)
        self.kf = KalmanFilter(n_dim_obs=1, n_dim_state=2,
                               initial_state_mean=np.zeros(2),
                               initial_state_covariance=np.ones((2, 2)),
                               transition_matrices=np.eye(2),
                               observation_matrices=obs_mat,
                               observation_covariance=1.0,
                               transition_covariance=trans_cov)
        means, cov = self.kf.filter(y_state_mean)
        self.mean = means[-1]
        self.cov = cov[-1]

    def update(self, x, y):
        self.mean, self.cov = self.kf.filter_update(
            self.mean, self.cov, y,
            observation_matrix=np.array([[x, 1.0]]))

    @property
    def state_mean(self):
        return self.mean


class BollingerBandJohansenStrategy(Strategy):
    """
    Uses a Johansen test to create a mean reverting portfolio,
    and uses bollinger bands strategy using resuling hedge ratios.
    """
    def __init__(self, bars, events, start_date):
        """
        Initialises the bollinger band johansen strategy.

        :param bars: The DataHandler object that provides bar information
        :type bars: DataHandler
        :param events: The Event Queue object.
        :type events: Queue
        """
        self.bars = bars
        self.events = events
        self.current_date = start_date

        # self.hedge_ratio = generate_hedge_ratio(
        #     self.bars.generate_train_set('Close'))
        self.kf = KalmanFilter(transition_matrices=[1],
                               observation_matrices=[1],
                               initial_state_mean=0,
                               initial_state_covariance=1,
                               observation_covariance=1,
                               transition_covariance=0.01)
        self.hedge_ratio = self._find_stationary_portfolio()
        self.long = False
        self.short = False
        self.enter = 2.0

    def _find_stationary_portfolio(self):
        """
        Iterates through s&p stocks sorted by oldest, creating combinations of
        12 stock large portfolios. Generates a hedge ratio for each of these
        portfolios and checks if it creates a stationary time series. If it
        does, will return the hedge ratio, otherwise, will exit program

        :return: hedge ratio
        :rtype: list(float)
        """
        tickers = self.bars.sort_oldest()
        for port in combinations(tickers, 12):
            self.bars.update_symbol_list(port, self.current_date)
            prices = self.bars.generate_train_set_all('Close')
            try:
                results = generate_hedge_ratio(prices)
            except np.linalg.LinAlgError as error:
                logger.info("Error: {error}".format(error=error))
            else:
                hedge_ratio = results.evec[:, 0]
                self.portfolio_prices = dot(prices, hedge_ratio)
                if results.lr1[0] >= results.cvt[0][-1] and\
                    results.lr2[0] >= results.cvm[0][-1] and\
                        is_stationary(self.portfolio_prices):
                    logger.info("Stationary portfolio found. Tickers: {port}"
                                .format(port=port))
                    # self.portfolio_prices = self.portfolio_prices.tolist()
                    # amp = np.fft.fft(self.portfolio_prices)
                    # amp = np.absolute(amp) / len(self.portfolio_prices)
                    # sort_idx = np.argsort(amp)
                    # max_amp_idx = sort_idx[-2]
                    # f = np.fft.fftfreq(len(self.portfolio_prices))
                    # max_freq = f[max_amp_idx]
                    # max_amp = amp[max_amp_idx]
                    # self.rolling_window = int(round(1 / max_freq))
                    state_means, state_cov = self.kf.filter(
                        self.portfolio_prices)
                    state_means = state_means.flatten()
                    state_cov = state_cov.flatten()
                    self.avg = state_means[-1]
                    self.cov = state_cov[-1]
                    self.diff_list = self.portfolio_prices - state_means
                    self.diff_list = self.diff_list.tolist()
                    self.portfolio_prices = self.portfolio_prices.tolist()
                    # self.enter = (max_amp - (
                    #     self.portfolio_prices)) / stdev(
                    #         self.portfolio_prices)
                    # self.enter = (max_amp - self.avg) / self.cov
                    # logger.debug(self.enter)
                    return hedge_ratio
            logger.info("{port} not stationary".format(port=port))
        logger.error("No stationary portfolios found!")
        sys.exit(0)

    def _current_portfolio_price(self, price_type):
        """
        generates the current portfolio price of price_type

        Parameters:
        :param price_type: price type (open, close, high, low)
        :type price_type: str

        :return: current portfolio price
        :rtype: float
        """
        prices = []
        for i, s in enumerate(self.bars.symbol_list):
            bar = self.bars.get_latest_bars(s, N=1)[0]
            self.current_date = bar['Date']
            adj_ratio = bar['Split']
            adj_ratio *= (bar['Close'] + bar['Dividend'])\
                / bar['Close']
            self.hedge_ratio[i] *= adj_ratio
            prices.append(bar[price_type])
        return dot(prices, self.hedge_ratio)

    def _order_portfolio(self, direction):
        """
        creates signal event to order all symbols in symbol list
        with given direction and strength based on hedge ratio

        :param direction: direction of signal ('LONG', 'SHORT', 'EXIT')
        :type direction: str
        """
        for i, s in enumerate(self.bars.symbol_list):
            bar = self.bars.get_latest_bars(s, N=1)[0]
            signal = SignalEvent(bar['Symbol'], bar['Date'], direction,
                                 self.hedge_ratio[i])
            logger.info("Signal Event created for {sym} on {date} to "
                        "{direction} with strength {strength}".format(
                            sym=bar['Symbol'], date=bar['Date'],
                            direction=direction, strength=self.hedge_ratio[i]))
            self.events.put(signal)

    def calculate_signals(self, event):
        """
        Calculates how many standard deviations away
        the current bar is from rolling average of the portfolio.
        If goes above or below defined entry and exit point, will
        long, short, or exit portfolio accordingly. If portfolio is no
        longer stationary, will exit all positions and find new stationary
        portfolio

        :param event: Event object
        :type event: Event
        """
        if event.type == "MARKET":
            price = self._current_portfolio_price('Close')
            self.portfolio_prices.append(price)
            if is_stationary(self.portfolio_prices):
                state_means, state_cov = self.kf.filter_update(
                    self.avg, self.cov, price)
                state_means = state_means.flatten()
                state_cov = state_cov.flatten()
                # rolling_avg = mean(
                #     self.portfolio_prices[-self.rolling_window:])
                self.avg = state_means[-1]
                self.cov = state_cov[-1]
                self.diff_list.append(price - self.avg)
                std = stdev(self.diff_list)
                # rolling_std = stdev(
                #     self.portfolio_prices[-self.rolling_window:])
                zscore = (self.diff_list[-1]) / std
                logger.debug("zscore: {zscore}".format(zscore=zscore))

                if self.long and zscore >= 0.0:
                    self._order_portfolio(direction='EXIT')
                    self.long = False
                elif self.short and zscore <= 0.0:
                    self._order_portfolio(direction='EXIT')
                    self.short = False
                elif not self.short and zscore >= self.enter:
                    self._order_portfolio(direction='SHORT')
                    self.short = True
                elif not self.long and zscore <= -self.enter:
                    self._order_portfolio(direction='LONG')
                    self.long = True
            else:
                logger.info("Portfolio no longer stationary!")
                self._order_portfolio(direction='EXIT')
                self.long = False
                self.short = False
                self.hedge_ratio = self._find_stationary_portfolio()
