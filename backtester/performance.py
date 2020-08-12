import os
import sys
import pickle
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import csv
from pandas.plotting import register_matplotlib_converters
from .utils import check_dir_exists, PickleType

register_matplotlib_converters()
logger_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "logs")
results_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           "results")
benchmark_file = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                              "SP500.csv")
check_dir_exists(logger_dir)
check_dir_exists(results_dir)
logger = logging.getLogger("backtester")


class PerformanceHandler(object):
    """
    Object for saving and loading backtest results and log files.
    Includes functions to analyze results such as plot_equity_curve
    """
    def __init__(self):
        """
        Initializes PerformanceHandler. Loads current backtest number or
        creates new backtest number starting from 0. Sets results directory
        """
        try:
            self._pickle_backtest_number(PickleType.LOAD)
        except FileNotFoundError:
            self.backtest_number = 0
            self._pickle_backtest_number(PickleType.DUMP)
        self._construct_logger()

    def _create_benchmark_data(self, returns):
        """
        Creates benchmark equity curve to compare with
        returns. Will need custom data provided.
        https://www.wsj.com/market-data/quotes/index/SPX/historical-prices
        """
        benchmark = pd.read_csv(benchmark_file)
        benchmark.set_index('Date', inplace=True)
        benchmark_returns = benchmark[' Close'].pct_change()
        benchmark_equity_curve = (1.0+benchmark_returns).cumprod()
        benchmark_equity_curve = benchmark_equity_curve.reindex(pd.to_datetime(benchmark_equity_curve.index))
        benchmark_equity_curve = benchmark_equity_curve.iloc[::-1]
        benchmark_equity_curve = benchmark_equity_curve[returns.index[0]:returns.index[-1]]
        return benchmark_equity_curve

    def _construct_logger(self):
        """
        Constructs logger with file handler with backtest_number, stream
        handler, and formatter
        """
        self.backtest_number += 1
        self._pickle_backtest_number(PickleType.DUMP)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("{asctime} {levelname} {message}",
                                      style='{')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        log_file = logging.FileHandler(os.path.join(
            logger_dir, "backtest_{num}.log".format(num=self.backtest_number)))
        log_file.setFormatter(formatter)
        logger.addHandler(ch)
        logger.addHandler(log_file)

    def _pickle_backtest_number(self, pickle_type):
        if pickle_type == PickleType.DUMP:
            pickle.dump(
                self.backtest_number,
                open(os.path.join(logger_dir, "backtest_number.pickle"), 'wb'))
        elif pickle_type == PickleType.LOAD:
            self.backtest_number = pickle.load(
                open(os.path.join(logger_dir, "backtest_number.pickle"), 'rb'))

    def save_results(self, results):
        """
        saves results to pickle file keyed based on backtest_number
        :param results: results object to save
        :type results: pd.Dataframe """
        self.results = results
        pickle.dump(self.results, open(os.path.join(
            results_dir,
            "backtest_{num}.pickle".format(num=self.backtest_number)), 'wb'))

    def load_results(self):
        """
        loads result object and creates simply stream handler for logging
        """
        logger.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        logger.addHandler(ch)
        try:
            self.results = pickle.load(open(
                os.path.join(results_dir, "backtest_{num}.pickle".format(
                    num=self.backtest_number)), 'rb'))
        except FileNotFoundError:
            self.backtest_number -= 1
            if self.backtest_number == 0:
                logger.error("no backtest files found!")
                sys.exit(0)
            self._pickle_backtest_number(PickleType.DUMP)
            self.load_results()

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio such
        as Sharpe Ratio and drawdown information.
        """
        holdings = self.results['holdings']
        total_return = holdings['equity_curve'][-1]
        returns = holdings['returns']
        pnl = holdings['equity_curve']

        sharpe_ratio = self._create_sharpe_ratio(returns)
        max_dd, dd_duration = self._create_drawdowns(pnl)

        stats = [("Total Return", "{0:.2f}".format(
            ((total_return - 1.0) * 100.0))),
                ("Sharpe Ratio", "{0:.2f}".format(sharpe_ratio)),
                ("Max Drawdown", "{0:.2f}".format(max_dd * 100.0)),
                ("Drawdown Duration", "{0:.0f}".format(dd_duration))]
        logger.info(stats)

    def _create_sharpe_ratio(self, returns, periods=252):
        """
        Create the Sharpe ratio for the strategy, based on a
        benchmark of zero (i.e. no risk-free rate information).

        :param returns: a pandas Series representing period percentage returns
        :type returns: pd.Series
        :param periods: Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc
        :type periods: int

        :return: sharpe ratio
        :rtype: float
        """
        return np.sqrt(periods) * (np.mean(returns) / np.std(returns))

    def _create_drawdowns(self, equity_curve):
        """
        Calculate the largest peak-to-trough drawdown of the PnL curve
        as well as the duration of the drawdown. Requires that the
        pnl_returs is a pandas Series.

        :param equity_curve: a pandas Series representing period percentage \
        returns.
        :type equity_curve: pd.Series

        :return: drawdown, duration - highest peak-to-trough drawdown and \
        duration
        :rtype: float, float
        """
        # Calculate the cumulative returns curve
        # and set up the High Water Mark
        # Then create the drawdown and duration series
        hwm = [0]
        eq_idx = equity_curve.index
        drawdown = pd.Series(index=eq_idx)
        duration = pd.Series(index=eq_idx)

        # Loop over the index range
        for t in range(1, len(eq_idx)):
            cur_hwm = max(hwm[t-1], equity_curve[t])
            hwm.append(cur_hwm)
            drawdown[t] = hwm[t] - equity_curve[t]
            duration[t] = 0 if drawdown[t] == 0 else duration[t-1] + 1
        return drawdown.max(), duration.max()

    def plot_equity_curve(self):
        """
        Plots the equity_curve using matplotlib.pyplot
        Green dots represent points of buying, red
        represents points of selling
        """
        holdings = self.results['holdings']
        positions = self.results['positions']
        returns = holdings['equity_curve']
        benchmark_equity_curve = self._create_benchmark_data(returns)
        longs = positions.diff()[
            positions.diff() > 0].dropna(how='all').index.values
        shorts = positions.diff()[
            positions.diff() < 0].dropna(how='all').index.values
        plt.plot(returns)
        plt.plot(benchmark_equity_curve)
        plt.scatter(longs, returns[returns.index.isin(longs)], c='green')
        plt.scatter(shorts, returns[returns.index.isin(shorts)], c='red')
        plt.show()
