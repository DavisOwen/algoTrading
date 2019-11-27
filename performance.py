import os
import sys
import pickle
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters


register_matplotlib_converters()
logger = logging.getLogger("backtester")


class PerformanceHandler(object):
    """
    Object for saving and loading backtest results and log files.
    Includes functions to analyze results such as plot_equity_curve
    """
    def __init__(self, results_dir):
        """
        Initializes PerformanceHandler. Loads current backtest number or
        creates new backtest number starting from 0. Sets results directory

        :param results_dir: Directory to store and load backtest results
        :type results_dir: str
        """
        try:
            self.backtest_number = pickle.load(
                open("backtest_number.pickle", 'rb'))
        except FileNotFoundError:
            self.backtest_number = 0
            pickle.dump(
                self.backtest_number,
                open("backtest_number.pickle", 'wb'))
        self.results_dir = results_dir

    def construct_logger(self, logger_dir):
        """
        Constructs logger with file handler with backtest_number, stream
        handler, and formatter

        :param logger_dir: directory to store log file
        :type logger_dir: str
        """
        self.backtest_number += 1
        pickle.dump(
            self.backtest_number,
            open("backtest_number.pickle", 'wb'))
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("{asctime} {levelname} {message}",
                                      style='{')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        file = logging.FileHandler(os.path.join(
            logger_dir, "backtest_{num}.log".format(num=self.backtest_number)))
        file.setFormatter(formatter)
        logger.addHandler(ch)
        logger.addHandler(file)

    def save_results(self, results):
        """
        saves results to pickle file keyed based on backtest_number

        :param results: results object to save
        :type results: pd.Dataframe
        """
        self.results = results
        pickle.dump(self.results, open(os.path.join(
            self.results_dir,
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
                os.path.join(self.results_dir, "backtest_{num}.pickle".format(
                    num=self.backtest_number)), 'rb'))
        except FileNotFoundError:
            self.backtest_number -= 1
            if self.backtest_number == 0:
                logger.error("no backtest files found!")
                sys.exit(0)
            pickle.dump(
                self.backtest_number,
                open("backtest_number.pickle", 'wb'))
            self.load_results()

    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio such
        as Sharpe Ratio and drawdown information.
        """
        total_return = self.results['equity_curve'][-1]
        returns = self.results['returns']
        pnl = self.results['equity_curve']

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
        """
        plt.plot(self.results['equity_curve'])
        plt.show()
