#!/usr/bin/env python

from queue import Queue, Empty
import os
import datetime
import logging

from performance import PerformanceHandler
from data import QuandlAPIDataHandler
from strategy import BollingerBandJohansenStrategy
from portfolio import NaivePortfolio
from execution import SimulatedExecutionHandler

# Paramters
events = Queue()
pickle_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "pickle_files")
logger_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          "logs")
results_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                           "results")
# symbol_list = ['ARNC', 'BA', 'CAT', 'DD', 'DIS', 'GE', 'HPQ', 'IBM', 'KO',
#                'AEP', 'CNP', 'CVX']
# symbol_list = ['AYI', 'APA', 'AMZN', 'LNT', 'CTL',
#                'ALB', 'ABBV', 'AMT', 'ADM', 'AON', 'ORCL']
symbol_list = []
start_date = datetime.datetime(2017, 1, 2)
enter = 0.5
exit = 0

# Logging and performance objects
performance = PerformanceHandler(results_dir)
performance.construct_logger(logger_dir)
logger = logging.getLogger("backtester")

# Objects
bars = QuandlAPIDataHandler(events, pickle_dir, symbol_list,
                            start_date, False)
strategy = BollingerBandJohansenStrategy(bars, events, enter, exit, start_date)
port = NaivePortfolio(bars, events, start_date)
broker = SimulatedExecutionHandler(events, bars)

logger.info("Starting Backtest on {date}".format(date=start_date))

# Main Loop
while True:
    # Update the bars (specific backtest code, as opposed to live trading)
    if bars.continue_backtest:
        bars.update_bars()
    else:
        break

    # Handle the events
    while True:
        try:
            event = events.get(block=False)
        except Empty:
            break
        else:
            if event is not None:
                if event.type == 'MARKET':
                    strategy.calculate_signals(event)
                    port.update_timeindex(event)
                elif event.type == 'SIGNAL':
                    port.update_signal(event)
                elif event.type == 'ORDER':
                    broker.execute_order(event)
                elif event.type == 'FILL':
                    port.update_fill(event)

logger.info("Backtest completed")
results = port.create_results_dataframe()
performance.save_results(results)
results.plot_equity_curve()
results.output_summary_stats
