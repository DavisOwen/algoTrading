#!/usr/bin/env python

from queue import Queue
import os
import datetime
import logging

from data import QuandlDataHandler
from strategy import BollingerBandJohansenStrategy
from portfolio import NaivePortfolio
from execution import SimulatedExecutionHandler

# Logger
logger = logging.getLogger("backtester")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
logger.addHandler(ch)

# Paramters
events = Queue()
pickle_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                          'pickle_files')
symbol_list = ['ARNC', 'BA', 'CAT', 'DD', 'DIS', 'GE', 'HPQ', 'IBM', 'KO',
               'AEP', 'CNP', 'CVX']
train_date = datetime.datetime(1970, 1, 2)
test_date = datetime.datetime(1980, 1, 2)
enter = 0.5
exit = 0

# Objects
bars = QuandlDataHandler(events, pickle_dir, symbol_list,
                         train_date, test_date)
bars.generate_train_set()
strategy = BollingerBandJohansenStrategy(bars, events, enter, exit)
port = NaivePortfolio(bars, events, train_date)
broker = SimulatedExecutionHandler(events)

logger.info("Starting Backtest on {date}".format(test_date))

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
            event = events.get(False)
        except Queue.Empty:
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
