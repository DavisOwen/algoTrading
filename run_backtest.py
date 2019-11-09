#!/usr/bin/env python

from queue import Queue, Empty
import os
import datetime
import logging

from data import QuandlAPIDataHandler
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
# symbol_list = ['ARNC', 'BA', 'CAT', 'DD', 'DIS', 'GE', 'HPQ', 'IBM', 'KO',
#                'AEP', 'CNP', 'CVX']
# symbol_list = ['AYI', 'APA', 'AMZN', 'LNT', 'CTL',
#                'ALB', 'ABBV', 'AMT', 'ADM', 'AON', 'ORCL']
symbol_list = []
start_date = datetime.datetime(2000, 1, 2)
enter = 0.5
exit = 0

# Objects
bars = QuandlAPIDataHandler(events, pickle_dir, symbol_list,
                            start_date)
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
