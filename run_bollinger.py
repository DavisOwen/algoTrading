#!/usr/bin/env python

import datetime
from queue import Queue
from data import QuandlAPIDataHandler
from strategy import BollingerBandJohansenStrategy
from portfolio import NaivePortfolio
from execution import SimulatedExecutionHandler
from backtester import Backtester

# Paramters
events = Queue()
# symbol_list = ['ARNC', 'BA', 'CAT', 'DD', 'DIS', 'GE', 'HPQ', 'IBM', 'KO',
#                'AEP', 'CNP', 'CVX']
# symbol_list = ['AYI', 'APA', 'AMZN', 'LNT', 'CTL',
#                'ALB', 'ABBV', 'AMT', 'ADM', 'AON', 'ORCL']
symbol_list = []
start_date = datetime.datetime(2017, 1, 2)
enter = 0.5
exit = 0

# Objects
bars = QuandlAPIDataHandler(events, symbol_list, start_date, False)
strategy = BollingerBandJohansenStrategy(bars, events, enter, exit, start_date)
port = NaivePortfolio(bars, events, start_date)
broker = SimulatedExecutionHandler(events, bars)

backtester = Backtester(events, bars, strategy, port, broker)
backtester.start()
backtester.show_performance()
