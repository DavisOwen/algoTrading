#!/usr/bin/env python

import datetime
from queue import Queue
from data import QuandlAPIDataHandler
from strategy import KalmanPairTradeStrategy
from portfolio import NaivePortfolio
from execution import SimulatedExecutionHandler
from backtester import Backtester

# Paramters
events = Queue()
symbol_list = ['STX', 'WDC', 'CBI', 'JEC', 'MAS', 'VMC', 'JPM', 'C', 'AON',
               'MMC', 'COP', 'CVX']
pairs = [('STX', 'WDC'), ('CBI', 'JEC'), ('MAS', 'VMC'), ('JPM', 'C'),
         ('AON', 'MMC'), ('COP', 'CVX')]
start_date = datetime.datetime(2017, 1, 2)

# Objects
bars = QuandlAPIDataHandler(events, symbol_list, start_date, False)
port = NaivePortfolio(bars, events, start_date)
strategy = KalmanPairTradeStrategy(bars, port, events, pairs)
broker = SimulatedExecutionHandler(events, bars)

backtester = Backtester(events, bars, strategy, port, broker)
backtester.start()
backtester.show_performance()
