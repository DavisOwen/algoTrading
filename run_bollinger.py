#!/usr/bin/env python

import datetime
from queue import Queue
from backtester.data import QuandlAPIDataHandler
from backtester.strategy import BollingerBandJohansenStrategy
from backtester.portfolio import NaivePortfolio
from backtester.execution import SimulatedExecutionHandler
from backtester.backtester import Backtester

# Paramters
events = Queue()
symbol_list = []
start_date = datetime.datetime(2017, 1, 2)

# Objects
bars = QuandlAPIDataHandler(events, symbol_list, start_date, False)
strategy = BollingerBandJohansenStrategy(bars, events, start_date)
port = NaivePortfolio(bars, events, start_date)
broker = SimulatedExecutionHandler(events, bars)

backtester = Backtester(events, bars, strategy, port, broker)
backtester.start()
backtester.show_performance()
