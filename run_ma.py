#!/usr/bin/env python

import datetime
from queue import Queue
from data import QuandlAPIDataHandler
from strategy import MovingAverageCrossoverStrategy
from portfolio import NaivePortfolio
from execution import SimulatedExecutionHandler
from backtester import Backtester

# Paramters
events = Queue()
symbol_list = ["AMZN"]
start_date = datetime.datetime(2017, 1, 2)

# Objects
bars = QuandlAPIDataHandler(events, symbol_list, start_date, False)
strategy = MovingAverageCrossoverStrategy(bars, events, start_date)
port = NaivePortfolio(bars, events, start_date)
broker = SimulatedExecutionHandler(events, bars)

backtester = Backtester(events, bars, strategy, port, broker)
backtester.start()
backtester.show_performance()
