#!algotrading/bin/python3

'''
Runs a backtest using ARMA model

'''

import sys
import datetime
import logbook
import numpy as np
import matplotlib.pyplot as plt
import pytz
from securityList import SecurityList
from statsmodels.tsa.arima_model import ARMA
from zipline import run_algorithm
from zipline.api import order_target_percent, symbol, schedule_function, \
    date_rules

zipline_logging = logbook.NestedSetup([
    logbook.NullHandler(level=logbook.DEBUG),
    logbook.StreamHandler(sys.stdout, level=logbook.INFO),
    logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
])
zipline_logging.push_application()


def initialize(context):

    tickers = ['AYI', 'APA', 'AMZN', 'LNT', 'CTL',
               'ALB', 'ABBV', 'AMT', 'ADM', 'AON', 'ORCL']
    context.tickers = [symbol(x) for x in tickers]
    schedule_function(place_orders, date_rules.every_day())
    context.days = int()
    context.return_thresh = 0.02
    context.hasOrdered = False
    start = datetime.datetime(2013, 1, 3)
    end = datetime.datetime(2017, 8, 1)
    sec_list = SecurityList(tickers=tickers)
    sec_list.downloadQuandl(start, end)
    context.hedgeRatio = sec_list.genHedgeRatio()
    context.predicted = list()


def before_trading_start(context, data):

    if context.hasOrdered:
        pass
    else:
        close = list()
        for tick in context.tickers:
            close.append(data.history(tick, 'close', 252+context.days, '1d'))
        sec_list = SecurityList(data=close)
        ts, hedgeRatio, matrix = sec_list.genTimeSeries()
        plt.plot(ts)
        plt.show()
        history = [x for x in ts]
        for t in range(252):
            model = ARMA(history, order=(5, 0))
            model_fit = model.fit(disp=0)
            output = model_fit.forecast()
            context.predicted.append(output[0])
            history.append(output[0])
        plt.plot(context.predicted)
        plt.show()

    context.days += 1


def place_orders(context, data):

    def orderPortfolio(order):

        for i, tick in enumerate(context.tickers):
            if order == 'long':
                order_target_percent(tick, context.hedgeRatio[i])
            if order == 'short':
                order_target_percent(tick, -context.hedgeRatio[i])
            if order == 'exit':
                order_target_percent(tick, 0)

    if context.hasOrdered:
        context.position_len += 1
        if context.position_len == context.exit_day:
            orderPortfolio(order='exit')
            context.hasOrdered = False
    else:
        close = list()
        for tick in context.tickers:
            close.append(np.array([data.current(tick, 'price')]))
        sec_list = SecurityList(data=close)
        matrix = sec_list.genMatrix()
        current_price = np.dot(matrix, context.hedgeRatio)
        rolling_max = max(context.predicted)
        rolling_min = min(context.predicted)
        rolling_max_i = np.where(context.predicted == rolling_max)
        rolling_min_i = np.where(context.predicted == rolling_min)
        long_returns = (rolling_max - current_price)/current_price
        short_returns = (current_price - rolling_min)/current_price
        if (long_returns - short_returns) > context.return_thresh:
            orderPortfolio(order='long')
            context.hasOrdered = True
            context.exit_day = rolling_max_i
            context.position_len = int()
        elif (short_returns - long_returns) > context.return_thresh:
            orderPortfolio(order='short')
            context.hasOrdered = True
            context.exit_day = rolling_min_i
            context.position_len = int()


eastern = pytz.timezone('US/Eastern')
start = datetime.datetime(2014, 1, 1, 0, 0, 0, 0, eastern)
end = datetime.datetime(2017, 8, 1, 0, 0, 0, 0, eastern)

results = run_algorithm(start=start, end=end,
                        initialize=initialize, capital_base=10000,
                        before_trading_start=before_trading_start)

plt.figure()
plt.plot(results.portfolio_value)
plt.title('Portfolio Value')
plt.figure()
plt.subplot(2, 1, 1)
plt.plot(results.benchmark_period_return)
plt.plot(results.algorithm_period_return)
plt.title('Benchmark Returns vs. Algo Returns')
plt.legend(['Benchmark Returns', 'Algo Returns'])
plt.subplot(2, 1, 2)
plt.plot(results.sharpe)
plt.title('Rolling Sharpe')
plt.show()
