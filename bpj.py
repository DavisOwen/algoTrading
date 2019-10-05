#!/usr/bin/env python

'''
Bollinger Pair Johansen,
using Johansen test to create a
mean reverting portfolio, and
using bollinger bands strategy
to backtest
'''

import matplotlib.pyplot as plt
import numpy as np
import sys
import logbook
import pytz
import datetime
from securityList import SecurityList
from utils import plot_results, get_oldest
from zipline import run_algorithm
from zipline.api import symbol, schedule_function, date_rules, time_rules, \
    set_slippage, order_target, set_commission
from zipline.finance.slippage import VolumeShareSlippage
from zipline.finance.commission import PerShare
zipline_logging = logbook.NestedSetup([
    logbook.NullHandler(level=logbook.DEBUG),
    logbook.StreamHandler(sys.stdout, level=logbook.INFO),
    logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
])
zipline_logging.push_application()

zEnter = 0.5
zExit = 0
dates = list()
prices = list()
colors = list()
real_prices = list()


def initialize(context):

    # https://www.quantopian.com/help#ide-slippage
    set_slippage(VolumeShareSlippage(volume_limit=0.025, price_impact=0.1))
    # https://www.quantopian.com/help#ide-commission
    set_commission(PerShare(cost=0.001, min_trade_cost=0))

    # train set start and end
    start = datetime.datetime(1970, 1, 2)
    train_end = datetime.datetime(2000, 3, 11)

    # backtest end
    bt_end = datetime.datetime(2017, 3, 11)

    # tickers = ['AYI', 'APA', 'AMZN', 'LNT', 'CTL',
    #            'ALB', 'ABBV', 'AMT', 'ADM', 'AON', 'ORCL']
    tickers = get_oldest()
    tickers = tickers[1:12]
    sec_list = SecurityList(tickers=tickers)
    sec_list.set_train(start, train_end)
    sec_list.load_data(start, bt_end)
    context.hedge_ratio = sec_list.genHedgeRatio()
    global ts
    ts = sec_list.genTimeSeries()
    plt.figure()
    plt.plot(ts)
    plt.show()
    context.adj_close = sec_list.get_adj_close()
    global dates_index
    dates_index = context.adj_close.index
    context.volume = sec_list.get_volume()
    context.tickers = [symbol(x) for x in tickers]
    context.long = False
    context.short = False
    context.leverage = 1
    context.share_num = 0
    context.diff_thresh = 100
    schedule_function(
        place_orders,
        date_rules.every_day(),
        time_rules.market_open(hours=1, minutes=30)
    )


def place_orders(context, data):

    today = context.get_datetime().date()
    start_window = today - datetime.timedelta(days=500)
    adj_close = context.adj_close.loc[today]
    current_price = np.dot(adj_close, context.hedge_ratio)

    def calc_target_share_ratio():
        """
        Calculates the target share ratio
        based on the given hedge ratio

        """

        shares = context.hedge_ratio * context.share_num
        port_val = np.dot(np.absolute(shares), adj_close)
        diff = context.leverage * context.portfolio.portfolio_value - port_val
        volume = context.volume.loc[today]
        max_vol = 0.025 * volume
        while diff > context.diff_thresh and \
                diff > 0 and (max_vol > shares).all():
            context.share_num += context.leverage
            shares = context.hedge_ratio * context.share_num
            port_val = np.dot(np.absolute(shares), adj_close)
            diff = context.leverage * context.portfolio.portfolio_value \
                - port_val
        while (diff < 0 or not (max_vol > shares).all()) \
                and context.share_num >= 0:
            context.share_num -= context.leverage
            shares = context.hedge_ratio * context.share_num
            port_val = np.dot(np.absolute(shares), adj_close)
            diff = context.leverage * context.portfolio.portfolio_value \
                - port_val
        return shares

    def orderPortfolio(order_type):

        shares = calc_target_share_ratio()
        for i, tick in enumerate(context.tickers):
            dates.append(today)
            current_price = np.dot(adj_close, context.hedge_ratio)
            real_prices.append(
                np.dot(data.current(context.tickers, 'price'), shares))
            prices.append(current_price)
            if order_type == 'long':
                colors.append('r')
                order_target(tick, shares[i])
            if order_type == 'short':
                colors.append('b')
                order_target(tick, -shares[i])
            if order_type == 'exit':
                colors.append('g')
                order_target(tick, 0)

    adj_close = context.adj_close.loc[today]
    rolling_prices = context.adj_close.loc[start_window:today]
    rolling_avg = np.mean(np.dot(rolling_prices, context.hedge_ratio))
    rolling_std = np.std(np.dot(rolling_prices, context.hedge_ratio))
    current_price = np.dot(adj_close, context.hedge_ratio)
    zscore = (current_price - rolling_avg) / rolling_std

    if context.long and zscore >= zExit:
        orderPortfolio(order_type='exit')
        context.long = False
    elif context.short and zscore <= -zExit:
        orderPortfolio(order_type='exit')
        context.short = False
    elif not context.short and zscore >= zEnter:
        orderPortfolio(order_type='short')
        context.short = True
    elif not context.long and zscore <= -zEnter:
        orderPortfolio(order_type='long')
        context.long = True


def main(Enter, Exit):

    zEnter = Enter
    zExit = Exit
    eastern = pytz.timezone('US/Eastern')
    # this is the earliest possible date
    start = datetime.datetime(1970, 1, 3, 0, 0, 0, 0, eastern)
    # this is the last good date for quandl dataset
    end = datetime.datetime(2017, 3, 11, 0, 0, 0, 0, eastern)
    results = run_algorithm(start=start, end=end, initialize=initialize,
                            capital_base=10000, bundle='quantopian-quandl')
    plot_results(results)
    plt.figure()
    plt.plot(dates_index, ts)
    # plt.scatter(dates,prices,color=colors)
    plt.scatter(dates, real_prices)
    plt.figure()
    plt.show()
    return results.sharpe[-1]


if __name__ == "__main__":
    res = main(zEnter, zExit)
    print(res)
