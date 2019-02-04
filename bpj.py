#!algotrading/bin/python3

'''
Bollinger Pair Johansen,
using Johansen test to create a 
mean reverting portfolio, and 
using bollinger bands strategy 
to backtest
'''

import numpy as np
import pandas as pd
import sys
import logbook
import pytz
import datetime
from securityList import SecurityList
from utils import plot_results
from zipline import run_algorithm
from zipline.api import order_target_percent, symbol, schedule_function, date_rules, time_rules, set_slippage, order_target, set_commission
from zipline.finance.slippage import VolumeShareSlippage
from zipline.finance.commission import PerShare
zipline_logging= logbook.NestedSetup([logbook.NullHandler(level=logbook.DEBUG),logbook.StreamHandler(sys.stdout,level=logbook.INFO),logbook.StreamHandler(sys.stderr,level=logbook.ERROR),])
zipline_logging.push_application()

zEnter = 0.5
zExit = 0

def initialize(context):

    # https://www.quantopian.com/help#ide-slippage
    set_slippage(VolumeShareSlippage(volume_limit=0.025,price_impact=0.1))
    
    # https://www.quantopian.com/help#ide-commission
    set_commission(PerShare(cost=0.001, min_trade_cost=0))

    tickers = ['AYI','APA','AMZN','LNT','CTL','ALB','ABBV','AMT','ADM','AON','ORCL']
    context.tickers = [ symbol(x) for x in tickers ]
    context.long = False
    context.short = False

    # train set start and end
    start = datetime.datetime(2013,1,3)
    train_end = datetime.datetime(2016,1,1)

    # backtest end
    bt_end = datetime.datetime(2017,11,3)

    sec_list = SecurityList(tickers = tickers, start = start, end = train_end)
    sec_list.loadData(start, bt_end)

    # Factors used to maintain mean reverting behavior
    # of prices as time goes on and stocks go through
    # dividend payments and splits
    context.adjDiv = sec_list.adjDividends()
    context.adjHedge = sec_list.adjSplits()
    context.divFactors = sec_list.getAdjFactors()
    context.splits = sec_list.getSplits()

    context.leverage = 1
    context.share_num = 0
    context.diff_thresh = 100
    schedule_function(adjust_splits_dividends, date_rules.every_day(), time_rules.market_open())
    schedule_function(place_orders, date_rules.every_day(), time_rules.market_open(hours=1, minutes=30))

def adjust_splits_dividends(context,data):

    '''
    Adjusts the hedge ratio based on splits and adjusts the 
    prices based on dividend payments

    '''

    splits = context.splits.loc[context.get_datetime().date()]
    divFactors = context.divFactors.loc[context.get_datetime().date()]
    context.adjHedge *= splits
    context.adjDiv /= divFactors

def place_orders(context,data): 

    def calc_target_share_ratio(leverage):

        '''
        Calculates the target share ratio
        based on the given hedge ratio

        '''

        shares = context.adjHedge*context.share_num
        port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
        diff = leverage*context.portfolio.portfolio_value - port_val
        curr_vol = data.current(context.tickers,'volume')
        max_vol = 0.025*curr_vol
        while diff > context.diff_thresh and diff > 0 and (max_vol.reset_index(drop=True) > shares.reset_index(drop=True)).all():
            context.share_num += 1*context.leverage
            shares = context.adjHedge*context.share_num
            port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
            diff = leverage*context.portfolio.portfolio_value - port_val
        while diff < 0 or not (max_vol.reset_index(drop=True) > shares.reset_index(drop=True)).all():
            context.share_num -= 1*context.leverage
            shares = context.adjHedge * context.share_num
            port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
            diff = leverage*context.portfolio.portfolio_value - port_val
        return shares

    def orderPortfolio(order_type,leverage):

        shares = calc_target_share_ratio(leverage)
        for i,tick in enumerate(context.tickers):
            if order_type == 'long':
                order_target(tick,shares[i])
            if order_type == 'short':
                order_target(tick,-shares[i])
            if order_type == 'exit':
                order_target(tick,0)

    # Dividend adjusted prices
    div_adj_price = data.current(context.tickers,'price').values/context.adjDiv.values

    rolling_prices = data.history(context.tickers,'price',250,'1d').values/context.adjDiv.values
    rolling_avg = np.mean(np.dot(rolling_prices,context.adjHedge))
    rolling_std = np.std(np.dot(rolling_prices,context.adjHedge))
    current_price = np.dot(div_adj_price,context.adjHedge)
    zscore = (current_price-rolling_avg)/rolling_std
 
    if context.long == True and zscore >= zExit:
        orderPortfolio(order_type='exit',leverage = context.leverage)
        context.long = False
    elif context.short == True and zscore <= -zExit:
        orderPortfolio(order_type='exit',leverage = context.leverage)
        context.short = False
    elif context.short == False and zscore >= zEnter:
        orderPortfolio(order_type='short',leverage = context.leverage)
        context.short= True
    elif context.long == False and zscore <= -zEnter:
        orderPortfolio(order_type='long',leverage = context.leverage)
        context.long= True

def main(Enter, Exit):

    zEnter = Enter
    zExit = Exit
    eastern = pytz.timezone('US/Eastern')        
    start= datetime.datetime(2016,1,2,0,0,0,0,eastern)
    end = datetime.datetime(2017,11,3,0,0,0,0,eastern) # this is the last good date for quandl dataset
    results = run_algorithm(start=start,end=end,initialize=initialize,capital_base=10000,bundle='quantopian-quandl')
    plot_results(results)
    return results.sharpe[-1]

if __name__ == "__main__":
    res = main(zEnter, zExit)
    print(res)
