#!/usr/bin/python

import numpy as np
import pandas as pd
import sys
import logbook
import pytz
import datetime
import matplotlib.pyplot as plt
from securityList import SecurityList

zipline_logging= logbook.NestedSetup([logbook.NullHandler(level=logbook.DEBUG),logbook.StreamHandler(sys.stdout,level=logbook.INFO),logbook.StreamHandler(sys.stderr,level=logbook.ERROR),])
zipline_logging.push_application()

from zipline import run_algorithm
from zipline.api import order_target_percent, symbol, schedule_function, date_rules, get_open_orders, set_slippage, order
from zipline.finance.slippage import VolumeShareSlippage

def initialize(context):
    set_slippage(VolumeShareSlippage(volume_limit=0.025,price_impact=0.1))
    tickers = ['AYI','APA','AMZN','LNT','CTL','ALB','ABBV','AMT','ADM','AON','ORCL']
    context.tick_list = tickers
    context.tickers = [ symbol(x) for x in tickers ]
    context.long= False
    context.short= False
    start = datetime.datetime(2013,1,3)
    end = datetime.datetime(2017,8,1)
    sec_list = SecurityList(tickers=tickers)
    sec_list.downloadQuandl(start,end)
    ts,context.hedgeRatio,context.df_close = sec_list.genTimeSeries()
    context.avg = ts.mean()
    context.std = ts.std()
    context.volume = sec_list.getVolume()
    schedule_function(place_orders, date_rules.every_day())
    
def place_orders(context,data): 

    def adjustHedgeRatio(hedge):
        """
        Adjusts the HedgeRatio to account for slippage model (Volume Limit)

        Will not order unless the order can be filled the same day

        Adjusts for the lowest common denominator
        """
        volume = context.volume.loc[pd.to_datetime(context.get_datetime()).date()]
        for i, tick in enumerate(context.tickers):
            max_order = volume[context.tick_list[i]]*0.025
            adj_hedge = (max_order*data.current(tick,'price')) \
                        /context.portfolio.portfolio_value
            order_size = abs(context.portfolio.portfolio_value * hedge[i]) \
                            /data.current(tick,'price')
            if order_size > max_order:
                hedge = adjustHedgeRatio(hedge*adj_hedge)
                break
        return hedge

    def computeCost(S):
        """
        Computes cost function given the number of shares S
        """
        numerator = data.current(context.tickers,'price')*S
        denominator = np.sum(numerator)
        numerator -= np.absolute(context.hedgeRatio)
        cost = numerator/denominator
        cost = np.sum(cost**2)
        return cost

    def order_target_portfolio_percentages(order_type):
        """
        Use gradient descent to minimize difference function between
        portfolio weights and hedge ratio
        """
        shares = (context.portfolio.portfolio_value*context.hedgeRatio)/data.current(context.tickers,'price')
        shares = np.absolute(shares.values)
        A = np.dot(data.current(context.tickers,'price'),shares)
        cost = computeCost(shares)
        alpha = 10000
        while cost >= 0.14:
            print('Current Cost Function = ',cost)
            for i,tick in enumerate(context.tickers):
                new_tickers = context.tickers[:i] + context.tickers[i+1:]
                new_shares = np.delete(shares,i,0)
                new_hedgeRatio = np.delete(np.absolute(context.hedgeRatio),i,0)
                Pi = data.current(tick,'price')
                B = data.current(new_tickers,'price')*new_shares
                C = np.sum(B)
                gradient = (Pi * (shares[i] * C + np.sum(B**2)))/A**3 \
                            - (np.absolute(context.hedgeRatio[i]) * C \
                            + (Pi * np.dot(new_hedgeRatio,B)))/A**2
                shares[i] -= alpha*gradient
            cost = computeCost(shares)
        shares[context.hedgeRatio < 0 ] *= -1
        for i,tick in enumerate(context.tickers):
            if order_type == 'long':
                order(tick,shares[i])
            if order_type == 'short':
                order(tick,-shares[i])

    def orderPortfolio(order_type):
        for i,tick in enumerate(context.tickers):
            if order_type == 'long':
                order_target_percent(tick,context.hedgeRatio[i])
            if order_type == 'short':
                order_target_percent(tick,-context.hedgeRatio[i])
            if order_type == 'exit':
                order_target_percent(tick,0)

    #def calculatePositionsValue():
    #    positions = context.portfolio.positions
    #    total = 0
    #    for tick in context.tickers:
    #        total += abs(positions[tick]['amount']*positions[tick]['cost_basis'])
    #    return total

    #orderPortfolio(order_type='long')

    #for i,tick in enumerate(context.tickers):
    #    positions_value = calculatePositionsValue()
    #    if positions_value != 0:
    #        percent = (context.portfolio.positions[tick]['amount'] * context.portfolio.positions[tick]['cost_basis'])/calculatePositionsValue()
    #        print(str(tick)+': '+str(percent)+'%    '+str(context.hedgeRatio[i]))

    #print('\n\n')


    current_price = np.dot(context.df_close.loc[pd.to_datetime(context.get_datetime()).date()],context.hedgeRatio)
    zscore = (current_price-context.avg)/context.std
 
    if context.long == True and zscore >= 0:
        orderPortfolio(order='exit')
        context.long = False
    elif context.short == True and zscore <= 0:
        orderPortfolio(order='exit')
        context.short = False
    elif context.short == False and zscore >= 2:
        orderPortfolio(order='short')
        context.short= True
    elif context.long == False and zscore <= 2:
        orderPortfolio(order='long')
        context.long= True

eastern = pytz.timezone('US/Eastern')        
start= datetime.datetime(2013,1,3,0,0,0,0,eastern)
end = datetime.datetime(2017,8,1,0,0,0,0,eastern)

results= run_algorithm(start=start,end=end,initialize=initialize,capital_base=100000,bundle='quantopian-quandl')

plt.figure()
plt.plot(results.portfolio_value)
plt.title('Portfolio Value')
plt.figure()
plt.subplot(2,1,1)
plt.plot(results.benchmark_period_return)
plt.plot(results.algorithm_period_return)
plt.title('Benchmark Returns vs. Algo Returns')
plt.legend(['Benchmark Returns','Algo Returns'])
plt.subplot(2,1,2)
plt.plot(results.sharpe)
plt.title('Rolling Sharpe')
plt.show()
