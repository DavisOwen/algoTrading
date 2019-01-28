#!algotrading/bin/python3

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
from zipline.api import order_target_percent, symbol, schedule_function, date_rules, time_rules, set_slippage, order_target
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
    ts = sec_list.genTimeSeries()
    context.adjDiv = sec_list.adjDividends()
    context.adjHedge = sec_list.adjSplits()
    context.divFactors = sec_list.getAdjFactors()
    context.splits = sec_list.getSplits()
    context.avg = ts.mean()
    context.std = ts.std()
    context.leverage = 1
    context.share_num = 0
    context.diff_thresh = 100
    schedule_function(adjust_splits_dividends, date_rules.every_day(), time_rules.market_open())
    schedule_function(place_orders, date_rules.every_day(), time_rules.market_open(hours=1, minutes=30))

def adjust_splits_dividends(context,data):
    splits = context.splits.loc[pd.to_datetime(context.get_datetime()).date()]
    divFactors = context.divFactors.loc[pd.to_datetime(context.get_datetime()).date()]
    context.adjHedge *= splits
    context.adjDiv /= divFactors

def place_orders(context,data): 

    def calc_target_share_ratio(leverage):
        shares = context.adjHedge*context.share_num
        port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
        diff = leverage*context.portfolio.portfolio_value - port_val
        curr_vol = data.current(context.tickers,'volume')
        max_vol = 0.025*curr_vol
        while diff > context.diff_thresh and diff > 0 and (max_vol > shares).all():
            context.share_num += 1*context.leverage
            shares = context.adjHedge*context.share_num
            port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
            diff = leverage*context.portfolio.portfolio_value - port_val
        while diff < 0 or not (max_vol > shares).all():
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

    div_adj_price = data.current(context.tickers,'price').values/context.adjDiv.values
    #rolling_prices = data.history(context.tickers,'price',250,'1d').values/context.adjDiv.values
    #rolling_avg = np.mean(np.dot(rolling_prices,context.adjHedge))
    #rolling_std = np.std(np.dot(rolling_prices,context.adjHedge))
    #rolling_avg_list.append(rolling_avg)
    #rolling_std_list.append(rolling_std)
    current_price = np.dot(div_adj_price,context.adjHedge)
    #zscore = (current_price-rolling_avg)/rolling_std
    zscore = (current_price-context.avg)/context.std
 
    if context.long == True and zscore >= 0:
        orderPortfolio(order_type='exit',leverage = context.leverage)
        context.long = False
    elif context.short == True and zscore <= -0:
        orderPortfolio(order_type='exit',leverage = context.leverage)
        context.short = False
    elif context.short == False and zscore >= 0.5:
        orderPortfolio(order_type='short',leverage = context.leverage)
        context.short= True
    elif context.long == False and zscore <= -0.5:
        orderPortfolio(order_type='long',leverage = context.leverage)
        context.long= True

eastern = pytz.timezone('US/Eastern')        
start= datetime.datetime(2013,1,3,0,0,0,0,eastern)
end = datetime.datetime(2017,8,1,0,0,0,0,eastern)

results= run_algorithm(start=start,end=end,initialize=initialize,capital_base=10000,bundle='quantopian-quandl')

plt.figure()
plt.plot(results.portfolio_value)
plt.title('Portfolio Value')
plt.figure()
plt.plot(results.benchmark_period_return)
plt.plot(results.algorithm_period_return)
plt.title('Benchmark Returns vs. Algo Returns')
plt.legend(['Benchmark Returns','Algo Returns'])
plt.figure()
plt.plot(results.sharpe)
plt.title('Rolling Sharpe')
plt.figure()
plt.subplot(2,2,1)
plt.plot(results.gross_leverage)
plt.title('Gross Leverage')
plt.subplot(2,2,2)
plt.plot(results.net_leverage)
plt.title('Net Leverage')
plt.subplot(2,2,3)
plt.plot(results.max_leverage)
plt.title('Max Leverage')
plt.show()
print(results.sharpe[-1])
