#!algotrading/bin/python3

'''
Wrapper for bpj that feeds results
to paramOpt.py to find out which zEnter
and zExit zscore values lead to optimal 
results from backtesting
'''

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
from zipline.api import order_target_percent, symbol, schedule_function, date_rules, set_slippage, order_target
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
    context.adjustedHedge = np.copy(context.hedgeRatio)
    context.adjustedHedge[3] = context.adjustedHedge[3]/2
    context.avg = ts.mean()
    context.std = ts.std()
    context.volume = sec_list.getVolume()
    context.share_num = 0
    context.diff_thresh = 100
    schedule_function(place_orders, date_rules.every_day())
    
def place_orders(context,data): 

    def calc_target_share_ratio():
        shares = context.adjustedHedge*context.share_num
        port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
        diff = context.portfolio.portfolio_value - port_val
        while diff > context.diff_thresh and diff > 0:
            context.share_num += 1
            shares = context.adjustedHedge*context.share_num
            port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
            diff = context.portfolio.portfolio_value - port_val
        while diff < 0:
            context.share_num -= 1
            shares = context.adjustedHedge * context.share_num
            port_val = np.dot(np.absolute(shares),data.current(context.tickers,'price'))
            diff = context.portfolio.portfolio_value - port_val
        return shares

    def orderPortfolio(order_type):
        shares = calc_target_share_ratio()
        for i,tick in enumerate(context.tickers):
            if order_type == 'long':
                order_target(tick,shares[i])
            if order_type == 'short':
                order_target(tick,-shares[i])
            if order_type == 'exit':
                order_target(tick,0)

    current_price = np.dot(context.df_close.loc[pd.to_datetime(context.get_datetime()).date()],context.hedgeRatio)
    zscore = (current_price-context.avg)/context.std
 
    if context.long == True and zscore >= zExit:
        orderPortfolio(order_type='exit')
        context.long = False
    elif context.short == True and zscore <= -zExit:
        orderPortfolio(order_type='exit')
        context.short = False
    elif context.short == False and zscore >= zEnter:
        orderPortfolio(order_type='short')
        context.short= True
    elif context.long == False and zscore <= -zEnter:
        orderPortfolio(order_type='long')
        context.long= True

def main(Ent,Ex):

    global zEnter
    global zExit
    zEnter = Ent
    zExit = Ex
    eastern = pytz.timezone('US/Eastern')        
    start= datetime.datetime(2013,1,3,0,0,0,0,eastern)
    end = datetime.datetime(2017,8,1,0,0,0,0,eastern)

    results= run_algorithm(start=start,end=end,initialize=initialize,capital_base=1000000,bundle='quantopian-quandl')
    return results.portfolio_value[-1]
