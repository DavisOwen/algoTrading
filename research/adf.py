#!/usr/bin/env python

'''
Augmented Dickey Fuller Test for two stocks
Resulting residual graph plotted with matplotlib

Usage:

    ./adf.py ticker1 ticker2

'''

import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import pandas_datareader.data as web
import pprint
import statsmodels.tsa.stattools as ts
import statsmodels.formula.api as sm
import sys


def test_stocks():

    ticker1 = sys.argv[1]
    ticker2 = sys.argv[2]

    start = datetime.datetime(1994, 9, 29)
    end = datetime.datetime(2017, 4, 5)

    gld = web.DataReader(ticker1, "yahoo", start, end)
    gdx = web.DataReader(ticker2, "yahoo", start, end)

    df = pd.DataFrame(index=gld.index)
    df[ticker1] = gld["Adj Close"]
    df[ticker2] = gdx["Adj Close"]

    # Plot the two time series
    plot_price_series(df, ticker1, ticker2, start, end)

    # Display a scatter plot of the two time series
    plot_scatter_series(df, ticker1, ticker2)

    # Calculate optimal hedge ratio "beta"
    res = sm.ols(formula=ticker1 + " ~ " + ticker2, data=df).fit()
    beta = res.params[1]

    # Calculate the residuals of the linear combination
    df["res"] = df[ticker1] - beta * df[ticker2]

    # Plot the residuals
    plot_residuals(df, start, end)

    # Calculate and output the CADF test on the residuals
    cadf = ts.adfuller(df["res"])
    pprint.pprint(cadf)
    print(beta_hr)


def plot_price_series(df, ts1, ts2, start, end):
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df.index, df[ts1], label=ts1)
    ax.plot(df.index, df[ts2], label=ts2)
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(start, end)
    ax.grid(True)
    fig.autofmt_xdate()

    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('%s and %s Daily Prices' % (ts1, ts2))
    plt.legend()
    plt.show()


def plot_scatter_series(df, ts1, ts2):
    plt.xlabel('%s Price ($)' % ts1)
    plt.ylabel('%s Price ($)' % ts2)
    plt.title('%s and %s Price Scatterplot' % (ts1, ts2))
    plt.scatter(df[ts1], df[ts2])
    plt.show()


def plot_residuals(df, start, end):
    months = mdates.MonthLocator()  # every month
    fig, ax = plt.subplots()
    ax.plot(df.index, df["res"], label="Residuals")
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.set_xlim(start, end)
    ax.grid(True)
    fig.autofmt_xdate()

    plt.xlabel('Month/Year')
    plt.ylabel('Price ($)')
    plt.title('Residual Plot')
    plt.legend()

    plt.plot(df["res"])
    plt.show()


if __name__ == "__main__":
    test_stocks()
