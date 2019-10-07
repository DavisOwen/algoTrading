#!/usr/bin/env python

'''
Runs a polynomial regression on a time series and tests
its ability to predict prices

'''

import datetime
import numpy as np
import matplotlib.pyplot as plt
from securityList import SecurityList
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

np.set_printoptions(threshold=np.nan)

start = datetime.datetime(2013, 1, 3)
end = datetime.datetime(2017, 8, 1)
ticks = ['AYI', 'APA', 'AMZN', 'LNT', 'CTL',
         'ALB', 'ABBV', 'AMT', 'ADM', 'AON', 'ORCL']

sec_list = SecurityList(ticks)
sec_list.downloadQuandl(start, end)
ts = sec_list.runJohansen()
plt.figure()
plt.plot(np.linspace(0, 1, len(ts)), ts)
plt.show()
X_train, X_test, y_train, y_test = train_test_split(
    np.linspace(0, 1, len(ts)), ts, random_state=0, shuffle=False)
poly = PolynomialFeatures(6)
X_poly_train = poly.fit_transform(X_train.reshape(len(X_train), 1))
X_poly_test = poly.fit_transform(X_test.reshape(len(X_test), 1))
linreg = LinearRegression().fit(X_poly_train, y_train)
train = linreg.predict(X_poly_train)
test = linreg.predict(X_poly_test)
plt.subplot(2, 1, 1)
plt.plot(X_train, train)
plt.subplot(2, 1, 2)
plt.plot(X_test, test)
plt.show()
plt.figure()
plt.plot(np.linspace(0, 1, len(ts)), ts)
plt.plot(X_train, train)
plt.plot(X_test, test)
plt.show()
