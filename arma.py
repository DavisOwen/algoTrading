#!algotrading/bin/python3
'''
Tests ARMA model using train-splits

params:
    start => start date
    end => end data
    ticks => tickers

'''

import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.arima_model import ARMA
from securityList import SecurityList
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

start = datetime.datetime(2013,1,3)
end = datetime.datetime(2017,8,1)
ticks = ['AYI','APA','AMZN','LNT','CTL','ALB','ABBV','AMT','ADM','AON','ORCL']

sec_list = SecurityList(ticks)
sec_list.downloadQuandl(start,end)
ts = sec_list.genTimeSeries()
X_train,X_test,y_train,y_test = train_test_split(np.arange(1,len(ts)+1),ts,shuffle = False)
model = ARMA(y_train, order = (5,0))
model_fit = model.fit(disp=0)
residuals = pd.DataFrame(model_fit.resid)
residuals.plot()
plt.show()
residuals.plot(kind='kde')
plt.show()
history = [x for x in y_train]
predictions = list()
for t in range(len(y_test)):
	model = ARMA(history, order = (5,0))
	model_fit = model.fit(disp=0)
	output = model_fit.forecast()
	yhat = output[0]
	predictions.append(yhat)
	obs = y_test[t]
	history.append(obs)
	print('predicted=%f, observed = %f' % (yhat, obs))
error = mean_squared_error(y_test, predictions)
print('Test MSE: %.3f' % error)
plt.plot(y_test)
plt.plot(predictions, color = 'red')
plt.show()
