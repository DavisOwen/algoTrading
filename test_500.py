from securityList import *
sl=SecurityList()
start = datetime.datetime(1994,9,29)
end = datetime.datetime(2017,4,5)
sl.downloadQuandl(start,end)
sl_arr=sl.data.values # this will be a 2d numpy array (time x stocks)

# need to remove columns corresponding to stocks that refused to download
#This will avoid matrix inversion errors
numNan=np.zeros(sl_arr.shape[1])
for col in range(sl_arr.shape[1]):
    numNan[col]=np.count_nonzero(np.isnan(sl_arr[:,col].astype('float')))

sl_arr_noNan=sl_arr[:,np.where(numNan==0)].squeeze()

sl.data=sl_arr_noNan
sl.genTimeSeries()

