#CryptoDataDownload
# First import the libraries that we need to use
import pandas as pd
import requests
import json

data = pd.read_csv('/Users/sadakhyanarnur/Desktop/grp_pro_225/coinbase/load.csv')  
print("BEFORE DATATYPE CHANGES:")
print(data.shape)
print("-"*50)
print(data.memory_usage(index=False, deep=True))
print("-"*50)
print(data.memory_usage(index=False, deep=True).sum())
print("-"*50)
print(data.dtypes)
print("-"*50)
print("unix:",data['unix'].max())
print("low:",data['low'].max())
print("high:",data['high'].max())
print("volume:",data['volume'].max())
print("vol_fiat:",data['vol_fiat'].max())
print("symbol:",data['symbol'].max())
print("year:",data['low'].max())
print("month:",data['high'].max())
data['unix']=data.unix.astype('int8')
data['low']=data.low.astype('float16')
data['high']=data.high.astype('float16')
data['open']=data.open.astype('float16')
data['close']=data.close.astype('float16')
data['volume']=data.close.astype('float16')
data['vol_fiat']=data.vol_fiat.astype('float32')
data['year']=data.year.astype('int8')
data['month']=data.month.astype('int8')
print("AFTER DATATYPE CHANGES:")
print(data.shape)
print("-"*50)
print(data.memory_usage(index=False, deep=True))
print("-"*50)
print(data.memory_usage(index=False, deep=True).sum())
print("-"*50)
print(data.dtypes)
print("-"*50)
print("unix:",data['unix'].max())
print("low:",data['low'].max())
print("high:",data['high'].max())
print("volume:",data['volume'].max())
print("vol_fiat:",data['vol_fiat'].max())
print("symbol:",data['symbol'].max())
print("year:",data['low'].max())
print("month:",data['high'].max())
                  