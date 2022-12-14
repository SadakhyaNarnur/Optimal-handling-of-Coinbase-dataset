#CryptoDataDownload
# First import the libraries that we need to use
import pandas as pd

data = pd.read_csv("bulk_loading.csv") 
data['unix']=data.unix.astype('int8')
data['low']=data.low.astype('float32')
data['high']=data.high.astype('float32')
data['open']=data.open.astype('float16')
data['close']=data.close.astype('float16')
data['vol_fiat']=data.vol_fiat.astype('float32')
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
print("date:",data['date'].max())
print("volume:",data['volume'].max())
print("vol_fiat:",data['vol_fiat'].max())
