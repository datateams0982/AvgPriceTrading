import numpy as np 
import pandas as pd 
import os 
import datetime
from datetime import datetime
from tqdm import tqdm 
import data_preprocess_fuction as func

##Read 2330 Data
filepath = "D:\\Strategic_Trading\\trade\\"
data = func.ReadFile_2330(filepath)

##Extract 2330 OHLCV
df = func.OHLCV_2330(data, 1)
df.to_csv("D:\\Strategic_Trading\\VolumeForecast\\data\\2330\\min_OHLCV.csv", index=False)

##Read Index Data
filepath = "D:\\Strategic_Trading\\index\\price\\"
data = func.ReadFile_index(filepath)

data['this_volume'] = data['累積成交數量']-data['累積成交數量'].shift(periods=1,fill_value=0)
data.loc[(data['時間']=='09:00:00'),'this_volume'] = 0

#Extract Index OHLCV 
df = OHLCV_index(data, 1)
df = df.reset_index()
df.columns = ["ts", "open_index", "high_index", "low_index", "close_index", "vol_index"]
df = df.sort_values(by=['ts'])

df["return_index"] = df["close_index"] - df["close_index"].shift(periods=1,fill_value=0)
df["return"][0] = 0

df.to_csv("D:\\Strategic_Trading\\index\\min_agg_all.csv", index=False)
