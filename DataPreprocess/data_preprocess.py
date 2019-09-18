import numpy as np 
import pandas as pd 
import os 
import datetime
from datetime import datetime
from tqdm import tqdm 
from multiprocessing import Pool, TimeoutError, Lock, Value, current_process, cpu_count
from functools import partial
import data_preprocess_function as func


##Read 2330 Data
filepath = "D:\\Strategic_Trading\\2330\\trade\\"
file_list = os.listdir(filepath)
df_list = []

if __name__ == '__main__':
    with Pool(processes=12) as pool:
        for i, x in enumerate(tqdm(pool.imap_unordered(partial(func.ReadFile_2330, filepath=filepath), file_list), total=len(file_list)), 1):
            df_list.append(x)
            
data = pd.concat(df_list)

##Extract 2330 OHLCV
df_list = [group[1] for group in data.groupby(data["ts"].dt.date)]
OHLCV_list = []

if __name__ == '__main__':
    with Pool(processes=12) as pool:
        for i, x in enumerate(tqdm(pool.imap_unordered(partial(func.OHLCV_2330, frequency=1), df_list), total=len(df_list)), 1):
                OHLCV_list.append(x)

df = pd.concat(OHLCV_list)
df = df.sort_values(by=['ts'])

df.to_csv("D:\\Strategic_Trading\\PriceForecast\\data\\2330\\min_OHLCV.csv", index=False)

##Read Index Data
filepath = "D:\\Strategic_Trading\\index\\price\\"
file_list = os.listdir(filepath)
df_list = []

if __name__ == '__main__':
    with Pool(processes=12) as pool:
        for i, x in enumerate(tqdm(pool.imap_unordered(func.read_and_join, file_list), total=len(file_list)), 1):
            df_list.append(x)
            
data = pd.concat(df_list)

data['this_volume'] = data['累積成交數量']-data['累積成交數量'].shift(periods=1,fill_value=0)
data.loc[(data['時間']=='09:00:00'),'this_volume'] = 0

#Extract Index OHLCV 
df_list = [group[1] for group in data.groupby('日期')]
OHLCV_list = []

if __name__ == '__main__':
    with Pool(processes=12) as pool:
        for i, x in enumerate(tqdm(pool.imap_unordered(partial(func.OHLCV_index, frequency=1), df_list), total=len(df_list)), 1):
                OHLCV_list.append(x)

df = pd.concat(OHLCV_list)
df = df.reset_index()
df.columns = ["ts", "open_index", "high_index", "low_index", "close_index", "vol_index"]
df = df.sort_values(by=['ts'])

df["return_index"] = df["close_index"] - df["close_index"].shift(periods=1)

df.to_csv("D:\\Strategic_Trading\\PriceForecast\\data\\index\\min_OHLCV.csv", index=False)
