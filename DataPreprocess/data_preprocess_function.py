import numpy as np 
import pandas as pd 
import os 
import datetime
from datetime import datetime
from tqdm import tqdm



##Create Time Stamp
def TimeStamp(row):
    time = datetime.strptime(row["Trade Time"][:2] + ":" + row["Trade Time"][2:4] + ":" + row["Trade Time"][4:6], "%H:%M:%S").time()
    date = datetime.strptime(row["Date"][:4] + "-" + row["Date"][4:6] + "-" + row["Date"][6:], "%Y-%m-%d").date()
    timestamp = datetime.combine(date, time)
    
    return timestamp

#Read and Join Index Data
def read_and_join(file):
    df_index = pd.read_csv('D:\\Strategic_Trading\\index\\price\\'+ file)[['日期','時間','發行量加權股價指數']]
    date = file[-14:-4]
    df_settle = pd.read_csv('D:\\Strategic_Trading\\index\\settle\\MI_5MINS_{}.csv'.format(date))[['日期','時間','累積成交筆數','累積成交數量','累積成交金額']]
    df_r = pd.merge(df_index, df_settle, how="left", on=['日期','時間'])
    df_r.index=pd.to_datetime(df_r['日期'] + ' ' + df_r['時間'])
    return df_r


#Read 2330
def ReadFile_2330(f, filepath):
    filename = filepath + f
    data = pd.read_csv(filename, converters = {"Date": str, "Trade Time": str })

    data["ts"] = data.apply(TimeStamp, axis = 1)
    data = data[(data["BUY-SELL"] == "B") & (data["ts"].dt.time <= datetime.strptime("13:30:00", "%H:%M:%S").time()) & (data["ts"].dt.time >= datetime.strptime("09:00:00", "%H:%M:%S").time())][["ts", "Trade Price", "Trade Volume(share)"]]
    
    
    return data


##Extract OHLCV of 2330
def OHLCV_2330(df, frequency):
    keys = [
        'ts',
        'open',
        'high',
        'low',
        'close', 
        'vol',
        'VWAP'
    ]

    tracker = {}
    for k in keys: tracker[k] = []
    
    df.index = pd.to_datetime(df['ts'])
    groups = df.groupby(pd.Grouper(freq=(str(frequency) + "min")))

    for group in groups:
        g1 = group[1]
        if len(g1) == 0: continue

        ts = group[0]
        tracker['ts'].append(ts)

        #extract OHLCV from grouped 15min ticks
        open = g1.iloc[0]['Trade Price']
        high = g1['Trade Price'].max()
        low = g1['Trade Price'].min()
        close = g1.iloc[-1]['Trade Price']
        vol = g1['Trade Volume(share)'].sum()
        VWAP = (g1["Trade Price"] * g1["Trade Volume(share)"]).sum() / g1["Trade Volume(share)"].sum()

        tracker['open'].append(open)
        tracker['high'].append(high)
        tracker['low'].append(low)
        tracker['close'].append(close)
        tracker['vol'].append(vol)
        tracker["VWAP"].append(VWAP)
    
    df = pd.DataFrame(data=tracker, columns=keys[0:])
    return df

#Extract OHLCV of index
def OHLCV_index(df, frequency):
    keys = [
        'ts',
        'open',
        'high',
        'low',
        'close', 
        'vol'
    ]

    tracker = {}
    for k in keys: tracker[k] = []
    
    df.index = pd.to_datetime(df['日期'] + ' ' + df['時間'])
    groups = df.groupby(pd.Grouper(freq=str(frequency)+"min"))

    for group in groups:
        g1 = group[1]
        if len(g1) == 0: continue

        ts = group[0]
        tracker['ts'].append(ts)

        #extract OHLCV from grouped 15min ticks
        open = g1.iloc[0]['發行量加權股價指數']
        high = g1['發行量加權股價指數'].max()
        low = g1['發行量加權股價指數'].min()
        close = g1.iloc[-1]['發行量加權股價指數']
        vol = g1['this_volume'].sum()

        tracker['open'].append(open)
        tracker['high'].append(high)
        tracker['low'].append(low)
        tracker['close'].append(close)
        tracker['vol'].append(vol)
    
    df = pd.DataFrame(data=tracker, columns=keys[1:], index=tracker['ts'])
    return df

