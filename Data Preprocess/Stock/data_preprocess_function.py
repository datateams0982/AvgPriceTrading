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

##Read
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


##OHLCV
#Extract OHLCV of 2330
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
    groups = df.groupby(pd.Grouper(freq=frequency))

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
    groups = df.groupby(pd.Grouper(freq=frequency))

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


##Fill missing value
#Create Whole Timestamp list:
def FillMissingTime(data, timelist):
    if len(data) == len(timelist):
        return data
    else:
        date = data["ts"].dt.date.iloc[0]
        timestamp = pd.DataFrame([datetime.combine(date, t) for t in timelist], columns = ["ts"])
        d = pd.merge(timestamp, data, on="ts", how="left")
        d["open"] = d["open"].interpolate(method="pad")
        d["high"] = d["high"].interpolate(method="pad")
        d["low"] = d["low"].interpolate(method="pad")
        d["close"] = d["close"].interpolate(method="pad")

        d["vol"] = d["vol"].fillna(0)
        d["VWAP"] = d["VWAP"].fillna(0)

        return d


##Daily Aggregation
#Daily Aggreration of 2330
def DailyOHLCV_2330(df):
    
    ts = df.iloc[0]["ts"].date()
    open = df.iloc[0]['open']
    high = df['high'].max()
    low = df['low'].min()
    close = df.iloc[-1]['close']
    vol = df['vol'].sum()
    vwap = (df["VWAP"] * df["vol"]).sum() / df["vol"].sum()
    
    return [ts, open, high, low, close, vol, vwap]


#Daily Aggreration of index
def DailyOHLCV_Index(df):
    
    ts = df.iloc[0]["ts"].date()
    open = df.iloc[0]['open_index']
    high = df['high_index'].max()
    low = df['low_index'].min()
    close = df.iloc[-1]['close_index']
    vol = df['vol_index'].sum()
    
    return [ts, open, high, low, close, vol]



