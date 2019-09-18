import numpy as np 
import pandas as pd 
import os 
import datetime
from datetime import timedelta
from datetime import datetime
import matplotlib.pyplot as plt 
from tqdm import tqdm


##Read Data
def TimeStamp(row):
    time = datetime.strptime(row["Trade Time"][:2] + ":" + row["Trade Time"][2:4] + ":" + row["Trade Time"][4:6], "%H:%M:%S").time()
    date = datetime.strptime(row["Date"][:4] + "-" + row["Date"][4:6] + "-" + row["Date"][6:], "%Y-%m-%d").date()
    timestamp = datetime.combine(date, time)
    
    return timestamp

def ReadFile(filepath):
    file_list = os.listdir(filepath)
    df_list = []
    
    for i, f in tqdm(enumerate(file_list)):
        filename = filepath + f
        data = pd.read_csv(filename, converters = {"Date": str, "Trade Time": str })
    
        data["ts"] = data.apply(TimeStamp, axis = 1)
        data = data[(data["BUY-SELL"] == "B") & (data["ts"].dt.time <= datetime.strptime("13:30:00", "%H:%M:%S").time()) & (data["ts"].dt.time >= datetime.strptime("09:00:00", "%H:%M:%S").time())][["ts", "Trade Price", "Trade Volume(share)"]]
        
        df_list.append(data)
        
    df = pd.concat(df_list)
    
    return df

##Extract OHLCV
def OHLCV(df, frequency):
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

##Compute Technical Indicators    
#Compute Accumulation Distribution
def AD(index, data):
    if (data.at[index, "high"] - data.at[index, "low"]) == 0:
        return 0
    
    else:
        AD = ((data.at[index, "close"] - data.at[index, "low"]) - (data.at[index, "high"] - data.at[index, "close"]))/(data.at[index, "high"] - data.at[index, "low"]) * data.at[index, "vol"]
        return AD

#Compute True Range
def TR(row):
    TR = max([(row["high"] - row["low"]), (row["high"] - row["close_lag"]), (row["close_lag"] - row["low"])])
    
    return TR

#Compute Directional Movement
def posDM(row):
    pos = row["high"] - row["high_lag"]
    neg = row["low"] - row["low_lag"]
    if (pos < 0) and (neg < 0):
        return 0
    elif pos < neg:
        return 0
    else:
        return pos
    
def negDM(row):
    pos = row["high"] - row["high_lag"]
    neg = row["low"] - row["low_lag"]
    if (pos < 0) and (neg < 0):
        return 0
    elif pos > neg:
        return 0
    else:
        return neg
    
def posDI(row):
    if row["TR"] == 0:
        return 0
    
    DI = 100 * row["pos_DM"]/row["TR"]
    
    return DI

def negDI(row):
    if row["TR"] == 0:
        return 0
    
    DI = 100 * row["neg_DM"]/row["TR"]
    
    return DI

#Compute EMA (20min)
def EMA(index, data, period):
    EMA_lag = data.at[index - 1, "EMA"]
    EMA = (data.at[index, "close"] - EMA_lag) * (1/(period + 1)) + EMA_lag
    
    return EMA


#Compute Chande Momentum Oscillator
def CMO(index, data, period):
    df = data.iloc[(index - period):index]
    df_pos = df[df["return"] > 0]
    df_neg = df[df["return"] < 0]
    
    pos = df_pos["return"].sum()
    neg = abs(df_neg["return"].sum())
    
    CMO = 100 * (pos - neg)/(pos + neg)
    
    return CMO

#Compute Commodity Channel Index
def CCI(index, data):
    df = data.iloc[(index - 20):(index+1)]
    D = (abs((df["M"] - df["MA20"]))).mean()
    CCI = (data["M"][i] - data["MA20"][i]) / (0.015 * D)
    
    return CCI

#Compute Bollinger Bands
def BBANDS_std(index, data, period):
    df = data.iloc[(index - period):(index+1)]
    std = (((df["close"] - df["MA20"])**2).mean())**(1/2)
    
    return std

#Compute On Balance Volume
def OBV(index, data):
    if data.at[index, "return"] > 0:
        OBV = data.at[index - 1, "OBV"] + data.at[index, "vol"]
    elif data.at[index, "return"] < 0:
        OBV = data.at[index - 1, "OBV"] - data.at[index, "vol"]
    else:
        OBV = data.at[index - 1, "OBV"]
        
    return OBV

#Compute Williams %R
def WR(index, data, period):
    H = data.iloc[(index-period):(index+ 1)]["high"].max()
    L = data.iloc[(index-period):(index+ 1)]["low"].min()
    C = data.at[index, "close"]
    
    W = 100 * (C - H)/(H - L)
    return W


##Compute Time index
def TimeSin(row):
    x = math.pi * (row["time_index"] - 1) / 265
    time = np.math.sin(x)
    
    return time

##Define Class
def label(row):
    if row["return"] > 0:
        return "up"
    elif row["return"] == 0:
        return "flat"
    else:
        return "down"