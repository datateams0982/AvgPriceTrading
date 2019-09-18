import numpy as np 
import pandas as pd 
import os 
import datetime
from datetime import timedelta
from datetime import datetime
import matplotlib.pyplot as plt 
from tqdm import tqdm
import data_preprocess_fuction as func

##Data Format: "ts": datetime, OHLCV
def Feature_Engineering(df, term): ##term: How many periods for MA/EMA calculation

    data = df.copy()
    
    ##Prepare for future usage
    data["close_lag"] = data["close"].shift(1)
    data["high_lag"] = data["high"].shift(1)
    data["low_lag"] = data["low"].shift(1)
    data["return"] = data["close"] - data["close_lag"]

    #Compute Moving Average
    print("Calculating Moving Average")
    data["MA20"] = data.close.rolling(term).mean()

    #Compute Accumulation Distribution
    print("Calculating Accumulation Distribution")
    data["AD"] = np.zeros(len(data))
    for i in tqdm(range(len(data))):
        if i == 0:
            data["AD"][i] = func.AD(i, data)
        
        else:
            data["AD"][i] = data.at[i - 1, "AD"] + func.AD(i, data)


    #Compute True Range for ATR
    data["TR"] = data.apply(func.TR, axis = 1)


    #Compute Directional Movement Index
    print("Calculating Directional Movement Index")
    data["pos_DM"], data["neg_DM"] = data.apply(func.posDM, axis = 1), data.apply(func.negDM, axis = 1)
    data["pos_DI"], data["neg_DI"] = data.apply(func.posDI, axis = 1), data.apply(func.negDI, axis = 1)

    #Compute EMA
    print("Calculating Exponential Moving Average")
    data["EMA"] = np.nan
    for i in tqdm(range(len(data))):
        if i < 20:
            continue
        elif i == 20 :
            data["EMA"][i] = data["MA20"][i]
        else:
            data["EMA"][i] = func.EMA(i, data, term)

    
    #Compute Average True Range
    print("Calculating Average True Range")
    data["ATR"] = data.TR.rolling(term).mean()

    
    #Compute Chande Momentum Oscillator
    print("Calculating Chande Momentum Oscillator")
    data["CMO"] = np.nan
    for i in tqdm(range(len(data))):
        if i < 20:
            continue
        else:
            data["CMO"][i] = func.CMO(i, data, term)

    
    #Prepare for CCI Calaulation
    data["M"] = (data["high"] + data["low"] + data["close"]) /3

    #Compute CCI
    print("Calculating CCI")
    data["CCI"] = np.nan
    for i in tqdm(range(len(data))):
        if i < 20:
            continue
        else:
            data["CCI"][i] = func.CCI(i, data)

    
    #Compute BBANDS
    print("Calculating BBANDs")
    data["uBBAND"], data["lBBAND"] = data["MA20"] + 2*data.close.rolling(term).std(), data["MA20"] - 2*data.close.rolling(term).std()

    
    #Compute On Balance Volume
    print("Calculating On Balance Volume")
    data["OBV"] = np.zeros(len(data))
    for i in tqdm(range(len(data))):
        if i < 20:
            continue
        else:
            data["OBV"][i] = func.OBV(i, data)

    
    #Compute Rate of Change
    print("Calculating Rate of Change")
    data["ROC_20"] = (data["close"] /data["close"].shift(term) - 1) * 100

    
    #Compute Williams R%
    print("Calculating WR")
    data["WR"] = np.nan
    for i in tqdm(range(len(data))):
        if i < 20:
            continue
        else:
            data["WR"][i] = WR(i, data, term)

    #Compute Time Feature
    print("Transforming Time Feature")
    data["date"] = data["ts"].dt.date
    data["time"] = data["ts"].dt.time
    x = data["ts"].dt.time.unique()
    y = [i for i in range(1, len(x)+1)]
    df = pd.DataFrame(np.stack((x, y), axis=1), columns=["time", "time_index"])

    data = pd.merge(data, df, on = "time")
    data = data.sort_values(by=['ts']).reset_index(drop=True)

    data["sin_time"] = data.apply(func.TimeSin, axis=1)

    #Define label
    print("Defining y")
    data["y"] = data.apply(func.label, axis=1)


    data = data[['open', 'high', 'low', 'close', 'vol', 'VWAP', 'MA20', 'AD', 'pos_DM', 'neg_DM', 'pos_DI', 'neg_DI', 'EMA', 'ATR', 'CMO', 'CCI', 'uBBAND', 'lBBAND', 'OBV', 'ROC_20', 'WR', 'time_index', 'sin_time', 'return', 'y']]


    return data ##Return whole dataframe including features above