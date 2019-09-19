import numpy as np 
import pandas as pd 
import os 
import datetime
from datetime import datetime
from tqdm import tqdm_notebook as tqdm

#Read TXT file and Transform to csv
def ReadRTD(filepath):
    with open(filepath, "r") as f:
        lines = f.readlines()
        
    keys = ['ts', 'open', 'high', 'low', 'close', 'vol']
    tracker = {}
    
    for k in keys:
        tracker[k] = []
        
    for i, line in tqdm(enumerate(lines), total = len(lines)):
        if i == 0:
            continue
        else:
            data = line.split(",")
            date = datetime.strptime(data[0], '%Y/%m/%d').date()
            time = datetime.strptime(data[1], '%H:%M:%S').time()
            timestamp = datetime.combine(date, time)
            tracker['ts'].append(timestamp)
            tracker['open'].append(data[2])
            tracker['high'].append(data[3])
            tracker['low'].append(data[4])
            tracker['close'].append(data[5])
            tracker['vol'].append(data[6])
                
    df = pd.DataFrame(data=tracker, columns=keys[0:])
    
    return df 


#Create Whole Timestamp list:
def FillMissingTime(data, timelist):
    if len(data) == len(timelist):
        return data
    else:
        date = data["ts"].dt.date.iloc[0]
        timestamp = pd.DataFrame([datetime.combine(date, t) for t in timelist], columns = ["ts"])
        d = pd.merge(timestamp, data, on="ts", how="left")
        d = d.sort_values(by="ts")
        d["open"] = d["open"].interpolate(method="pad")
        d["high"] = d["high"].interpolate(method="pad")
        d["low"] = d["low"].interpolate(method="pad")
        d["close"] = d["close"].interpolate(method="pad")

        d["vol"] = d["vol"].fillna(0)

        return d
