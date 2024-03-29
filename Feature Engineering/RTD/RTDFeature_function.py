import numpy as np 
import pandas as pd 
import datetime
from datetime import datetime
from datetime import datetime, timedelta
from tqdm import tqdm_notebook as tqdm
import calendar
import math

#Transform Intra-Daily Time Period by Sin Function
def IntraDailyTime(row, timedict):
    time = row["ts"].time()
    index = timedict[time]
    x = math.pi * index / (len(timedict) - 1)
    time = np.math.sin(x)
    
    return time


#Transform Intra-monthly Period by linear function
def IntraMonth(row, month_dict):
    y, m, date = row["ts"].year, row["ts"].month, row["ts"].date()
    d = month_dict[y][m]
    total = len(d)
    day = month_dict[y][m][date]
    time = day/(total - 1)
    
    return time

#Transform Intra-monthly feature to categorical (5 categories)
def MonthPeriod(row):
    num = row["Intramonth"]
    if num < 0.2:
        return 1
    elif num < 0.4:
        return 2
    elif num < 0.6:
        return 3
    elif num < 0.8:
        return 4
    else:
        return 5

#Transform week information by linear function
def WeekTime(row):
    y, m, d, date = row["ts"].year, row["ts"].month, row["ts"].day, row["ts"].date()
    week = date.isocalendar()[1] - date.replace(day=1).isocalendar()[1] + 1

    if week < 0:
        if m == 1:
            week = date.isocalendar()[1] + 1
        else:
            week = date.replace(day = d-7).isocalendar()[1] - date.replace(day=1).isocalendar()[1] + 2
        
    total = len(calendar.monthcalendar(y, m))
    week_time = (week - 1)/(total - 1)

    return week_time

#Compute Cumulative Volume
def Cumulative(data):
    data["cumvol"] = data["vol"].cumsum()
    
    return data

#Compute Daily OHLCV
def DailyOHLCV(df):
    
    df['DailyOpen'] = df.iloc[0]['open']
    df['DailyHigh'] = df['high'].max()
    df['DailyLow'] = df['low'].min()
    df['DailyClose'] = df.iloc[-1]['close']
    df['DailyVol'] = df['vol'].sum()
    
    return df


