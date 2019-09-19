import numpy as np 
import pandas as pd 
import datetime
from datetime import datetime
from datetime import datetime, timedelta
from sklearn.preprocessing import OneHotEncoder
from tqdm import tqdm_notebook as tqdm
import calendar
import math


##Compute Time Feature
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

#Transform Month to Categorical Feature
def OneHotMonth(data):
    onehot_encoder = OneHotEncoder(sparse=False)
    m = np.array(data['Month']).reshape(len(data['Month']), 1)
    month = onehot_encoder.fit_transform(m)

    data = pd.concat([data, pd.DataFrame(month, columns=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])], axis=1)

    return data

#Transform Weekday to Categorical Feature
def OneHotWeekday(data):
    onehot_encoder = OneHotEncoder(sparse=False)
    w = np.array(data['Weekday']).reshape(len(data['Weekday']), 1)
    week = onehot_encoder.fit_transform(w)

    data = pd.concat([data, pd.DataFrame(week, columns=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'])], axis=1)

    return data


##Compute Historical Feature
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

#Compute Exponential Average True Range
def EATR(index, data, period):
    EATR_lag = data.at[index - 1, "EATR"]
    alpha = 2/(period+1)
    EATR = data.at[index, "TR"] * alpha  + EATR_lag * (1 - alpha)
    
    return EATR

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
    alpha = 2/(period+1)
    EMA = data.at[index, "close"] * alpha  + EMA_lag * (1 - alpha)
    
    return EMA


#Compute Chande Momentum Oscillator
def CMO(index, data, period):
    df = data.iloc[(index - period + 1):(index+1)]
    df_pos = df[df["return"] > 0]
    df_neg = df[df["return"] < 0]
    
    pos = df_pos["return"].sum()
    neg = abs(df_neg["return"].sum())
    
    if (pos+neg) == 0:
        CMO = 0
    else:
        CMO = 100 * (pos - neg)/(pos + neg)
    
    return CMO

#Compute Commodity Channel Index
def CCI(index, data):
    df = data.iloc[(index - 19):(index+1)]
    D = (abs((df["M"] - df["MA20"]))).mean()
    CCI = (data["M"].iloc[index] - data["MA20"].iloc[index]) / (0.015 * D)
    
    return CCI


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
    H = data.iloc[(index-period + 1):(index+ 1)]["high"].max()
    L = data.iloc[(index-period + 1):(index+ 1)]["low"].min()
    C = data.at[index, "close"]
    
    if (H - L) == 0:
        W = 0
    else:
        W = 100 * (C - H)/(H - L)

    return W


##Compute Weekly Return
#Preparation
def YearWeek(row):
    date = row["ts"].date()
    week = date.isocalendar()[1]
    
    return week

#Computaion
def WeeklyReturn(row, week_dict):
    week = row['YearWeek']
    year = row['ts'].year
    month = row['ts'].month
    week_day = week_dict[year][week]['End']
    start_day = week_dict[year][week]['Start']
    
    if (row['Weekday'] != week_day):
        if row['Weekday'] == start_day:
            return -9999
        else:
            return 9999
    
    else:
        if week == min(week_dict[year]):
            if month == 1:
                year = year - 1
            
            week_last = max(week_dict[year])
                
        else:
            week_last = max({key:value for (key,value) in week_dict[year].items() if key < week})
        
    close_lag = week_dict[year][week_last]['close']
    weekly_return = row['close'] - close_lag
    
    return weekly_return


##Define Class
#Daily
def Dailylabel(row):
    if row["return"] > 0:
        return "up"
    elif row["return"] == 0:
        return "flat"
    else:
        return "down"

#Weekly Abs
def Weeklylabel(row):
    if abs(row['WeeklyReturn']) == 9999:
        return np.nan
    elif row['WeeklyReturn'] > 0:
        return 'up'
    elif row['WeeklyReturn'] < 0:
        return 'down'
    else:
        return 'flat'

#Weekly ATR
def Weeklylabel_ATR(row):
    if abs(row['WeeklyReturn']) == 9999:
        return np.nan
    elif row['WeeklyReturn'] > row['EATR']:
        return 'up'
    elif row['WeeklyReturn'] < (-1) * row['EATR']:
        return 'down'
    else:
        return 'flat'

