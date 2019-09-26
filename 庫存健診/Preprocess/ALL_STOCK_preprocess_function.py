import numpy as np 
import pandas as pd 
import os
import datetime
from datetime import datetime
from datetime import datetime, timedelta
from sklearn.preprocessing import OneHotEncoder
from tqdm import tqdm_notebook as tqdm
import calendar
import math
import pymssql as mssql
import time

#Send query to NsSQL
def send_query_to_MSSQL(query, db = 'ODS', timecost = True, showcol = True, showlen = True):
    """
    db: 選擇資料庫
    timecost = True: 統計此次查詢所花時間
    showcol = True: 顯示 table 欄位
    """

    tStart = time.time()
    if db == 'ODS':
        ods = mssql.connect(host = '128.110.13.89', user = '011553', password = 'Sino821031pac')
    if db == 'ODS_BK':
        ods = mssql.connect(host = '128.110.13.89', user = '011553', password = 'Sino821031pac')
    if db == 'CRM':
        ods = mssql.connect(host = '128.110.13.68', user = '011553', password = 'Sino821031pac')

    odscur = ods.cursor(as_dict = True)
    odscur.execute(query)
    temp = odscur.fetchall()
    df = pd.DataFrame(temp)
    odscur.close()
    tEnd = time.time()

    if timecost == True:
        print("It cost %f sec" % (tEnd - tStart))
    if showlen == True:
        print('Data length:', len(df))
    if showcol == True:
        print(df.columns)

    return df


#From MsSQL Extract Trading Data
def get_stock():

    query = f"""
        SELECT STOCK_NO 
                ,TR_TOL 
                ,TR_AMT
                ,B_PRICE 
                ,H_PRICE 
                ,L_PRICE 
                ,C_PRICE 
                ,DATE_YMD 
        FROM ODS.dbo.ST_STK_PRICE_D
            """


    stock = send_query_to_MSSQL(query, db='ODS')
    stock.columns = ['StockNo', 'total', 'vol', 'open', 'high', 'low', 'close', 'ts']
    stock['ts'] = stock['ts'].apply(lambda x: datetime.strptime((str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:]), '%Y-%m-%d').date())


    return stock

def data_preprocess(filename):
    d = pd.read_csv(filename, converters={'StockNo': str, 'StockName': str})
    date = filename[-14:-4]
    d['ts'] = datetime.strptime(date, '%Y-%m-%d').date()
    d[['open', 'high', 'low', 'close']] = d[['open', 'high', 'low', 'close']].replace('--', np.nan)
    d['vol'], d['total'], d['open'], d['close'], d['high'], d['low'] = d['vol'].apply(lambda x: str(x).replace(',', '')), d['total'].apply(lambda x: str(x).replace(',', '')), d['open'].apply(lambda x: str(x).replace(',', '')), d['close'].apply(lambda x: str(x).replace(',', '')), d['high'].apply(lambda x: str(x).replace(',', '')), d['low'].apply(lambda x: str(x).replace(',', ''))
    d[['vol', 'total', 'open', 'high', 'low', 'close']] = d[['vol', 'total', 'open', 'high', 'low', 'close']].astype(np.float)
    
    return d


def Get_period_data(file_path, start, end):
    filelist = os.listdir(file_path)
    date_list = [f for f in filelist if (datetime.strptime(f[-14:-4], '%Y-%m-%d').date() >= start) and (datetime.strptime(f[-14:-4], '%Y-%m-%d').date() < end)]
    df_list = []

    for i, f in enumerate(tqdm(date_list, total=len(date_list))):
        filename = file_path + f
        d = pd.read_csv(filename, converters={'StockNo': str, 'StockName': str})
        date = filename[-14:-4]
        d['ts'] = datetime.strptime(date, '%Y-%m-%d').date()
        d[['open', 'high', 'low', 'close']] = d[['open', 'high', 'low', 'close']].replace('--', np.nan)
        d['vol'], d['total'], d['open'], d['close'], d['high'], d['low'] = d['vol'].apply(lambda x: str(x).replace(',', '')), d['total'].apply(lambda x: str(x).replace(',', '')), d['open'].apply(lambda x: str(x).replace(',', '')), d['close'].apply(lambda x: str(x).replace(',', '')), d['high'].apply(lambda x: str(x).replace(',', '')), d['low'].apply(lambda x: str(x).replace(',', ''))
        d[['vol', 'total', 'open', 'high', 'low', 'close']] = d[['vol', 'total', 'open', 'high', 'low', 'close']].astype(np.float)
        df_list.append(d)

    df = pd.concat(df_list, axis=0)
    df = df.sort_values(by='ts')

    return df


def GetStockList(data):
    stock_list = data['StockNo'].unique().tolist()
    stock = [s for s in stock_list if (len(s) == 4) and (s[0] in [str(i) for i in range(1,10)])]
        
    return stock



def GetStockData(filename, stocknumber):
    data = pd.read_csv(filename, converters={'StockNo': str, 'StockName': str})
    status = True
    df = data[data['StockNo'] == str(stocknumber)]
    
    if len(df) == 0:
        status = False
        return [status]
    elif df.iloc[0]['open'] == '--':
        status = False
        return [status]
    else:
        date = filename[-14:-4]
        df['ts'] = datetime.strptime(date, '%Y-%m-%d').date()
        
        return [status, df.iloc[0]]


def FillMissingTime(data, timedf):
    data['ts'] = pd.to_datetime(data['ts'])
    data = data.sort_values(by='ts')

    status = True

    if len(data) == len(timedf):
        data["open"] = data["open"].interpolate(method="pad")
        data["high"] = data["high"].interpolate(method="pad")
        data["low"] = data["low"].interpolate(method="pad")
        data["close"] = data["close"].interpolate(method="pad")

        data["vol"] = data["vol"].fillna(0)
        data["total"] = data["total"].fillna(0)
        data['return'] = data['close'] - data['close'].shift(1)
        data['return'] = data['close'] - data['close'].shift(1)
        
        return [status, data]

    else:
        Start_date = data['ts'].iloc[0]
        End_date = data['ts'].iloc[-1]
        
        if End_date < datetime.strptime('2019-09-23', '%Y-%m-%d').date():
            status = False
            return [status]
        

        time_df = timedf[timedf.ts.dt.date >= Start_date]
        time_df = time_df[time_df.ts.dt.date <= End_date]
        d = pd.merge(time_df, data, on="ts", how="left")

        d = d.sort_values(by="ts")

        d['StockNo'] = data['StockNo'].unique()[0]
        d['StockName'] = data['StockName'].unique()[0]

        d["open"] = d["open"].interpolate(method="pad")
        d["high"] = d["high"].interpolate(method="pad")
        d["low"] = d["low"].interpolate(method="pad")
        d["close"] = d["close"].interpolate(method="pad")

        d["vol"] = d["vol"].fillna(0)
        d["total"] = d["total"].fillna(0)
        d['return'] = d['close'] - d['close'].shift(1)

        return [status, d.sort_values(by='ts')]

