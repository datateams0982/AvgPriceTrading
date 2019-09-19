from tqdm import tqdm
import numpy as np  
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
import pandas as pd


#Split Training/Validation/Testing data
def TrainTestSplit(data, train_ratio=0.6, test_ratio=0.2):

    data = data[data.ROC_20.notnull()]

    End_datelist = data[abs(data['WeeklyReturn']) != 9999]['ts'].dt.date.unique()
    train_date_idx = int(len(End_datelist) * train_ratio)
    val_date_idx = int(len(End_datelist) * (1-test_ratio))

    d1 = End_datelist[train_date_idx]
    d2 = End_datelist[val_date_idx]

    split_train_val_idx = data.loc[(data['ts'].dt.date==d1)].index[-1]
    split_val_test_idx = data.loc[(data['ts'].dt.date==d2)].index[-1]

    train_df = data.iloc[:split_train_val_idx+1]
    val_df = data.iloc[split_train_val_idx+1:split_val_test_idx+1]
    test_df = data.iloc[split_val_test_idx+1:]

    return [train_df, val_df, test_df]


##Form Training shape
#Standardized
def create_dataset_standardized(dataset, lookback):
    dataX = []
    dataY_abs = []
    dataY_ATR = []
    
    End_datelist = dataset[abs(dataset['WeeklyReturn']) != 9999]['ts'].dt.date.unique()
    Start_datelist = dataset[dataset['WeeklyReturn'] == -9999]['ts'].dt.date.unique()
    
    for i, date in tqdm(enumerate(Start_datelist)):
        if (i - lookback) < 0:
            continue
            
        start = Start_datelist[i - lookback]
        this_ds = dataset[(dataset['ts'].dt.date < date) & (dataset['ts'].dt.date >= start)].reset_index(drop=True).copy()
        numerical = this_ds[['open', 'high', 'low', 'close', 'vol', 'vwap', 'open_index',
                        'high_index', 'low_index', 'close_index', 'vol_index', 'Intramonth', 'IntramonthPeriod', 'WeekNum', 'MA20', 'return', 'TR', 'SATR', 'EATR',
                        'AD', 'pos_DM', 'neg_DM', 'pos_DI', 'neg_DI', 'EMA', 'CMO', 'CCI',
                        'uBBAND', 'lBBAND', 'OBV', 'ROC_20', 'WR']]
        sc = StandardScaler()
        a = sc.fit_transform(numerical)
        this_ds[['open', 'high', 'low', 'close', 'vol', 'vwap', 'open_index',
                        'high_index', 'low_index', 'close_index', 'vol_index', 'Intramonth', 'IntramonthPeriod', 'WeekNum', 'MA20', 'return', 'TR', 'SATR', 'EATR',
                        'AD', 'pos_DM', 'neg_DM', 'pos_DI', 'neg_DI', 'EMA', 'CMO', 'CCI',
                        'uBBAND', 'lBBAND', 'OBV', 'ROC_20', 'WR']] = a[:]
        
        my_ds = this_ds[['open', 'high', 'low', 'close', 'vol', 'vwap', 'open_index',
                        'high_index', 'low_index', 'close_index', 'vol_index', 'Intramonth', 'IntramonthPeriod', 'WeekNum', 'MA20', 'return', 'TR', 'SATR', 'EATR',
                        'AD', 'pos_DM', 'neg_DM', 'pos_DI', 'neg_DI', 'EMA', 'CMO', 'CCI',
                        'uBBAND', 'lBBAND', 'OBV', 'ROC_20', 'WR', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
                        'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']]
        dataX.append(np.array(my_ds))
        
        Y_abs = dataset[(dataset['ts'].dt.date == End_datelist[i])]['y_abs'].iloc[0]
        Y_ATR = dataset[(dataset['ts'].dt.date == End_datelist[i])]['y_ATR'].iloc[0]
        
        dataY_abs.append(Y_abs)
        dataY_ATR.append(Y_ATR)

    label_encoder = LabelEncoder()
    onehot_encoder = OneHotEncoder(sparse=False)
    y_abs_int, y_atr_int = label_encoder.fit_transform(dataY_abs).reshape(len(dataY_abs), 1), label_encoder.fit_transform(dataY_ATR).reshape(len(dataY_ATR), 1)
    y_abs, y_ATR = onehot_encoder.fit_transform(y_abs_int), onehot_encoder.fit_transform(y_atr_int)    
        
    return [dataX, y_abs, y_ATR]


#MinMax
def create_dataset_MinMax(dataset, lookback):
    dataX = []
    dataY_abs = []
    dataY_ATR = []
    
    End_datelist = dataset[abs(dataset['WeeklyReturn']) != 9999]['ts'].dt.date.unique()
    Start_datelist = dataset[dataset['WeeklyReturn'] == -9999]['ts'].dt.date.unique()
    
    for i, date in tqdm(enumerate(Start_datelist)):
        if (i - lookback) < 0:
            continue
            
        start = Start_datelist[i - lookback]
        this_ds = dataset[(dataset['ts'].dt.date < date) & (dataset['ts'].dt.date >= start)].reset_index(drop=True).copy()
        numerical = this_ds[['open', 'high', 'low', 'close', 'vol', 'vwap', 'open_index',
                        'high_index', 'low_index', 'close_index', 'vol_index', 'Intramonth', 'IntramonthPeriod', 'WeekNum', 'MA20', 'return', 'TR', 'SATR', 'EATR',
                        'AD', 'pos_DM', 'neg_DM', 'pos_DI', 'neg_DI', 'EMA', 'CMO', 'CCI',
                        'uBBAND', 'lBBAND', 'OBV', 'ROC_20', 'WR']]
        sc = MinMaxScaler(feature_range = (0, 1))
        a = sc.fit_transform(numerical)
        this_ds[['open', 'high', 'low', 'close', 'vol', 'vwap', 'open_index',
                        'high_index', 'low_index', 'close_index', 'vol_index', 'Intramonth', 'IntramonthPeriod', 'WeekNum', 'MA20', 'return', 'TR', 'SATR', 'EATR',
                        'AD', 'pos_DM', 'neg_DM', 'pos_DI', 'neg_DI', 'EMA', 'CMO', 'CCI',
                        'uBBAND', 'lBBAND', 'OBV', 'ROC_20', 'WR']] = a[:]
        
        my_ds = this_ds[['open', 'high', 'low', 'close', 'vol', 'vwap', 'open_index',
                        'high_index', 'low_index', 'close_index', 'vol_index', 'Intramonth', 'IntramonthPeriod', 'WeekNum', 'MA20', 'return', 'TR', 'SATR', 'EATR',
                        'AD', 'pos_DM', 'neg_DM', 'pos_DI', 'neg_DI', 'EMA', 'CMO', 'CCI',
                        'uBBAND', 'lBBAND', 'OBV', 'ROC_20', 'WR', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
                        'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']]
        dataX.append(np.array(my_ds))
        
        Y_abs = dataset[(dataset['ts'].dt.date == End_datelist[i])]['y_abs'].iloc[0]
        Y_ATR = dataset[(dataset['ts'].dt.date == End_datelist[i])]['y_ATR'].iloc[0]
        
        dataY_abs.append(Y_abs)
        dataY_ATR.append(Y_ATR)
        

    label_encoder = LabelEncoder()
    onehot_encoder = OneHotEncoder(sparse=False)
    y_abs_int, y_atr_int = label_encoder.fit_transform(dataY_abs).reshape(len(dataY_abs), 1), label_encoder.fit_transform(dataY_ATR).reshape(len(dataY_ATR), 1)
    y_abs, y_ATR = onehot_encoder.fit_transform(y_abs_int), onehot_encoder.fit_transform(y_atr_int)    
        
    return [dataX, y_abs, y_ATR]
