 # Average Price Trading
 
 ## Data Preprocess

* Read and join csv
* Aggregate data
* Extract OHLCV
* Filling missing timestamps


## Feature Engineering

* Time Feature: Intra-daily, daily, week-day, weekly, monthly
* Previous Day OHLCV
* Previous Day (same bin) OHLC, cumulative volume
* Technical Indicators
* Return
* Dependent Variable


## Training Preparation

* Feature Standardization
* Transform to (batch, time, feature) shape
* Split into Training and Testing Data


## Model 

* Model
* Hyperparameter Tuning
* Evaluation
* Result Printing
