# 庫存健診模型建置

## Crawling

* Crawling stock daily data from twse


## Data Preprocess

* Read and join daily data
* Basic data cleaning
* Generate date and return
* Filling missing timestamps
* Drop non-preserved stocks
* Drop ETF


## Cluster

* Select date
* Create correlation matrix by daily return
* Try Dimension Reduction
* Clustering by Kmeans, Kmedians, Hierarchical
* Create cluster correlation matrix
* Check results


## Feature Engineering

* Time Feature: Intra-daily, daily, week-day, weekly, monthly
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
