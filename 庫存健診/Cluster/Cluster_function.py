import numpy as np 
import pandas as pd 
import datetime
from datetime import datetime, timedelta
from tqdm import tqdm_notebook as tqdm
from sklearn.preprocessing import Normalizer
from sklearn.decomposition import PCA


import pylab as pl

from sklearn.base import BaseEstimator
from sklearn.utils import check_random_state
from sklearn.cluster import MiniBatchKMeans
from sklearn.cluster import KMeans as KMeansGood
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import euclidean_distances, manhattan_distances
from sklearn.datasets.samples_generator import make_blobs

from sklearn.metrics import silhouette_score


def CorrMatrix(data, Start, End, feature='return', normalization=True):
    delta = End - Start  
    dates = [Start + timedelta(days=x) for x in range((End-Start).days)]

    df = data[data.ts.isin(dates)]
    df_list = [group[1] for group in df.groupby(df['StockNo'])]

    return_list = []
    for i, d in enumerate(tqdm(df_list, total=len(df_list))):
        Stockname = d['StockName'].unique()[0]
        d = d.sort_values(by='ts').set_index('ts')

        Series = (d[feature]).rename(Stockname)
        return_list.append(Series.iloc[1:])

    Return_df = pd.concat(return_list, axis=1)

    if normalization:
        X = Return_df.values
        nc = Normalizer(norm='l2')
        a = nc.fit_transform(X)
        Return_df.loc[:, :] = a

    corr = Return_df.corr()
    corr = corr.fillna(0)

    return corr



def DimensionReduction(data, component, method='PCA'):
    X = data.values

    if method == 'PCA':
        Reduce_model = PCA(n_components=component)
        reduce_matrix = Reduce_model.fit_transform(X)

    df = pd.DataFrame(reduce_matrix, columns=['PC{}'.format(i) for i in range(1, reduce_matrix.shape[1]+1)], index = data.index)
    explained = pd.DataFrame(Reduce_model.explained_variance_ratio_.tolist(), columns=['explained'], index=pd.Series(['PC{}'.format(i) for i in range(1, reduce_matrix.shape[1]+1)]))

    return df, explained


class KMeans(BaseEstimator):

    def __init__(self, k, max_iter=100, random_state=0, tol=1e-4):
        self.k = k
        self.max_iter = max_iter
        self.random_state = random_state
        self.tol = tol

    def _e_step(self, X):
        self.labels_ = euclidean_distances(X, self.cluster_centers_,
                                     squared=True).argmin(axis=1)

    def _average(self, X):
        return X.mean(axis=0)

    def _m_step(self, X):
        X_center = None
        for center_id in range(self.k):
            center_mask = self.labels_ == center_id
            if not np.any(center_mask):
                # The centroid of empty clusters is set to the center of
                # everything
                if X_center is None:
                    X_center = self._average(X)
                self.cluster_centers_[center_id] = X_center
            else:
                self.cluster_centers_[center_id] = \
                    self._average(X[center_mask])

    def fit(self, X, y=None):
        n_samples = X.shape[0]
        vdata = np.mean(np.var(X, 0))

        random_state = check_random_state(self.random_state)
        self.labels_ = random_state.permutation(n_samples)[:self.k]
        self.cluster_centers_ = X[self.labels_]

        for i in range(self.max_iter):
            centers_old = self.cluster_centers_.copy()

            self._e_step(X)
            self._m_step(X)

            if np.sum((centers_old - self.cluster_centers_) ** 2) < self.tol * vdata:
                break

        return self

class KMedians(KMeans):

    def _e_step(self, X):
        self.labels_ = manhattan_distances(X, self.cluster_centers_).argmin(axis=1)

    def _average(self, X):
        return np.median(X, axis=0)



def Cluster(data, weights, method, cluster_list, weighting=True):

    if weighting:
        d = np.multiply(data, weights)
        X = d.values

    else:
        X = data.values

    sc_scores = []
    label_list = []

    for i, c in enumerate(tqdm(cluster_list, total=len(cluster_list))):
        if method == 'Kmeans':
            cluster_model = KMeansGood(n_clusters=c, init='k-means++', max_iter=1000).fit(X)
        
        elif method == 'Kmedians':
            cluster_model = KMedians(k=c, max_iter=1000)
            cluster_model.fit(X)

        else:
            cluster_model = AgglomerativeClustering(n_clusters=c, affinity='euclidean', linkage='ward')
            cluster_model.fit_predict(X)

        label = cluster_model.labels_.tolist()
        label_list.append(label)

        sc_score = silhouette_score(X, cluster_model.labels_, metric='euclidean')
        sc_scores.append(sc_score)

    
    label_df = pd.DataFrame(np.stack(label_list, axis=1), columns=[str(i) for i in cluster_list], index=data.index)

    return label_df, sc_scores


def ClusterCorrMatrix(cluster_data, corr_matrix, cluster_num):
    ClusterNo = str(cluster_num)
    cluster_corr = pd.DataFrame(index=['Cluster_{}'.format(i) for i in range(1, cluster_num+1)], columns=['Cluster_{}'.format(i) for i in range(1, cluster_num+1)])

    for i in range(cluster_num):
        d_i = cluster_data[cluster_data[ClusterNo] == i]
        for j in range(i, cluster_num):
            d_j = cluster_data[cluster_data[ClusterNo] == j]
            cluster_corr.iloc[i, j], cluster_corr.iloc[j, i] = corr_matrix.loc[d_i.index.tolist(), d_j.index.tolist()].mean().mean(), corr_matrix.loc[d_i.index.tolist(), d_j.index.tolist()].mean().mean()

    
    return cluster_corr






