import psycopg2
import pandas as pd
import sqlalchemy
from pairing import pair
from suricate.explore import KBinsCluster
from sklearn.cluster import KMeans
import pickle
import logging

#TODO: Save clusters_models, configure export_path

from dockersetup.matchingdbutils import pg_hook


def tfidf_scores(conn):
    q_pg = "SELECT * FROM mlm.tfidf;"
    df = pd.read_sql(q_pg, conn)
    df['row_id_source'] = df['puid'].str.split('-').str[0].astype(int)
    df['row_id_target'] = df['puid'].str.split('-').str[1].astype(int)
    df.set_index(['puid'], inplace=True)
    return df

def kmeans_clusters(X, n_clusters=10, n_samples=100000, update_model=False):
    km_fp = '/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/ml_models/kmeans_model.pickle'
    if update_model is True:
        km = KMeans(n_clusters=n_clusters)
        km.fit(X.sample(min(X.shape[0], n_samples)))
        with open(km_fp, 'wb') as f:
            pickle.dump(km, f)
    else:
        with open(km_fp, 'rb') as f:
            km = pickle.load(f)
    y_kmeans = pd.Series(data=km.predict(X), index=X.es_index, name='y_kmeans')
    return y_kmeans

def kbins_clusters(X, n_clusters=10, update_model=False):
    kb_fp = '/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/ml_models/kbins_model.pickle'
    if update_model is True:
        kb = KBinsCluster(n_clusters=n_clusters)
        kb.fit(X)
        with open(kb_fp, 'wb') as f:
            pickle.dump(kb, f)
    else:
        with open(kb_fp, 'rb') as f:
            kb = pickle.load(f)
    y_kbins = pd.Series(data=kb.predict(X), index=X.es_index, name='y_kbins')
    return y_kbins

def cluster_uid(y1, y2):
    df_clusters = pd.concat([pd.DataFrame(y1), pd.DataFrame(y2)], axis=1, ignore_index=False)
    df_clusters['cluster_uid'] = df_clusters.apply(lambda r: pair(r[y1.name], r[y2.name]), axis=1)
    return df_clusters

def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_format = '%(asctime)s %(filename)s: %(message)s'
    logging.basicConfig(format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger.info('start')
    conn = pg_hook()
    conn.autocommit = True
    update_model = False
    n_clusters = 10
    logger.info('Load data from PG')
    df = tfidf_scores(conn)
    logger.info('Fill blanks')
    X = df[['firstnames_tfidf', 'locality_tfidf', 'route_tfidf', 'surname_tfidf']].fillna(0)
    logger.info('KBins')
    y_kbins=  kbins_clusters(X, n_clusters=n_clusters, update_model=update_model)
    logger.info('KMeans')
    y_kmeans = kmeans_clusters(X, n_clusters=n_clusters, update_model=update_model)
    logger.info('Cluster UID')
    df_clusters = cluster_uid(y_kbins, y_kmeans)
    logger.info('Save Clusters')
    for c in ['y_kbins', 'y_kmeans', 'cluster_uid']:
        df_clusters[c] = df_clusters[c].astype(int)
    df_clusters.to_csv('/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/c_labelling_data/x_clusters.csv', index=True, encoding='utf-8', sep=',')
    logger.info('Finished')
    pass
if __name__ == '__main__':
    main()















