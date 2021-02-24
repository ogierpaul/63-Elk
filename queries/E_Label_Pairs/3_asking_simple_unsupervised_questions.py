

import psycopg2
import psycopg2.sql as S
import pandas as pd
import sqlalchemy
from suricate.explore import  SimpleQuestions

from dockersetup.matchingdbutils import pg_hook, read_on_puid, pg_engine

#TODO: Add cluster_uid to sbs as one of the first columns
if __name__ == '__main__':
    conn = pg_hook()
    conn.autocommit = True
    eng = pg_engine()
    q_pg =  """
    SELECT puid, cluster_uid FROM exploration.xcluster
    WHERE NOT EXISTS(SELECT puid FROM exploration.ytrue WHERE exploration.ytrue.puid = exploration.xcluster.puid);
    """
    df = pd.read_sql(q_pg, conn)
    df['row_id_source'] = df['puid'].str.split('-').str[0].astype(int)
    df['row_id_target'] = df['puid'].str.split('-').str[1].astype(int)
    X_cluster = df[['row_id_source', 'row_id_target', 'puid', 'cluster_uid']]\
    .set_index(['row_id_source', 'row_id_target'], drop=False)[['puid', 'cluster_uid']]
    sq = SimpleQuestions(n_questions=10)
    ix_sq = sq.fit_predict(X_cluster['cluster_uid'])
    ix_sq_puid = pd.Index(X_cluster.loc[ix_sq, 'puid'].values, name='puid')
    df_sbs= read_on_puid(ix_sq_puid, "exploration", "sbs", conn=conn, eng=eng)
    df_sbs.to_csv('/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/c_labelling_data/simple_questions.csv', index=True, encoding='utf-8', sep=',')
