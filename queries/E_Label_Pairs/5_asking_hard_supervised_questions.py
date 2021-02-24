import psycopg2
import psycopg2.sql as S
import pandas as pd
import sqlalchemy
from suricate.explore import HardQuestions

from dockersetup.matchingdbutils import pg_hook, read_on_puid, pg_engine

if __name__ == '__main__':
    conn = pg_hook()
    conn.autocommit = True
    eng = pg_engine()
    q_pg = """
    SELECT puid, cluster_uid, y_true
    FROM (SELECT puid, cluster_uid FROM exploration.xcluster) a
    LEFT JOIN (SELECT puid, y_true FROM exploration.ytrue) b USING (puid)
    ;
    """
    df = pd.read_sql(q_pg, conn)
    print(df.shape[0])
    df['row_id_source'] = df['puid'].str.split('-').str[0].astype(int)
    df['row_id_target'] = df['puid'].str.split('-').str[1].astype(int)
    X_cluster = df[['row_id_source', 'row_id_target', 'puid', 'cluster_uid', 'y_true']]\
    .set_index(['row_id_source', 'row_id_target'], drop=False)[['puid', 'cluster_uid', 'y_true']]
    y_true = X_cluster['y_true'].dropna().astype(int)
    hq = HardQuestions(n_questions=15)
    hq.fit(X_cluster['cluster_uid'], y_true)
    ix_hq = hq.transform(X_cluster['cluster_uid'])
    ix_hq = ix_hq.difference(y_true.es_index)
    ix_hq_puid = pd.Index(X_cluster.loc[ix_hq, 'puid'].values, name='puid')
    print(len(ix_hq_puid))
    df_sbs= read_on_puid(ix_hq_puid, "exploration", "sbs", conn=conn, eng=eng)
    print(df_sbs.shape[0])
    df_sbs.to_csv('/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/c_labelling_data/hard_questions.csv', index=True, encoding='utf-8', sep='|')
    df_sbs.to_excel('/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/c_labelling_data/hard_questions.xlsx', index=True)
