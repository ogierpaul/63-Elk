import elasticsearch
import psycopg2
import pandas as pd
import sqlalchemy
import psycopg2.sql as S
import logging

from dockersetup.matchingdbutils import pg_hook, es_hook

def main(export_filepath, n_rows):
    """
    Loads the body of the query from PostGres
    Send it to ElasticSearch
    Write the results as a JSON Lines file in export filepath
    :param export_filepath: str:
    :param n_rows: int:
    :return: None
    """
    import logging

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    log_format = '%(asctime)s %(filename)s: %(message)s'
    logging.basicConfig(format=log_format,
                        datefmt='%Y-%m-%d %H:%M:%S')
    logger.warning('start')

    conn = pg_hook()
    conn.autocommit = True
    e = es_hook()
    q_pg = S.SQL("SELECT row_id_source, body FROM elasticqueries.z_final_body LIMIT {n_rows};").format(n_rows=S.Literal(n_rows))
    df = pd.read_sql(q_pg, conn)
    logger.warning(df.shape[0])
    df['es_results']=df['body'].apply(lambda s:e.search(index='pgtarget', body=s).get('hits').get('hits'))
    logger.info('Loaded all results from ES')
    df2 = df[['row_id_source', 'es_results']].set_index('row_id_source').explode('es_results')
    df2.reset_index(drop=False).to_json(export_filepath, orient='records', lines=True)
    logger.warning('done')
    return None


if __name__ == '__main__':
    # export_filepath is the place where you store the results
    # around 0.02 seconds per request = 50 requests per seconds --> 1.25 hour for 200k records
    export_filepath = '/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/b_es_results/es_output.json'
    n_rows = 35000
    main(export_filepath, n_rows)

