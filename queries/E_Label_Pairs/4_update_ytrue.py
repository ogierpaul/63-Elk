import psycopg2
import psycopg2.sql as S
import pandas as pd
import sqlalchemy
from suricate.explore import  SimpleQuestions

from dockersetup.matchingdbutils import pg_hook, read_on_puid, pg_engine

def copy_ytrue(fp_answers, fp_staging_py, fp_staging_pg):
    df_answers=pd.read_csv(fp_answers, encoding='utf-8', sep=',')
    df_answers = df_answers[['puid', 'y_true']]
    df_answers['y_true'] = df_answers['y_true'].astype(int)
    df_answers.to_csv(fp_staging_py, encoding='utf-8', sep=',', index=False)
    conn = pg_hook()
    cur = conn.cursor()
    q_stage_y_true = S.SQL("""
    COPY exploration.staging_ytrue
    FROM {fp}
    CSV HEADER ENCODING 'UTF-8' DELIMITER ',';
    """).format(fp=S.Literal(fp_staging_pg))
    cur.execute(q_stage_y_true)
    q_merge_ytrue = S.SQL("""
    INSERT INTO exploration.ytrue
    SELECT puid, y_true
    FROM exploration.staging_ytrue
    ON CONFLICT (puid) DO UPDATE
    SET y_true = excluded.y_true,
        update_ts = current_timestamp;
    """)
    cur.execute(q_merge_ytrue)
    conn.close()
    return None


if __name__ == '__main__':
    filepath_answers = '/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/c_labelling_data/y_true.csv'
    filepath_staging_py = '/Users/pogier/Documents/63-Elk/dockersetup/shared_volume/staging/c_labelling_data/stage_ytrue.csv'
    filepath_staging_pg = '/shared_volume/staging/c_labelling_data/stage_ytrue.csv'
    copy_ytrue(filepath_answers, filepath_staging_py, filepath_staging_pg)