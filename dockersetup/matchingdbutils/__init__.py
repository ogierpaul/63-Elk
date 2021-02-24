import psycopg2
import psycopg2.sql as S
import pandas as pd
import sqlalchemy
import elasticsearch

def pg_hook():
    conn =  psycopg2.connect(dbname="mydb", user="myuser", password="mypassword", host='localhost', port=5439)
    conn.autocommit = True
    return conn

def pg_engine():
    return sqlalchemy.create_engine('postgres://myuser:mypassword@localhost:5439/mydb')

def es_hook():
    return elasticsearch.Elasticsearch(http_auth=('elastic', 'changeme'))

def read_on_puid( puid, schemaname, tablename, conn, eng):
    if isinstance(puid, pd.Index):
        df = pd.DataFrame(index=puid)
        df.index.name = 'puid'
        df.reset_index(drop=False, inplace=True)
        df = df[['puid']]
    elif isinstance(puid, pd.Series):
        df = pd.DataFrame(puid)
        df.columns=['puid']
    elif isinstance(puid, pd.DataFrame):
        assert puid.shape[1] ==1
        df = puid.copy()
        df.columns=['puid']

    cur = conn.cursor()
    cur.execute("TRUNCATE exploration.temp_puid;")
    df.to_sql("temp_puid", eng, schema="exploration", if_exists='append', index=False)
    qt = S.SQL("""
    SELECT *
    FROM exploration.temp_puid
    INNER JOIN {schemaname}.{tablename} USING (puid);
    """)
    qf = qt.format(schemaname = S.Identifier(schemaname), tablename=S.Identifier(tablename))
    dfr = pd.read_sql(qf, conn).set_index('puid')
    conn.close()
    return dfr
