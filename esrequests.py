import elasticsearch
import psycopg2
import pandas as pd

def pg_hook():
    return psycopg2.connect(dbname="mydb", user="myuser", password="mypassword", host='localhost')

def es_hook():
    return elasticsearch.Elasticsearch(http_auth=('elastic', 'changeme'))


if __name__ == '__main__':
    conn = pg_hook()
    e = elasticsearch.Elasticsearch(http_auth=('elastic', 'changeme'))
    df = pd.read_sql()
    print(e.info(pretty=True, human=True))
