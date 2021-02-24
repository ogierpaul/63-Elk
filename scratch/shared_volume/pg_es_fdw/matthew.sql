CREATE EXTENSION IF NOT EXISTS multicorn;

DROP SERVER IF EXISTS multicorn_es CASCADE ;
CREATE SERVER IF NOT EXISTS multicorn_es FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'pg_es_fdw.ElasticsearchFDW'
);

DROP FOREIGN TABLE IF EXISTS articles_es;
CREATE FOREIGN TABLE IF NOT EXISTS articles_es
    (
        id BIGINT,
        title TEXT,
        body TEXT,
        metadata JSON,
        query TEXT,
        score NUMERIC,
        sort TEXT
    )
SERVER multicorn_es
OPTIONS
    (
        host 'elasticsearch2',
        port '9201',
        index 'article-index',
        type 'article',
        rowid_column 'id',
        query_column 'query',
        score_column 'score',
        default_sort 'last_updated:desc',
        sort_column 'sort',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme'
    )
;

INSERT INTO articles_es
    (
        id,
        title,
        body,
        metadata
    )
VALUES
    (
        1,
        'foo',
        'spike',
        '{"score": 3}'::json
    );