CREATE EXTENSION IF NOT EXISTS multicorn;

DROP SERVER IF EXISTS multicorn_suricate CASCADE ;
CREATE SERVER IF NOT EXISTS multicorn_suricate FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'pg_es_suricate.SuricateFDW'
);

DROP FOREIGN TABLE IF EXISTS nameix;
CREATE FOREIGN TABLE nameix
    (
        id BIGINT,
        title TEXT,
        body TEXT,
        metadata JSON,
        query TEXT,
        score NUMERIC,
        sort TEXT
    )
SERVER multicorn_suricate
OPTIONS
    (
        host 'elasticsearch2',
        port '9201',
        index 'nameix',
        rowid_column 'id',
        query_column 'query',
        score_column 'score',
        default_sort 'name:desc',
        sort_column 'name',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme'
    )
;

SELECT * FROM nameix;



SELECT json_build_object('query',json_build_object('match_all', json_build_object()))::TEXT;