
DROP FOREIGN TABLE IF EXISTS fdwes.explain_target;
CREATE FOREIGN TABLE IF NOT EXISTS fdwes.explain_target
    (
        row_id INT,
        query TEXT,
        score NUMERIC,
        _explanation TEXT
    )
SERVER multicorn_suricate
OPTIONS
    (
        host 'elasticsearch',
        port '9200',
        index 'pgtarget',
        rowid_column 'row_id',
        query_column 'query',
        score_column 'score',
        default_sort 'score:desc',
        sort_column 'score:desc',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme',
        query_dsl 'true',
        scroll_duration '0nanos',
        scroll_size '10',
        size '10',
        explain 'true',
        explain_column '_explanation'
    );


INSERT INTO fdwes.es_results
SELECT t.pg_id,  c.row_id as es_id,  c.score, c._explanation
FROM (SELECT * FROM fdwes.unmatched_dsl LIMIT 3) t
CROSS JOIN LATERAL (SELECT * FROM fdwes.explain_target b WHERE b.query =t.query LIMIT 10) c
ON CONFLICT(pg_id, es_id) DO NOTHING ;


