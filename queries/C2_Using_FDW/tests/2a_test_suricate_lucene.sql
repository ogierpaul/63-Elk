DROP FOREIGN TABLE IF EXISTS fdwes.lucene_target;
CREATE FOREIGN TABLE IF NOT EXISTS fdwes.lucene_target
    (
        row_id INT,
        title TEXT,
        firstnames TEXT,
        surname TEXT,
        name TEXT,
        streetnumber TEXT,
        route TEXT,
        locality TEXT,
        postalcodelong TEXT,
        country TEXT,
        formattedaddress TEXT,
        email TEXT,
        ni_number TEXT,
        phone TEXT,
        query TEXT,
        score NUMERIC
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
        scroll_duration '0',
        scroll_size '10',
        size '10',
        explain 'false',
        query_dsl 'false'
    )
;
INSERT INTO fdwes.es_results
SELECT t.pg_id,  c.row_id as es_id,  c.score
FROM (SELECT * FROM fdwes.unmatched_lucene LIMIT 3) t
CROSS JOIN LATERAL (SELECT * FROM fdwes.lucene_target b WHERE b.query =t.query LIMIT 10) c
ON CONFLICT(pg_id, es_id) DO NOTHING ;