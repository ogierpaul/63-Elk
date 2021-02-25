DROP SERVER IF EXISTS multicorn_es CASCADE ;

CREATE SERVER IF NOT EXISTS  multicorn_es FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'pg_es_fdw.ElasticsearchFDW'
);


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
SERVER multicorn_es
OPTIONS
    (
        host 'elasticsearch',
        port '9200',
        index 'pgtarget',
        rowid_column 'row_id',
        query_column 'query',
        score_column 'score',
--         default_sort 'score:desc',
--         sort_column 'score',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme'
    )
;

DROP MATERIALIZED VIEW IF EXISTS fdwes.lucene_query CASCADE ;
CREATE MATERIALIZED VIEW fdwes.lucene_query AS
    -- nesting the array with the different terms in its proper place
SELECT
       row_id as pg_id,
       'firstnames:' || split_part(firstnames, ' ', 1) as query
FROM source.attributes LIMIT 400;

DROP MATERIALIZED VIEW IF EXISTS fdwes.lucene_results;
CREATE MATERIALIZED VIEW fdwes.lucene_results
AS
SELECT t.pg_id, t.query, c.row_id as es_is, c.firstnames, c.surname
FROM (SELECT * FROM fdwes.lucene_query ) t
CROSS JOIN LATERAL (SELECT * FROM fdwes.lucene_target b WHERE b.query =t.query LIMIT 10) c;