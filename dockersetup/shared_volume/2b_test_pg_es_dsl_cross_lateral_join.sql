DROP SERVER IF EXISTS multicorn_es CASCADE ;

CREATE SERVER IF NOT EXISTS  multicorn_es FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'pg_es_fdw.ElasticsearchFDW'
);


DROP FOREIGN TABLE IF EXISTS fdwes.dsl_target;
CREATE FOREIGN TABLE IF NOT EXISTS fdwes.dsl_target
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
        default_sort 'score:desc',
        sort_column 'score:desc',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme',
        query_dsl 'true',
        scroll_duration '0',
        scroll_size '10'
    )
;

DROP MATERIALIZED VIEW IF EXISTS fdwes.dsl_query CASCADE ;
CREATE MATERIALIZED VIEW fdwes.dsl_query AS
    -- nesting the array with the different terms in its proper place
SELECT
       row_id AS pg_id,
       json_build_object(
           'query', json_build_object(
               'bool', jsonb_build_object(
                   'should',  jsar
               )
           )
       )::TEXT as query
FROM elasticqueries.c_array_wo_nulls;



SELECT t.pg_id, t.query, c.row_id as es_is, c.firstnames, c.surname, c.score
FROM (SELECT * FROM fdwes.dsl_query LIMIT 5) t
CROSS JOIN LATERAL (SELECT * FROM fdwes.dsl_target b WHERE b.query =t.query LIMIT 10) c;