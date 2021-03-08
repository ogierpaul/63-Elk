DROP SERVER IF EXISTS multicorn_suricate CASCADE ;
CREATE SCHEMA IF NOT EXISTS fdwes;

CREATE SERVER IF NOT EXISTS  multicorn_suricate FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'suricate_fdw.SuricateFDW'
);


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


CREATE TABLE IF NOT EXISTS fdwes.es_results (
    pg_id INTEGER,
    es_id INTEGER,
    score NUMERIC,
    _explanation TEXT DEFAULT NULL,
    updated_ts timestamp DEFAULT current_timestamp,
    PRIMARY KEY (pg_id, es_id)
);

CREATE OR REPLACE VIEW fdwes.unmatched_dsl AS
SELECT * FROM fdwes.dsl_query
WHERE NOT EXISTS(SELECT pg_id FROM fdwes.es_results WHERE fdwes.es_results.pg_id = fdwes.dsl_query.pg_id);
