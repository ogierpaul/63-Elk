DROP SERVER IF EXISTS multicorn_suricate CASCADE ;
CREATE SCHEMA IF NOT EXISTS fdwes;

CREATE SERVER IF NOT EXISTS  multicorn_suricate FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'suricate_fdw.SuricateFDW'
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

DROP MATERIALIZED VIEW IF EXISTS fdwes.lucene_query CASCADE ;
CREATE MATERIALIZED VIEW fdwes.lucene_query AS
    -- nesting the array with the different terms in its proper place
SELECT
       row_id as pg_id,
       'firstnames:' || split_part(firstnames, ' ', 1) as query
FROM source.attributes;
DROP TABLE IF EXISTS fdwes.es_results CASCADE ;

CREATE TABLE fdwes.es_results (
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

CREATE OR REPLACE VIEW fdwes.unmatched_lucene AS
SELECT * FROM fdwes.lucene_query
WHERE NOT EXISTS(SELECT pg_id FROM fdwes.es_results WHERE fdwes.es_results.pg_id = fdwes.lucene_query.pg_id);
