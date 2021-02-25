DROP SERVER IF EXISTS multicorn_suricate CASCADE ;

CREATE SERVER IF NOT EXISTS  multicorn_suricate FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'suricate_fdw.SuricateFDW'
);


DROP FOREIGN TABLE IF EXISTS fdwes.dsl_target;
CREATE FOREIGN TABLE IF NOT EXISTS fdwes.dsl_target(
        pg_id INTEGER,
        query TEXT,
        response TEXT
    )
SERVER multicorn_suricate
OPTIONS
    (
        host 'elasticsearch',
        port '9200',
        index 'pgtarget',
        query_column 'query',
        pg_id_column 'pg_id',
        response_column 'response',
        size '10',
        explain 'true',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme'
    )
;

DROP MATERIALIZED VIEW IF EXISTS fdwes.dsl_query;
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
FROM elasticqueries.c_array_wo_nulls LIMIT 7;

-- SELECT * FROM fdwes.dsl_target
-- WHERE query = '{"query" : {"bool" : {"should": [{"match": {"surname": {"query": "WALLS", "fuzziness": 1}}}, {"match": {"firstnames": {"query": "Kenneth Charle", "fuzziness": 1}}}, {"match": {"route": {"query": "Squirrel Rise", "fuzziness": 1}}}, {"match": {"locality": {"query": "Marlow", "fuzziness": 1}}}, {"match": {"postalcodelong": {"query": "SL7 3PN", "fuzziness": 0}}}]}}}' AND pg_id = 1;
-- --
DROP MATERIALIZED VIEW IF EXISTS fdwes.dsl_results;
CREATE MATERIALIZED VIEW fdwes.dsl_results
AS
SELECT t.pg_id, t.query, c.response
FROM (SELECT * FROM fdwes.dsl_query ) t
CROSS JOIN LATERAL (SELECT * FROM fdwes.dsl_target b WHERE b.query =t.query LIMIT 7) c;
