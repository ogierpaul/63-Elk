DROP SERVER IF EXISTS multicorn_suricate CASCADE ;

CREATE SERVER IF NOT EXISTS  multicorn_suricate FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'suricate_fdw.SuricateFDW'
);


DROP FOREIGN TABLE IF EXISTS fdwes.target_jq;
CREATE FOREIGN TABLE IF NOT EXISTS fdwes.target_jq(
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

-- SELECT pg_id, query, response
-- FROM fdwes.target_jq
-- WHERE query ='{"query" : {"bool" : {"should": [{"match": {"surname": {"query": "WALLS", "fuzziness": 1}}}, {"match": {"firstnames": {"query": "Kenneth Charle", "fuzziness": 1}}}, {"match": {"route": {"query": "Squirrel Rise", "fuzziness": 1}}}, {"match": {"locality": {"query": "Marlow", "fuzziness": 1}}}, {"match": {"postalcodelong": {"query": "SL7 3PN", "fuzziness": 0}}}]}}}' AND pg_id=1;

DROP MATERIALIZED VIEW IF EXISTS fdwes.z_text;
CREATE MATERIALIZED VIEW fdwes.z_text AS
    -- nesting the array with the different terms in its proper place
SELECT
       row_id_source as pg_id,
       body::TEXT as query
FROM fdwes.z_final_body;

-- SELECT * FROM fdwes.z_text LIMIT 1;

DROP FUNCTION IF EXISTS es_search(body TEXT, i INTEGER);
CREATE OR REPLACE FUNCTION es_search(body TEXT, i INTEGER)
RETURNS TABLE (pg_id INTEGER, query TEXT, response TEXT)
AS
$$
BEGIN
    RAISE NOTICE 'Sql Notice: pg_id %', i;
RETURN QUERY
SELECT
    b."pg_id",
    b."query",
    b."response"
FROM
    fdwes.target_jq b
WHERE b."query"=body and b."pg_id" =i;
END
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE STRICT ;
--
-- SELECT row_id_source,
--        body::TEXT
-- FROM fdwes.z_final_body LIMIT 5;
--SELECT COUNT(*) FROM es_search('{"query" : {"bool" : {"should": [{"match": {"surname": {"query": "WALLS", "fuzziness": 1}}}, {"match": {"firstnames": {"query": "Kenneth Charle", "fuzziness": 1}}}, {"match": {"route": {"query": "Squirrel Rise", "fuzziness": 1}}}, {"match": {"locality": {"query": "Marlow", "fuzziness": 1}}}, {"match": {"postalcodelong": {"query": "SL7 3PN", "fuzziness": 0}}}]}}}', 1);

-- SELECT * FROM fdwes.z_text;
--
DROP TABLE IF EXISTS fdwes.raw_results;
CREATE TABLE IF NOT EXISTS fdwes.raw_results as

EXPLAIN (FORMAT JSON) SELECT f.pg_id, f.response
FROM (SELECT * FROM fdwes.z_text ) t
CROSS JOIN LATERAL es_search(t.query, t.pg_id) f;
--
-- SELECT f.* FROM fdwes.z_text t, es_search(t.query, t.pg_id) f;
SELECT COUNT(*) FROM fdwes.z_text;
-- CREATE MATERIALIZED VIEW fdwes.z_final_body AS
--     -- nesting the array with the different terms in its proper place
-- SELECT
--        row_id AS row_id_source,
--        json_build_object(
--            'query', json_build_object(
--                'bool', jsonb_build_object(
--                    'should',  jsar
--                )
--            )
--        ) as body
-- FROM elasticqueries.c_array_wo_nulls;
--
-- CREATE TABLE fdwes.raw_results (
--     pg_id INTEGER,
--     query TEXT,
--     response TEXT
-- );
--
--
--
-- CREATE TABLE foo as
-- SELECT fdwes.es_search(body::TEXT, row_id_source) as b
-- FROM fdwes.z_final_body LIMIT 5;

--
-- INSERT INTO fdwes.raw_results
-- SELECT b.pg_id, b.query, b.response from (SELECT fdwes.es_search(body::TEXT, row_id_source) FROM fdwes.z_final_body LIMIT 5) b;
--
--
