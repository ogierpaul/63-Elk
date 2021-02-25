REFRESH MATERIALIZED VIEW fdwes.dsl_query;

-- INSERT INTO fdwes.dsl_results (pg_id, query, response)
-- SELECT d.pg_id, d.query, d.response
-- FROM
-- (SELECT t.pg_id, t.query, c.response
-- FROM (SELECT * FROM fdwes.dsl_query ) t
-- CROSS JOIN LATERAL (SELECT * FROM fdwes.dsl_target b WHERE b.query = t.query LIMIT 7) c) d
-- ON CONFLICT (pg_id)
-- DO NOTHING ;
--
--SELECT * FROM fdwes.dsl_query;

SELECT * FROM fdwes.dsl_target
WHERE query = '{"query" : {"bool" : {"should": [{"match": {"surname": {"query": "WALLS", "fuzziness": 1}}}, {"match": {"firstnames": {"query": "Kenneth Charle", "fuzziness": 1}}}, {"match": {"route": {"query": "Squirrel Rise", "fuzziness": 1}}}, {"match": {"locality": {"query": "Marlow", "fuzziness": 1}}}, {"match": {"postalcodelong": {"query": "SL7 3PN", "fuzziness": 0}}}]}}}' and pg_id =1
LIMIT 1;

SELECT q.pg_id, q.query, r.response
FROM (SELECT pg_id, query FROM fdwes.dsl_query LIMIT 1) q
CROSS JOIN LATERAL (SELECT * FROM fdwes.dsl_target e WHERE (e.query = q.query::TEXT AND q.pg_id=e.pg_id) )r
LIMIT 1;