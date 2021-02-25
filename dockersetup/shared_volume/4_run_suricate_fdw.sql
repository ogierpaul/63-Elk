
INSERT INTO fdwes.suricate_results (pg_id, es_id, score)
SELECT t.pg_id,  c.row_id as es_id, c.score
FROM (SELECT pg_id, query FROM fdwes.unmatched_queries) t
CROSS JOIN LATERAL (SELECT * FROM fdwes.suricate_target b WHERE b.query =t.query LIMIT 10) c
ON CONFLICT (pg_id, es_id) DO NOTHING ;



SELECT COUNT(DISTINCT pg_id) FROM fdwes.suricate_results;

