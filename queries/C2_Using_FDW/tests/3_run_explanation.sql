INSERT INTO fdwes.es_results
SELECT t.pg_id,  c.row_id as es_id,  c.score, c._explanation
FROM (SELECT * FROM fdwes.unmatched_dsl LIMIT 50000) t
CROSS JOIN LATERAL (SELECT * FROM fdwes.explain_target b WHERE b.query =t.query LIMIT 10) c
ON CONFLICT(pg_id, es_id) DO UPDATE
SET
    score = excluded.score,
    _explanation = excluded._explanation,
    updated_ts = current_timestamp
;
