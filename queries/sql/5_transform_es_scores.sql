-- in this script, we process the json files received from ElasticSearch (via the python script)
-- the aim is finally to create a table for each unique pair
-- |unique_pair_id (puid) | firstnames_tfidf | surname_tfidf | route_tfidf | locality_tfidf |

CREATE EXTENSION IF NOT EXISTS tablefunc; --extension used for pivot table (crosstable)
CREATE EXTENSION IF NOT EXISTS intarray; -- extension used to sort arraay (sort)

TRUNCATE TABLE esqueries.a_index;
INSERT INTO esqueries.a_index
--8m41se with 1900k records
SELECT
   (data->>'source_row_id')::INTEGER as source_row_id,
    (data->'es_results'->>'_id')::INTEGER as target_row_id,
   data->'es_results'->>'_score' as es_score,
    -- es_score: total score (unique for each pid), careful the score ist not totally symetric, score(a,b) <> score (b,a) --> we will need to average that later
    data->'es_results'->'_explanation'->'details' as details
FROM esqueries."staging";



TRUNCATE TABLE esqueries.b_pairs;
INSERT INTO esqueries.b_pairs
-- 11s with 1900k records
SELECT
-- puid: pair unique id ('bar-foo')
-- pid: pair id ('foo-bar' OR 'bar-foo')
array_to_string(sort(ARRAY[source_row_id, target_row_id]),'-') as puid,
array_to_string(ARRAY[source_row_id, target_row_id] ,'-') as pid,
source_row_id,
target_row_id,
es_score,
details
FROM  esqueries.a_index
WHERE a_index.source_row_id<>a_index.target_row_id;



TRUNCATE TABLE esqueries.c_explode;
INSERT INTO esqueries.c_explode
--1m10 sec with 1900k records input, 4900k output
SELECT
array_to_string(sort(ARRAY[source_row_id, target_row_id]),'-') as puid,
array_to_string(ARRAY[source_row_id, target_row_id] ,'-') as pid,
-- details: json containing each details of one component of the score , unique in (col_name, / term): you can have one row for firstname Claire, and the second one for firstname Marie, while the complete document is "Claire Marie" as first name
json_array_elements(details) AS termdetails
FROM esqueries.b_pairs;


TRUNCATE TABLE esqueries.d_normalize;
INSERT INTO esqueries.d_normalize
    -- We parse the column name (attribute) from the description, as well as the score value
    -- the parsing could be improved (regex instead of split?)!!
    -- 1m46s with 4900k records
    SELECT
        puid,
        pid,
        split_part(split_part(split_part((termdetails->>'description')::VARCHAR, '(', 2), ' ', 1),  ':', 1) as col_name,
        (termdetails->>'value')::FLOAT as value
FROM
    esqueries.c_explode;

TRUNCATE TABLE esqueries.e_groupcol;
INSERT INTO esqueries.e_groupcol
-- 25s with 4900k records input, 3700k records output
SELECT
        puid,
        pid,
       col_name,
       SUM(value) as value
-- at this stage we have one value per pid per column name (route, surname, firstnames...)
-- we still have two values for each puid, one for each pid
FROM esqueries.d_normalize
GROUP BY puid, pid, col_name;


TRUNCATE TABLE esqueries.f_grouppuid;
INSERT INTO esqueries.f_grouppuid
-- 14s with 2500k records output
SELECT
       --now we have aggregated the scores per col_name, deduplicated the scores per puid, we are ready to use it in production
    puid,
       col_name,
       avg(value) as value
FROM esqueries.e_groupcol
GROUP BY puid, col_name;


TRUNCATE TABLE esqueries.g_pivot;
INSERT INTO esqueries.g_pivot
-- 7seconds with 1200k records
SELECT
    row_name as puid,
    firstnames as firstnames_tfidf,
    surname as surname_tfidf,
    route as route_tfidf,
    rocality as locality_tfidf
FROM
(SELECT *
FROM crosstab(
    'SELECT puid as row_name, col_name as cat, value as value FROM esqueries.f_grouppuid ORDER BY row_name, cat;',
    $$VALUES ('firstnames'::text), ('rocality'::TEXT), ('route'::TEXT), ('surname'::TEXT)$$
    )
AS ct(row_name TEXT, firstnames FLOAT,  rocality FLOAT, route FLOAT,  surname FLOAT )
) b
ORDER BY puid ASC;



