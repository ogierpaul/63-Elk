CREATE TABLE IF NOT EXISTS elasticresults.staging_results (
    data JSON
);
COMMENT ON TABLE elasticresults.staging_results IS 'Staging table for the JSON Lines file extracted from the ElasticSearch Queries';

CREATE MATERIALIZED VIEW IF NOT EXISTS elasticresults.a_index
AS
--8m41se with 1900k records
SELECT
   (data->>'row_id_source')::INTEGER as row_id_source,
    (data->'es_results'->>'_id')::INTEGER as row_id_target,
   data->'es_results'->>'_score' as es_score,
    -- es_score: total score (unique for each pid), careful the score ist not totally symetric, score(a,b) <> score (b,a) --> we will need to average that later
    data->'es_results'->'_explanation'->'details' as details
FROM elasticresults."staging_results";
COMMENT ON MATERIALIZED VIEW elasticresults.a_index IS 'JSON with the keys if the source and target row, row_id_source, row_id_target';



CREATE MATERIALIZED VIEW IF NOT EXISTS elasticresults.b_pairs
AS
-- 11s with 1900k records
SELECT
-- puid: pair unique id ('bar-foo')
-- pid: pair id ('foo-bar' OR 'bar-foo')
array_to_string(sort(ARRAY[row_id_source, row_id_target]),'-') as puid,
array_to_string(ARRAY[row_id_source, row_id_target] ,'-') as pid,
row_id_source,
row_id_target,
es_score,
details
FROM  elasticresults.a_index
WHERE a_index.row_id_source<>a_index.row_id_target;
COMMENT ON MATERIALIZED VIEW elasticresults.b_pairs IS 'Create unique pair id (puid). Remove exact slef-matches : ex : (1,1), or (2,2)';


CREATE MATERIALIZED VIEW IF NOT EXISTS elasticresults.c_explode
AS
--1m10 sec with 1900k records input, 4900k output
SELECT
array_to_string(sort(ARRAY[row_id_source, row_id_target]),'-') as puid,
array_to_string(ARRAY[row_id_source, row_id_target] ,'-') as pid,
-- details: json containing each details of one component of the score , unique in (col_name, / term): you can have one row for firstname Claire, and the second one for firstname Marie, while the complete document is "Claire Marie" as first name
json_array_elements(details) AS termdetails
FROM elasticresults.b_pairs;
COMMENT ON MATERIALIZED VIEW elasticresults.c_explode IS 'Explode the score details to obtain information per each term';


CREATE MATERIALIZED VIEW IF NOT EXISTS elasticresults.d_normalize
AS
    -- We parse the column name (attribute) from the description, as well as the score value
    -- the parsing could be improved (regex instead of split?)!!
    -- 1m46s with 4900k records
    SELECT
        puid,
        pid,
        split_part(split_part(split_part((termdetails->>'description')::VARCHAR, '(', 2), ' ', 1),  ':', 1) as col_name,
        (termdetails->>'value')::FLOAT as value
FROM
    elasticresults.c_explode;
COMMENT ON MATERIALIZED VIEW elasticresults.d_normalize IS 'Parse from term & score description. We parse the column name (attribute) from the description, as well as the score value';


CREATE MATERIALIZED VIEW IF NOT EXISTS elasticresults.e_groupcol
AS
-- 25s with 4900k records input, 3700k records output
SELECT
        puid,
        pid,
       col_name,
       SUM(value) as value
-- at this stage we have one value per pid per column name (route, surname, firstnames...)
-- we still have two values for each puid, one for each pid
FROM elasticresults.d_normalize
GROUP BY puid, pid, col_name;
COMMENT ON MATERIALIZED VIEW elasticresults.e_groupcol IS 'Group score details per column name. at this stage we have one value per pid per column name (route, surname, firstnames...). we still have two values for each puid, one for each pid';

CREATE MATERIALIZED VIEW IF NOT EXISTS elasticresults.f_grouppuid
AS
-- 14s with 2500k records output
SELECT
       --now we have aggregated the scores per col_name, deduplicated the scores per puid, we are ready to use it in production
    puid,
       col_name,
       avg(value) as value
FROM elasticresults.e_groupcol
GROUP BY puid, col_name;
COMMENT ON MATERIALIZED VIEW elasticresults.f_grouppuid IS 'Group results per pair unique id. aggregated the scores per col_name, deduplicated the scores per puid.';


CREATE MATERIALIZED VIEW IF NOT EXISTS elasticresults.g_pivot AS
-- 7seconds with 1200k records
SELECT
    row_name as puid,
    firstnames as firstnames_tfidf,
    locality as locality_tfidf,
    postalcodelong as postalcodelong_tfidf,
    route as route_tfidf,
    surname as surname_tfidf,
    title as title_tfidf

FROM
(SELECT *
FROM crosstab(
    'SELECT puid as row_name, col_name as cat, value as value FROM elasticresults.f_grouppuid ORDER BY row_name, cat;',
    $$VALUES ('firstnames'::text), ('locality'::TEXT), ('postalcodelong'::TEXT), ('route'::TEXT), ('surname'::TEXT), ('title'::TEXT)$$
    )
AS ct(row_name TEXT, firstnames FLOAT,  locality FLOAT, postalcodelong FLOAT, route FLOAT,  surname FLOAT, title FLOAT )
) b
ORDER BY puid ASC;
COMMENT ON MATERIALIZED VIEW elasticresults.g_pivot IS 'Pivot the table with the puid as index. Only columns defined in the view are selected. Modify the pivot table query  if nore score columns are added.';



CREATE TABLE IF NOT EXISTS elasticresults.tfdif(
    puid VARCHAR PRIMARY KEY,
    firstnames_tfidf FLOAT,
    surname_tfidf FLOAT,
    locality_tfidf FLOAT,
    postalcodelong_tfidf FLOAT,
    route_tfidf FLOAT,
    title_tfidf FLOAT);
COMMENT ON TABLE elasticresults.tfidf IS 'Persistent table containing the score data. Modify the table  query  if nore score columns are added.';

