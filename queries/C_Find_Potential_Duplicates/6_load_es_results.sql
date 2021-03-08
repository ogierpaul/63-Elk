-- in this script, we process the json files received from ElasticSearch (via the python script)
-- the aim is finally to create a table for each unique pair
-- |unique_pair_id (puid) | firstnames_tfidf | surname_tfidf | route_tfidf | locality_tfidf |
-- WHEN modifying the scores in the es query body, do not forget to modify the scores in the z_tfidf table


TRUNCATE TABLE elasticresults.staging_results;

COPY elasticresults."staging_results"
FROM '/shared_volume/staging/b_es_results/es_output.json';


REFRESH MATERIALIZED VIEW elasticresults.a_index;

-- Update the table to keep track of rows already matched
INSERT INTO source.rows_source
    SELECT DISTINCT ON(row_id_source) row_id_source
        FROM elasticresults.a_index
    ON CONFLICT (row_id_source) DO UPDATE
        SET update_ts = current_timestamp;

REFRESH MATERIALIZED VIEW elasticresults.b_pairs;

REFRESH MATERIALIZED VIEW elasticresults.c_explode;

REFRESH MATERIALIZED VIEW elasticresults.d_normalize;

REFRESH MATERIALIZED VIEW elasticresults.e_groupcol;

REFRESH MATERIALIZED VIEW elasticresults.f_grouppuid;

REFRESH MATERIALIZED VIEW elasticresults.g_pivot;


-- update the score table to keep track of the scores
-- The score is updated by taking an average of the previous score and the new score
-- This is made in becase some times score (1,2) <> score (2,1)
INSERT INTO elasticresults.tfidf
    SELECT puid, firstnames_tfidf, surname_tfidf, locality_tfidf, postalcodelong_tfidf, route_tfidf, title_tfidf
    FROM elasticresults.g_pivot
    ON CONFLICT (puid) DO UPDATE
    SET firstnames_tfidf = (elasticresults.tfidf.firstnames_tfidf + excluded.firstnames_tfidf) /2 ,
        surname_tfidf = (elasticresults.tfidf.surname_tfidf + excluded.surname_tfidf) /2,
        locality_tfidf = (elasticresults.tfidf.locality_tfidf + excluded.locality_tfidf)/2,
        postalcodelong_tfidf = (elasticresults.tfidf.postalcodelong_tfidf + excluded.postalcodelong_tfidf) /2,
        route_tfidf = (elasticresults.tfidf.route_tfidf + excluded.route_tfidf)/2,
        title_tfidf = (elasticresults.tfidf.title_tfidf + excluded.title_tfidf) /2;
