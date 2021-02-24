CREATE MATERIALIZED VIEW IF NOT EXISTS sbs.id_exact AS
SELECT
       puid,
    sbs.exact_match(phone_source, phone_target) AS phone_exact,
    sbs.exact_match(email_source, email_target) as email_exact,
    sbs.exact_match(ni_number_source, ni_number_target) as ni_number_exact
FROM exploration.sbs;
COMMENT ON MATERIALIZED VIEW sbs.id_exact IS 'Exact_scores on Identifier columns of the exploration.sbs dataset';


CREATE MATERIALIZED VIEW IF NOT EXISTS sbs.trg_similarity AS
SELECT
    puid,
    similarity(dmetaphone(firstnames_source), dmetaphone(firstnames_target)) as firstnames_trg_metaphone,
    similarity(dmetaphone(surname_source), dmetaphone(surname_target)) as surname_trg_metaphone,
    similarity(dmetaphone(locality_source), dmetaphone(locality_target)) as locality_trg_metaphone,
    similarity(dmetaphone(route_source), dmetaphone(route_target)) as route_trg_metaphone
FROM
     (SELECT *
     FROM exploration.sbs
     ) b;
COMMENT ON MATERIALIZED VIEW sbs.id_exact IS 'Trigram similarity score of metaphone representation of attribute columns';



CREATE MATERIALIZED VIEW IF NOT EXISTS sbs.levenshtein AS
SELECT
       puid,
    sbs.leven_similarity(firstnames_source, firstnames_target) as firstnames_levenshtein,
    sbs.leven_similarity(surname_source, surname_target) as surname_levenshtein,
    sbs.leven_similarity(route_source, route_target) as route_levenshtein,
    sbs.leven_similarity(locality_source, locality_target) as locality_levenshtein,
    sbs.leven_similarity(postalcodelong_source, postalcodelong_target) as postalcodelong_levenshtein
FROM exploration.sbs;
COMMENT ON MATERIALIZED VIEW sbs.levenshtein IS 'Levenshtein similarity columns of the exploration.sbs dataset';

CREATE MATERIALIZED VIEW IF NOT EXISTS sbs.geo_distance AS
SELECT
    puid,
       --st_distance_sphere gives us the distance in meters
       -- postgis is stricky with distance, be careful of what you do
    ST_Distance_Sphere(geopoint_source, geopoint_target) as geo_dist_m
FROM (
    SELECT
        puid,
        split_part(puid, '-', 1) ::INTEGER as row_id_source,
        split_part(puid, '-', 2) ::INTEGER as row_id_target
    FROM elasticresults.tfidf
    ) g
LEFT JOIN (
    SELECT row_id as row_id_source,
           geopoint as geopoint_source
    FROM source.attributes
    WHERE geopoint IS NOT NULL
    ) s USING (row_id_source)
LEFT JOIN (
    SELECT row_id as row_id_target,
           formatteaddress as formatted_target,
           geopoint as geopoint_target
    FROM target.attributes
    WHERE geopoint IS NOT NULL
    ) t USING(row_id_target);
COMMENT ON MATERIALIZED VIEW sbs.geo_score IS 'Calculation of distance between two points';

CREATE MATERIALIZED VIEW IF NOT EXISTS sbs.geo_score AS
    SELECT
        puid,
        sbs.geo_score_ths(geo_dist_m, 2000)

    FROM sbs.geo_distance;
COMMENT ON MATERIALIZED VIEW sbs.geo_score IS 'Similarity score based on geographic distance, with a threshold of 2000m (1=same point, 0 = distance of 2000 or more)';


CREATE OR REPLACE VIEW sbs.v_geo_stats AS
SELECT y_true, min(geo_dist_m), avg(geo_dist_m), max(geo_dist_m)
FROM (
SELECT * FROM mlm.ytrue
INNER JOIN sbs.geo_distance USING(puid) ) b
GROUP BY y_true
ORDER BY y_true DESC;
COMMENT ON VIEW sbs.v_geo_stats IS 'min, max, and avg distance between two points based on their match status';


