CREATE TABLE IF NOT EXISTS exploration.staging_xcluster (
    puid VARCHAR primary key,
    y_kbins SMALLINT,
    y_kmeans SMALLINT,
    cluster_uid SMALLINT
);
COMMENT ON TABLE exploration.staging_xcluster IS 'Staging layer for cluster information';

CREATE TABLE IF NOT EXISTS exploration.xcluster (
    puid VARCHAR primary key,
    y_kbins SMALLINT,
    y_kmeans SMALLINT,
    cluster_uid SMALLINT,
    create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp
);
COMMENT ON TABLE exploration.xcluster IS 'Contains the clustering information';

CREATE TABLE IF NOT EXISTS exploration.staging_ytrue (
    puid VARCHAR primary key,
    y_true SMALLINT
);
COMMENT ON TABLE exploration.staging_xcluster IS 'Staging layer for y_true';

CREATE TABLE IF NOT EXISTS exploration.ytrue (
    puid VARCHAR primary key,
    y_true SMALLINT,
    create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp
);
COMMENT ON TABLE exploration.xcluster IS 'Contains the labelling (y_true) information.';

CREATE OR REPLACE VIEW exploration.sbs
AS
SELECT
    puid,
       firstnames_tfidf,
        firstnames_source,
       firstnames_target,
       surname_tfidf,
       surname_source,
       surname_target,
       streetnumber_source,
       streetnumbe_target,
       route_tfidf,
       route_source,
       route_target,
       locality_tfidf,
       locality_source,
       locality_target,
       postalcodelong_tfidf,
       postalcodelong_source,
       postalcodelong_target,
        title_tfidf,
       title_source,
       title_target,
       ni_number_source,
       ni_number_target,
       email_source,
       email_target,
       phone_source,
       phone_target,
       row_id_source,
       row_id_target
FROM (
    SELECT
        *,
        split_part(puid, '-', 1) ::INTEGER as row_id_source,
        split_part(puid, '-', 2) ::INTEGER as row_id_target
    FROM elasticresults.tfidf
    ) g
LEFT JOIN (
    SELECT row_id as row_id_source,
        title as  title_source,
    firstnames AS firstnames_source,
    surname AS surname_source,
    streetnumber as streetnumber_source,
    route as route_source,
    postalcodelong as postalcodelong_source,
    locality as locality_source,
    ni_number as ni_number_source,
    email as email_source,
    phone as phone_source
    FROM target.attributes
    ) s USING (row_id_source)
LEFT JOIN (
    SELECT row_id as row_id_target,
           title as title_target,
    firstnames AS firstnames_target,
    surname AS surname_target,
    streetnumber as streetnumbe_target,
    route as route_target,
    postalcodelong as postalcodelong_target,
    locality as locality_target,
    ni_number as ni_number_target,
    email as email_target,
    phone as phone_target
    FROM target.attributes
    ) t USING(row_id_target)
;
COMMENT ON VIEW exploration.sbs IS 'Side-by-side view of the data with scores';

CREATE OR REPLACE VIEW exploration.simplesbs
AS
SELECT
    puid,
        firstnames_source,
       firstnames_target,
       surname_source,
       surname_target,
       streetnumber_source,
       streetnumbe_target,
       route_source,
       route_target,
       locality_source,
       locality_target,
       postalcodelong_source,
       postalcodelong_target,
       title_source,
       title_target,
       ni_number_source,
       ni_number_target,
       email_source,
       email_target,
       phone_source,
       phone_target,
       row_id_source,
       row_id_target
FROM (
    SELECT
        puid,
        split_part(puid, '-', 1) ::INTEGER as row_id_source,
        split_part(puid, '-', 2) ::INTEGER as row_id_target
    FROM elasticresults.tfidf
    ) g
LEFT JOIN (
    SELECT row_id as row_id_source,
        title as  title_source,
    firstnames AS firstnames_source,
    surname AS surname_source,
    streetnumber as streetnumber_source,
    route as route_source,
    postalcodelong as postalcodelong_source,
    locality as locality_source,
    ni_number as ni_number_source,
    email as email_source,
    phone as phone_source
    FROM source.attributes
    ) s USING (row_id_source)
LEFT JOIN (
    SELECT row_id as row_id_target,
           title as title_target,
    firstnames AS firstnames_target,
    surname AS surname_target,
    streetnumber as streetnumbe_target,
    route as route_target,
    postalcodelong as postalcodelong_target,
    locality as locality_target,
    ni_number as ni_number_target,
    email as email_target,
    phone as phone_target
    FROM target.attributes
    ) t USING(row_id_target)
;
COMMENT ON VIEW exploration.simplesbs IS 'Side-by-side view of the data without scores';

CREATE TABLE IF NOT EXISTS exploration.temp_puid (
    puid VARCHAR primary key
);
COMMENT ON TABLE exploration.temp_puid IS 'Table as a staging layer used for quick look-ups from Pandas. Allows joins on puid';

CREATE OR REPLACE VIEW exploration.v_temp_puid AS
        SELECT
        puid,
        split_part(puid, '-', 1) ::INTEGER as row_id_source,
        split_part(puid, '-', 2) ::INTEGER as row_id_target
FROM exploration.temp_puid;
COMMENT ON VIEW exploration.v_temp_puid IS 'View on staging layer used for quick look-ups from Pandas. Allows easy joins.';


CREATE OR REPLACE VIEW exploration.v_kbins_match AS
SELECT y_kbins, AVG(y_true) as pct_true, COUNT(*) as n_pairs
FROM exploration.ytrue
INNER JOIN (SELECT puid, y_kbins FROM exploration.xcluster) c USING(puid)
GROUP BY y_kbins
ORDER BY pct_true DESC, n_pairs DESC;
COMMENT ON VIEW exploration.v_kbins_match IS 'Stats on number of matches per KBins cluster';

CREATE OR REPLACE VIEW exploration.v_kmeans_match AS
SELECT y_kmeans, AVG(y_true) as pct_true, COUNT(*) as n_pairs
FROM exploration.ytrue
INNER JOIN (SELECT puid, y_kmeans FROM exploration.xcluster) c USING(puid)
GROUP BY y_kmeans
ORDER BY pct_true DESC, n_pairs DESC;
COMMENT ON VIEW exploration.v_kmeans_match IS 'Stats on number of matches per KMeans cluster';

CREATE OR REPLACE VIEW exploration.v_ytrue_count AS
    SELECT y_true, COUNT(*)
    FROM exploration.ytrue
    GROUP BY y_true
ORDER BY y_true DESC;
COMMENT ON VIEW exploration.v_kmeans_match IS 'Stats on y_true class balancing';


