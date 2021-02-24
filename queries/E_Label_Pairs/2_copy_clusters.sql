TRUNCATE exploration.staging_xcluster;

COPY exploration.staging_xcluster
FROM '/shared_volume/staging/c_labelling_data/x_clusters.csv'
CSV HEADER ENCODING 'UTF-8' DELIMITER ',';


INSERT INTO exploration.xcluster
SELECT puid, y_kbins, y_kmeans, cluster_uid
FROM exploration.staging_xcluster
ON CONFLICT (puid) DO UPDATE
SET y_kbins = excluded.y_kbins,
    y_kmeans = excluded.y_kmeans,
    cluster_uid = excluded.cluster_uid,
    update_ts = current_timestamp
;