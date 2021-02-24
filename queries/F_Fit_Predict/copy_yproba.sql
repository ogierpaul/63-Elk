TRUNCATE mlm.staging_yproba;

COPY mlm.staging_yproba
FROM '/shared_volume/staging/y_proba.csv'
CSV HEADER ENCODING 'UTF-8' DELIMITER ',';


INSERT INTO mlm.yproba
SELECT puid, y_proba
FROM mlm.staging_yproba
ON CONFLICT (puid) DO UPDATE
SET y_proba = excluded.y_proba,
    update_ts = current_timestamp
;