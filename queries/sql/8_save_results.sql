CREATE TABLE mlm.y_true (
    puid VARCHAR PRIMARY KEY ,
    y_true SMALLINT
);

COPY mlm.y_true
FROM '/shared_volume/staging/uk_y_true.csv'
CSV HEADER DELIMITER ',' ENCODING 'UTF-8';

