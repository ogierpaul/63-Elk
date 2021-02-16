CREATE TABLE esqueries.staging (
    data JSON
);

TRUNCATE TABLE esqueries.staging;

COPY esqueries."staging"
FROM '/shared_volume/staging/output.json';