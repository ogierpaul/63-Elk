COPY uknamescomplete
FROM '/shared_volume/input/uknamescompleteclean.csv'
CSV HEADER DELIMITER '|' ENCODING 'UTF-8';