
create table IF NOT EXISTS source.staging_source (
	row_id integer,
	surname varchar,
	firstnames varchar,
	title varchar,
	day integer,
	month integer,
	year integer,
	name varchar,
	country_of_residence varchar,
	nationality varchar,
	formatteaddress varchar,
	streetnumber varchar,
	route varchar,
	locality varchar,
	administrationarealevel2 varchar,
	administrationarealevel1 varchar,
	postalcodelong varchar,
	country varchar,
	longitude float,
	latitude float,
	email varchar,
	ni_number varchar,
	phone varchar,
	phone_type varchar,
	phone_country varchar
	);
COMMENT ON TABLE source.staging_source IS 'Staging Area for Source Data with common data types';


CREATE TABLE IF NOT EXISTS source.attributes (
    row_id integer PRIMARY KEY,
	surname varchar,
	firstnames varchar,
	title varchar,
	day integer,
	month integer,
	year integer,
	name varchar,
	country_of_residence varchar,
	nationality varchar,
	formatteaddress varchar,
	streetnumber varchar,
	route varchar,
	locality varchar,
	administrationarealevel2 varchar,
	administrationarealevel1 varchar,
	postalcodelong varchar,
	country varchar,
	geopoint geometry,
	email varchar,
	ni_number varchar,
	phone varchar,
	phone_type varchar,
	phone_country varchar,
	create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp
	);
COMMENT ON TABLE source.attributes IS 'Source master data (attributes), with geo-point, and change information (update_ts).';


CREATE TABLE IF NOT EXISTS source.rows_source (
    row_id_source INTEGER PRIMARY KEY,
    es_results BOOLEAN DEFAULT FALSE,
    create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp);
COMMENT ON TABLE source.rows_source IS 'Keeps track which rows have been sent to ES for matching';

CREATE OR REPLACE VIEW source.unmatched_rows AS
SELECT * FROM source.attributes
WHERE NOT EXISTS(
    SELECT * FROM source.rows_source WHERE source.rows_source.es_results IS TRUE AND source.attributes.row_id = source.rows_source.row_id_source);
COMMENT ON VIEW  source.unmatched_rows IS 'view on attributes where the rows have not yet been sent to ES for matching';
