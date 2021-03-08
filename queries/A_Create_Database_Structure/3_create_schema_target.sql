create table IF NOT EXISTS target.staging_target (
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
COMMENT ON TABLE target.staging_target IS 'Staging Area for Target Data with common data types'


CREATE TABLE IF NOT EXISTS target.attributes (
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
COMMENT ON TABLE target.attributes IS 'Target master data (attributes), with geo-point, and change information (update_ts).'

CREATE OR REPLACE VIEW target.to_elastic AS
SELECT
    row_id,
    surname,
       firstnames,
       title,
       day,
       month,
       year,
       name,
       country_of_residence,
       nationality,
       formatteaddress,
       streetnumber,
       route,
       locality,
       administrationarealevel2,
       administrationarealevel1,
       postalcodelong,
       country,
       st_astext(geopoint) as geopoint,
       email,
       ni_number,
       phone,
       phone_type,
       phone_country,
       update_ts,
       extract(epoch from update_ts at time zone 'utc') AS unix_ts_in_secs
FROM target.attributes;
COMMENT ON TABLE target.attributes IS 'View to load data into ElasticSearch. Logstash use update_ts and unix_ts_in_secs for capturing change. Geo-point is encoded as text.'


