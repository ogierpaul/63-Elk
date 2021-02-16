CREATE SCHEMA IF NOT EXISTS target;

create table IF NOT EXISTS target.staging (
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

CREATE INDEX ix_target_geopoint ON target.attributes USING GIST ( geopoint );

