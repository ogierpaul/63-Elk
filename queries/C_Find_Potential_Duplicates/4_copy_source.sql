COPY source.staging_source
FROM '/shared_volume/input/uknamescompleteclean.csv'
CSV HEADER DELIMITER '|' ENCODING 'UTF-8';


-- Populate reference data
-- Careful with the order of ST_Make Point which requires longitude and latitude
INSERT INTO source.attributes
SELECT row_id, surname, firstnames, title, day, month,
        year, "name", country_of_residence, nationality, formatteaddress, streetnumber,
        route, locality, administrationarealevel2, administrationarealevel1, postalcodelong,
        country, ST_SetSRID(ST_MakePoint(longitude, latitude),4326),
        email, ni_number, phone, phone_type, phone_country
FROM source.staging_source
ON CONFLICT (row_id) DO UPDATE
SET surname = excluded.surname,
    firstnames = excluded.firstnames,
    title = excluded.title,
    day = excluded.day,
    month = excluded.month,
    year = excluded.year,
    "name" = excluded."name",
    country_of_residence = excluded.country_of_residence,
    nationality = excluded.nationality,
    formatteaddress = excluded.formatteaddress,
    streetnumber = excluded.streetnumber,
    route = excluded.route,
    locality = excluded.locality,
    administrationarealevel2 = excluded.administrationarealevel2,
    administrationarealevel1 = excluded.administrationarealevel1,
    postalcodelong = excluded.postalcodelong,
    country = excluded.country,
    geopoint = excluded.geopoint,
    email = excluded.email,
    ni_number = excluded.ni_number,
    phone = excluded.phone,
    phone_type = excluded.phone_type,
    phone_country = excluded.phone_country,
    update_ts = current_timestamp
;
