CREATE SCHEMA mlm;

DROP TABLE mlm.simplesbs;
CREATE TABLE mlm.simplesbs AS
SELECT
    *
FROM (
    SELECT
        puid,
        split_part(puid, '-', 1) ::INTEGER as source_row_id,
        split_part(puid, '-', 2) ::INTEGER as target_row_id,
        firstnames_tfidf
    FROM esqueries.g_pivot
    ) g
LEFT JOIN (
    SELECT row_id as source_row_id,
    firstnames AS source_firstnames,
    surname AS source_surname,
    dtreetnumber as source_streetnumber,
    route as source_route,
    postalcodelong as source_postalcodelong,
    rocality as source_locality,
    ni_number as source_ni_number,
    email as source_email,
    phone as source_phone
    FROM public.uknamescomplete
    ) s USING (source_row_id)
LEFT JOIN (
    SELECT row_id as target_row_id,
    firstnames AS target_firstnames,
    surname AS target_surname,
    dtreetnumber as target_streetnumber,
    route as target_route,
    postalcodelong as target_postalcodelong,
    rocality as target_locality,
    ni_number as target_ni_number,
    email as target_email,
    phone as target_phone
    FROM public.uknamescomplete
    ) t USING(target_row_id);