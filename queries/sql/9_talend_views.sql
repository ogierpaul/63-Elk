
CREATE VIEW mlm.talend_source AS
SELECT *
    FROM
         (SELECT split_part(puid, '-', 1)::INTEGER as source_row_id, puid
         FROM esqueries.f_grouppuid) a
LEFT JOIN (
SELECT
    row_id as source_row_id,
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
    ) s USING (source_row_id);

CREATE VIEW mlm.talend_target AS
SELECT *
    FROM
         (SELECT split_part(puid, '-', 2)::INTEGER as target_row_id, puid
         FROM esqueries.f_grouppuid) a
LEFT JOIN (
SELECT
    row_id as target_row_id,
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
    ) s USING (target_row_id);


CREATE VIEW mlm.talend_blocking
AS
SELECT *
FROM
((SELECT
     split_part(puid, '-', 1)::INTEGER as row_id, puid, 'source' as origin
FROM esqueries.f_grouppuid LIMIT 200)
UNION ALL
(SELECT
     split_part(puid, '-', 2)::INTEGER as row_id, puid, 'target' as origin
FROM esqueries.f_grouppuid LIMIT 200) ) b
LEFT JOIN (SELECT row_id, firstnames, surname, dtreetnumber FROM uknamescomplete) u USING (row_id);

CREATE VIEW  mlm.suricate_sbs AS
SELECT
    *
FROM (
    SELECT
        puid,
        split_part(puid, '-', 1) ::INTEGER as ix_source,
        split_part(puid, '-', 2) ::INTEGER as ix_target,
        firstnames_tfidf
    FROM esqueries.g_pivot
    ) g
LEFT JOIN (
    SELECT row_id as ix_source,
    firstnames AS firstnames_source,
    surname AS surname_source,
    dtreetnumber as streetnumber_source,
    route as route_source,
    postalcodelong as postalcodelong_source,
    rocality as locality_source,
    ni_number as ni_number_source,
    email as email_source,
    phone as phone_source
    FROM public.uknamescomplete
    ) s USING (ix_source)
LEFT JOIN (
    SELECT row_id as ix_target,
    firstnames AS firstnames_target,
    surname AS surname_target,
    dtreetnumber as streetnumber_target,
    route as route_target,
    postalcodelong as postalcodelong_target,
    rocality as locality_target,
    ni_number as ni_number_target,
    email as email_target,
    phone as phone_target
    FROM public.uknamescomplete
    ) t USING(ix_target);


