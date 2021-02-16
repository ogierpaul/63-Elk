-- This script gives you a quick visualization of how tfidf score can vary
-- Try selecting the lowest tfidf firstnames, and the highest
-- which first names is more common in this dataset (Dear John)?


WITH high_scores AS (
SELECT
    puid,
    firstnames_tfidf
from esqueries.g_pivot
WHERE firstnames_tfidf IS NOT NULL
ORDER by firstnames_tfidf DESC
LIMIT 5),
low_scores AS
(SELECT
    puid,
    firstnames_tfidf
from esqueries.g_pivot
    WHERE firstnames_tfidf IS NOT NULL AND firstnames_tfidf > 1
    ORDER BY firstnames_tfidf ASC
LIMIT 5),
myscores AS (
SELECT * FROM high_scores
UNION
SELECT * FROM low_scores
ORDER BY firstnames_tfidf DESC)

SELECT
    *
FROM (
    SELECT
           split_part(puid, '-', 1) ::INTEGER as source_row_id,
        split_part(puid, '-', 2) ::INTEGER as target_row_id,
          firstnames_tfidf
    FROM myscores
    ) g
LEFT JOIN (
    SELECT row_id as source_row_id,
    firstnames AS source_firstnames
    -- surname AS source_surname,
    -- route as source_route,
    -- rocality as source_locality
    FROM public.uknamescomplete
    ) s USING (source_row_id)
LEFT JOIN (
    SELECT row_id as target_row_id,
    firstnames AS target_firstnames
    -- surname AS target_surname,
    -- route as target_route,
    -- rocality as target_locality
    FROM public.uknamescomplete
    ) t USING(target_row_id);


SELECT AVG(firstnames_tfidf), MAX(firstnames_tfidf)
FROM g_pivot;


SELECT AVG(surname_tfidf),  MAX(surname_tfidf)
FROM g_pivot;


