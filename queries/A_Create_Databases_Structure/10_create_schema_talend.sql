CREATE OR REPLACE VIEW talend.v_puid_blocking AS
WITH pairs as  ( SELECT
        puid,
        split_part(puid, '-', 1) ::INTEGER as row_id_source,
        split_part(puid, '-', 2) ::INTEGER as row_id_target
    FROM elasticresults.tfidf),
source as (SELECT row_id_source as row_id, puid FROM pairs),
target as (SELECT row_id_target as row_id, puid FROM pairs),
union_pairs as (
     SELECT * FROM source
UNION ALL
SELECT * FROM target)
SELECT * FROM union_pairs
LEFT JOIN target.attributes USING (row_id);
COMMENT ON VIEW talend.v_puid_blocking IS 'Attributes data (firstnames, surnames... ) with the puid as blcking key for input to tMatchGroup';

