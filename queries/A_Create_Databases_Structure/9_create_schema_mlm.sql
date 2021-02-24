CREATE OR REPLACE VIEW mlm.tfidf
AS
    SELECT
        puid,
           firstnames_tfidf,
           surname_tfidf,
            locality_tfidf,
            postalcodelong_tfidf,
          route_tfidf,
           title_tfidf
    FROM elasticresults.tfidf;
COMMENT ON VIEW mlm.tfidf IS 'View on elasticresults.tfidf table';

CREATE MATERIALIZED VIEW IF NOT EXISTS mlm.scores
AS
    SELECT * FROM mlm.tfidf
    FULL OUTER JOIN sbs.levenshtein USING(puid)
    FULL OUTER JOIN sbs.trg_similarity USING(puid)
    FULL OUTER JOIN sbs.geo_distance USING (puid)
    FULL OUTER JOIN sbs.id_exact USING (puid);
COMMENT ON MATERIALIZED VIEW mlm.scores IS 'View on all scores from sbs schema';

CREATE TABLE IF NOT EXISTS mlm.staging_yproba (
    puid varchar PRIMARY KEY ,
    y_proba FLOAT
);
COMMENT ON TABLE mlm.staging_yproba IS 'Staging layer for y_proba vector.';

CREATE TABLE IF NOT EXISTS mlm.yproba (
    puid VARCHAR primary key,
    y_proba FLOAT,
    create_ts timestamp default current_timestamp,
	update_ts timestamp default current_timestamp
);
COMMENT ON TABLE mlm.yproba IS 'Table containing the  probability of a match between two pairs (y_proba).';


CREATE MATERIALIZED VIEW IF NOT EXISTS mlm.ydecision AS
    SELECT
puid,
CASE
    WHEN
        y_proba < 0.10 THEN 'No-Match'
    WHEN
        y_proba >= 0.10 AND y_proba < 0.85 THEN 'TDS'
    ELSE
        'Match'
    END AS y_decision
FROM mlm.yproba;
COMMENT ON MATERIALIZED VIEW mlm.ydecision IS 'View on the decision (TDS, No-Match, Match) taken for each possible pair based on thresholds.';


CREATE OR REPLACE VIEW mlm.confusion_matrix AS
    SELECT
        y_decision,
        AVG(y_true) as pct_true,
        SUM(y_true) as nb_true,
        COUNT(*) as n_pairs,
        AVG(y_proba) as avg_y_proba
    FROM
        mlm.ydecision
    INNER JOIN mlm.ytrue USING (puid)
    INNER JOIN mlm.yproba USING (puid)
    GROUP BY y_decision
ORDER BY pct_true DESC;
COMMENT ON VIEW mlm.confusion_matrix IS 'View on a confusion matrix with the three outcome possible (TDS, No-Match, Match), and some relevant statistics';



CREATE OR REPLACE VIEW mlm.tds_sbs AS
SELECT puid, y_decision, y_proba, (y_proba>=0.5)::BOOL as y_pred,
       firstnames_tfidf, firstnames_levenshtein, firstnames_source, firstnames_target,
       surname_tfidf, surname_levenshtein, surname_source, surname_target,
       streetnumber_source, streetnumbe_target,
       route_tfidf, route_source, route_target,
       postalcodelong_tfidf, postalcodelong_source, postalcodelong_target,
       locality_tfidf, locality_source, locality_target
FROM mlm.ydecision
INNER JOIN exploration.sbs USING (puid)
INNER JOIN mlm.yproba USING (puid)
INNER JOIN sbs.levenshtein USING(puid)
WHERE y_decision = 'TDS';
COMMENT ON VIEW mlm.tds_sbs IS 'view on pairs sent to tds with additional context information';




