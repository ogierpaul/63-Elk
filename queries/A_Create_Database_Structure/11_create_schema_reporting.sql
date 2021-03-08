CREATE OR REPLACE VIEW reporting.ytrue AS
SELECT puid, y_true, CASE WHEN y_true =0 THEN 'NoMatch' ELSE 'MATCH' END as y_label FROM exploration.ytrue;

CREATE OR REPLACE VIEW reporting.yproba AS
SELECT puid, y_proba FROM mlm.yproba
INNER JOIN (SELECT puid FROM reporting.ytrue) b USING (puid);

CREATE OR REPLACE VIEW reporting.ydecision AS
SELECT puid, y_decision, confidence FROM mlm.ydecision
INNER JOIN (SELECT puid FROM reporting.ytrue) b USING (puid);

CREATE OR REPLACE VIEW reporting.simplesbs AS
SELECT * FROM exploration.simplesbs
INNER JOIN (SELECT puid FROM reporting.ytrue) b USING (puid);

CREATE OR REPLACE VIEW reporting.scores
AS
SELECT
    puid,
    COALESCE(firstnames_tfidf, 0) as firstnames_tfidf,
    COALESCE(firstnames_levenshtein, 0) as firstnamed_levenshtein,
    COALESCE(firstnames_trg_metaphone, 0) as firstnamed_trg_metaphone,
    COALESCE(surname_levenshtein, 0) as surname_levenshtein,
    COALESCE(postalcodelong_tfidf, 0) as postalcodelong_tfidf,
    COALESCE(postalcodelong_levenshtein, 0) as postalcodelong_levenshtein,
    COALESCE(geo_score_ths, 0) as geo_score_ths,
    COALESCE(locality_tfidf, 0) as locality_tfidf,
    COALESCE(locality_levenshtein, 0) as locality_levenshtein
FROM mlm.scores
INNER JOIN (SELECT puid FROM reporting.ytrue) b USING (puid);
