CREATE EXTENSION IF NOT EXISTS intarray;

REFRESH MATERIALIZED VIEW sbs.levenshtein;

REFRESH MATERIALIZED VIEW sbs.id_exact;

REFRESH MATERIALIZED VIEW sbs.trg_similarity;

REFRESH MATERIALIZED VIEW sbs.geo_distance;

REFRESH MATERIALIZED VIEW sbs.geo_score;

REFRESH MATERIALIZED VIEW mlm.scores;