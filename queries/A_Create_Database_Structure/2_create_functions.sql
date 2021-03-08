CREATE EXTENSION IF NOT EXISTS intarray; -- for pivot table (crosstable)
CREATE EXTENSION IF NOT EXISTS tablefunc; --for sort
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- for similarity
CREATE EXTENSION IF NOT EXISTS plpgsql; -- for language
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch; -- for levenshtein


CREATE OR REPLACE FUNCTION elasticqueries.json_fuzzy_match(n VARCHAR, v VARCHAR, f INTEGER DEFAULT 1)
-- This function returns the match term of a query dsl as JSON
-- Will return null if the input value is NULL
-- Returns JSON with fuzzy match Query DSL. n Is field name, v field value, f fuzziness
-- Optional : Can add additional parameters like fuzziness, analyzer, synonyms etc..
-- https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html
-- https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-graph-tokenfilter.html
RETURNS json
STRICT IMMUTABLE PARALLEL SAFE
LANGUAGE plpgsql
AS
$$
DECLARE myjs json;
BEGIN
SELECT
 CASE
    WHEN v IS NULL THEN NULL
    ELSE json_build_object(
        'match', json_build_object(
            n, json_build_object(
                'query', v,
                'fuzziness', f
            )
        )
    )
END
INTO myjs;
RETURN myjs;
END;
$$;
COMMENT ON FUNCTION elasticqueries.json_fuzzy_match(n VARCHAR, v VARCHAR, f INTEGER) IS 'Returns JSON with fuzzy match Query DSL. n Is field name, v field value, f fuzziness';

CREATE OR REPLACE FUNCTION sbs.exact_match(a int, b int)
-- Returns null if null, and then 1 if an exact match, 0 otherwise
-- Version for INT
RETURNS FLOAT LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS
$$
BEGIN
    RETURN CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE (a=b)::INT::FLOAT END ;
END;
$$;
COMMENT ON FUNCTION sbs.exact_match(a varchar, b varchar) IS 'Returns 1 if a=b, 0 if a<>b , null if null. INT version';


CREATE OR REPLACE FUNCTION sbs.exact_match(a varchar, b varchar)
-- Returns null if null, and then 1 if an exact match, 0 otherwise
-- Version for VARCHAR
RETURNS FLOAT LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS
$$
BEGIN
    RETURN CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE (a=b)::INT::FLOAT END ;
END;
$$;
COMMENT ON FUNCTION sbs.exact_match(a varchar, b varchar) IS 'Returns 1 if a=b, 0 if a<>b , null if null. VARCHAR version';

CREATE OR REPLACE FUNCTION sbs.geo_score_ths(d float, t float DEFAULT 2000)
-- Returns 0 if the distance (d, float), is greater than the threshold t
-- Otherwise a linear scoring function of the distance (1 if distance is 0; decreases linearly up to the threshold for value 0)
RETURNS FLOAT LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS
$$
BEGIN
    RETURN     CASE
            WHEN d IS NULL THEN 0
            WHEN d >= t THEN 0
            ELSE (t-d)/t
        END;
END;
$$;
COMMENT ON FUNCTION sbs.geo_score_ths(d float, t float) IS 'Similarity score based on distance d between two points. 1 if the distance is 0; 0 if the distance is equal or greater than the threshold t (in meters)';



CREATE OR REPLACE FUNCTION sbs.leven_similarity(a varchar, b varchar)
-- Sort the tokens of the strings a&, calculate a levensthein score
RETURNS FLOAT LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS
$$
BEGIN
    RETURN CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE ((sort(ARRAY [length(a), length(b)]))[2]::FLOAT- levenshtein(a, b)::FLOAT)/((sort(ARRAY [length(a), length(b)]))[2]::FLOAT) END ;
END;
$$;
COMMENT ON FUNCTION sbs.leven_similarity(a varchar, b varchar) IS 'Levenshtein similarity of strings a & b, where the tokens are sorted alphabetically';

