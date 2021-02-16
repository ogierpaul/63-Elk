

CREATE OR REPLACE FUNCTION esqueries.json_fuzzy_match(n VARCHAR, v VARCHAR, f INTEGER DEFAULT 1)
RETURNS json
STRICT
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


DROP VIEW esqueries.body;

CREATE VIEW esqueries.body
AS
SELECT
       row_id as source_row_id,
       json_build_object(
           'query', json_build_object(
               'bool', jsonb_build_object(
                   'should', json_agg(jsarel) FILTER ( WHERE json_typeof(t.jsarel) <> 'null' )
               )
           ),
           'explain', true,
           'size', 10
       ) as payload
FROM
     (SELECT
             row_id, json_array_elements( jsar ) as jsarel
     FROM (SELECT row_id,
                  json_build_array(
                          esqueries.json_fuzzy_match('surname', "surname", 1),
                          esqueries.json_fuzzy_match('firstnames', "firstnames", 1),
                          esqueries.json_fuzzy_match('route', "route", 1),
                          esqueries.json_fuzzy_match('rocality', "rocality", 1)
                      ) as jsar
            FROM uknamescomplete
          ) b
    ) t
GROUP BY row_id;

