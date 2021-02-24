CREATE OR REPLACE VIEW elasticqueries.a_should_array AS
    -- this query returns a json array with all the matche clauses of a multi-match query
    -- this is the place where you can edit the different facets of the search
SELECT row_id,
  json_build_array(
            elasticqueries.json_fuzzy_match('title', "title", 1),
          elasticqueries.json_fuzzy_match('surname', "surname", 1),
          elasticqueries.json_fuzzy_match('firstnames', "firstnames", 1),
          elasticqueries.json_fuzzy_match('route', "route", 1),
          elasticqueries.json_fuzzy_match('locality', "locality", 1),
          elasticqueries.json_fuzzy_match('postalcodelong', "postalcodelong", 0)
      ) as jsar
FROM source.unmatched_rows;
COMMENT ON VIEW elasticqueries.a_should_array IS 'returns a json array with all the matche clauses of a multi-match query. this is the place where you can edit the different facets of the search.';




CREATE OR REPLACE VIEW elasticqueries.b_as_elements
    -- array manipulation to explode the array of the query and expose the nulls for filtering
AS
 SELECT
         row_id, json_array_elements( jsar ) as jsarel
 FROM elasticqueries.a_should_array;
COMMENT ON VIEW elasticqueries.b_as_elements IS 'array manipulation to explode the array of the query and expose the nulls for filtering';


CREATE OR REPLACE VIEW elasticqueries.c_array_wo_nulls
    -- using the exploded array above, re-aggregate it filtering out the nulls
AS
SELECT row_id, json_agg(jsarel) FILTER ( WHERE json_typeof(t.jsarel) <> 'null' ) AS jsar
FROM elasticqueries.b_as_elements t
GROUP BY row_id;
COMMENT ON VIEW elasticqueries.c_array_wo_nulls IS 'using the exploded array b_as_elements, re-aggregate it, filtering out the nulls';



CREATE OR REPLACE VIEW elasticqueries.z_final_body AS
    -- nesting the array with the different terms in its proper place
SELECT
       row_id AS row_id_source,
       json_build_object(
           'query', json_build_object(
               'bool', jsonb_build_object(
                   'should',  jsar
               )
           ),
           'explain', true,
           'size', 20
       ) as body
FROM elasticqueries.c_array_wo_nulls;

COMMENT ON VIEW elasticqueries.c_array_wo_nulls IS 'Body of the query sent to ES. Json with the different terms in their proper place';


