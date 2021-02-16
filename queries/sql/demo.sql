CREATE SCHEMA demo;
CREATE TABLE demo.demo(
    row_id INTEGER PRIMARY KEY,
    firstname VARCHAR,
    route VARCHAR
);
INSERT INTO demo.demo (row_id, firstname, route) VALUES (1, 'Paul', 'road');
INSERT INTO demo.demo (row_id, firstname, route) VALUES (2, 'Fritz', NULL);

--


SELECT row_id as source_row_id,
       --json_typeof(t.jsarel),
       -- ,
       json_agg(jsarel) FILTER ( WHERE json_typeof(t.jsarel) <> 'null' ) as payload
FROM (SELECT row_id,
             json_array_elements(jsar) as jsarel
      FROM (SELECT row_id,
                   json_build_array(
                           esqueries.json_fuzzy_match('firstname', "firstname", 1),
                           esqueries.json_fuzzy_match('route', "route", 1)
                       ) as jsar
            FROM demo.demo
           ) b
     ) t
GROUP BY row_id;



SELECT
             row_id, json_array_elements( jsar ) as jsarel
     FROM (SELECT row_id,
                  json_build_array(
                          esqueries.json_fuzzy_match('firstname', "firstname", 1),
                          esqueries.json_fuzzy_match('route', "route", 1)
                      ) as jsar
            FROM demo.demo
          ) b

CREATE TABLE demo.cartesian AS (
SELECT
a.row_id as source_row_id,
b.target_row_id
FROM demo.demo a
CROSS JOIN (SELECT row_id as target_row_id from demo.demo) b);


CREATE EXTENSION intarray;

SELECT
       array_to_string(sort(ARRAY[source_row_id, target_row_id]),'-') ,
       source_row_id,
       target_row_id
FROM demo.cartesian
WHERE source_row_id <> cartesian.target_row_id;