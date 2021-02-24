CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.demo(
    row_id INTEGER PRIMARY KEY,
    name VARCHAR,
    city VARCHAR
);
INSERT INTO demo.demo (row_id, name, city) VALUES (1, 'Paul', 'New York');
INSERT INTO demo.demo (row_id, name, city) VALUES (2, 'Robert', 'Milan');
INSERT INTO demo.demo (row_id, name, city) VALUES (3, 'Alice', 'Shanghai');
INSERT INTO demo.demo (row_id, name, city) VALUES (4, 'Alizah', 'Schanghai');
INSERT INTO demo.demo (row_id, name, city) VALUES (5, 'Bob', 'Mailand');

CREATE OR REPLACE VIEW demo.body
AS
SELECT
       row_id AS row_id_source,
       name as name_source,
       city as city_source,
       json_build_object(
           'query', json_build_object(
               'bool', jsonb_build_object(
                   'should',    json_build_array(
                      json_build_object('match', json_build_object('name', json_build_object('query', "name", 'fuzziness', 2.0))),
                      json_build_object('match', json_build_object('city', json_build_object('query', "city", 'fuzziness', 2.0)))
                  )
               )
           ),
           'explain', true,
           'size', 20
       ) as body
FROM
    demo.demo;