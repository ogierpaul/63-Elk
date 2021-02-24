CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.demo(
    row_id INTEGER PRIMARY KEY,
    name VARCHAR,
    city VARCHAR
);
TRUNCATE TABLE demo.demo;
INSERT INTO demo.demo (row_id, name, city) VALUES (1, 'Paul', 'New York');
INSERT INTO demo.demo (row_id, name, city) VALUES (2, 'Robert', 'Milan');
INSERT INTO demo.demo (row_id, name, city) VALUES (3, 'Alice', 'Shanghai');
INSERT INTO demo.demo (row_id, name, city) VALUES (4, 'Alizah', 'Schanghai');
INSERT INTO demo.demo (row_id, name, city) VALUES (5, 'Bob', 'Mailand');