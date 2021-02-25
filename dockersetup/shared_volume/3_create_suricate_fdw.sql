DROP SERVER IF EXISTS multicorn_suricate CASCADE ;

CREATE SERVER IF NOT EXISTS  multicorn_suricate FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'suricate_fdw.SuricateFDW'
);


DROP FOREIGN TABLE IF EXISTS fdwes.suricate_target;
CREATE FOREIGN TABLE IF NOT EXISTS fdwes.suricate_target
    (
        row_id INT,
        title TEXT,
        firstnames TEXT,
        surname TEXT,
        name TEXT,
        streetnumber TEXT,
        route TEXT,
        locality TEXT,
        postalcodelong TEXT,
        country TEXT,
        formattedaddress TEXT,
        email TEXT,
        ni_number TEXT,
        phone TEXT,
        query TEXT,
        score NUMERIC
    )
SERVER multicorn_suricate
OPTIONS
    (
        host 'elasticsearch',
        port '9200',
        index 'pgtarget',
        rowid_column 'row_id',
        query_column 'query',
        score_column 'score',
        default_sort 'score:desc',
        sort_column 'score:desc',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme',
        query_dsl 'true',
        scroll_duration '0nanos',
        scroll_size '10',
        size '10'
    )
;


CREATE TABLE fdwes.suricate_results (
    pg_id INTEGER,
    es_id INTEGER,
    score NUMERIC,
    PRIMARY KEY (pg_id, es_id)
);

CREATE OR REPLACE VIEW fdwes.unmatched_queries AS
SELECT * FROM fdwes.dsl_query
WHERE NOT EXISTS(SELECT pg_id FROM fdwes.suricate_results WHERE fdwes.suricate_results.pg_id = fdwes.dsl_query.pg_id);

SELECT * FROM fdwes.unmatched_queries;




