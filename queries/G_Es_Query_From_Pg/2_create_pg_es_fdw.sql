DROP SERVER IF EXISTS multicorn_es CASCADE ;

CREATE SERVER IF NOT EXISTS  multicorn_es FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'pg_es_fdw.ElasticsearchFDW'
);

DROP FOREIGN TABLE IF EXISTS fdwes.target_es;
CREATE FOREIGN TABLE IF NOT EXISTS fdwes.target_es
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
SERVER multicorn_es
OPTIONS
    (
        host 'elasticsearch',
        port '9200',
        index 'pgtarget',
        rowid_column 'row_id',
        query_column 'query',
        score_column 'score',
--         default_sort 'score:desc',
--         sort_column 'score',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme'
    )
;

SELECT * FROM fdwes.target_es
WHERE query = 'surname:Pemble'
LIMIT 5;
