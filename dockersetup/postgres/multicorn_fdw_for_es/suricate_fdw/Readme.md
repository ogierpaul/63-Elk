# Elastic Search JSON search Wrapper
## The use of this FDW is restricted to a very specific  syntax
- foreign table with fixed columns
- ALWAYS include a WHERE clause with a pg_id_column and query_column
- The query is in JSON, deserialized to text (because the FDW does not understand a query where the predicate (WHERE) is a json)

## IN SHORT

```sql
CREATE EXTENSION IF NOT EXISTS multicorn;
DROP SERVER IF EXISTS multicorn_es CASCADE ;

CREATE SERVER IF NOT EXISTS  multicorn_es FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'suricate_fdw.SuricateFDW'
);


DROP FOREIGN TABLE IF EXISTS suricate;
CREATE FOREIGN TABLE IF NOT EXISTS suricate(
        pg_id INTEGER,
        query TEXT,
        response TEXT
    )
SERVER multicorn_es
OPTIONS
    (
        host 'elasticsearch',
        port '9200',
        index 'article-index',
        query_column 'query',
        pg_id_column 'pg_id',
        response_column 'response',
        size '10',
        explain 'true',
        refresh 'false',
        complete_returning 'false',
        timeout '20',
        username 'elastic',
        password 'changeme'
    )
;

SELECT pg_id, query, response
FROM suricate
WHERE query='{"query" : {"match_all" : {}}}' and pg_id =1
```

- always use those three columns: `query`, `pg_id`, and `response`
- query is written in JSON and converted in TEXT
- always use the `quals` `query='{...}` and `pg_id=..` - it cannot work without it
- the response from ElasticSearch is in JSON, it must be converted

