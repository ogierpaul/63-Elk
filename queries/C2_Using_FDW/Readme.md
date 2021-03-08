# Using Foreign Data Wrapper
## Purpose
- Connect to ElasticSearch from Postgres (PG) Using a Foreign Data Wrapper (FDW)
- Uses the work done by Matthew Franglen:
    - https://github.com/matthewfranglen/postgres-elasticsearch-fdw
    - Small modifications to be able to pass an _explain parameter and get the details of the response, as well having no scroll and fixed-size queries
- Experimental / PoC purpose
- I have not re-tested the code

## Results
- It is possible to skip Python Scripts to use Foreign Data Wrapper to query ElasticSearch
- The FDW uses Python in the background
- There is not a great performance improvment (60 rows per sec with FDW vs 50 rows per sec with script)
- Maybe more convenient for scheduling / Triggering purposes since all data exchange is triggered by SQL
