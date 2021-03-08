# Find Potential Duplicates
## Purpose
- Query the reference data stored in elastic search for potential duplicates
- Populates the potential pairs including their Tf-Idf score from Elastic

## Steps
### Part 1: Copy the source data
- The source data contains the rows that you wish to compare against the target data for potential duplicates

### Part 2: Query Elastic Search
- The body of a Elastic Query DSL is generated for each source row (Schema ElasticQueries)
- A python routine reads this body and pass it as a query to Elastic
- This routine returns the top 10 best scoring records, and the explanation for the scores with the score details
- The results are saved as a flat file containing JSON Lines

### Part 3: Loading the results
- The results are copied into Postgres
- The JSON is parsed to extract the score details for each potential pairs

## Improvments / Workaround
### Improve speed of passing requests to ES
- The program is limited by the speed of requests to ES
- The requests are not send in a parallel / distributed way
- Thus the speed is fixed: around 60 rows/sec

### Format the body in other language than SQL
- SQL formatting of JSON field is quite heavy, and we have to do several acrobatic manipulations to remove nulls...
- It is doable to do it in other languages, for more flexibility