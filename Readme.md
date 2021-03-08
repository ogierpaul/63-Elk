# Deduplication project using Postgres and ElasticSearch
## Purpose: Improve classic Matching Techniques
- Most matching techniques involve:
    - String Distance comparison (Levenshtein, metaphone...)
    - Cartesian Join of records: Comparing each record of A with each record of B
- This method uses ElasticSearch to Index the data before-hand
    - Finding potential duplicates is faster because the data has been pre-indexe
- The scores are more precise
    - Using Tf-Idf
    - Using geographical distance
- The sampling technique to create the labelled pairs is more robust

## Repository Structure
- dockersetup: how to launch the docker-compose.yml file
- queries: The queries to pass to Postgres to launch the deduplication
