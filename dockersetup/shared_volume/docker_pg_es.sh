# docker exec postgres /bin/bash -c "cd /multicorn_fdw_for_es/suricate_fdw; pip install --upgrade ."
docker exec postgres /bin/bash -c "cd /multicorn_fdw_for_es/pg_es_fdw; pip install --upgrade ."
cd /Users/pogier/Documents/63-Elk/dockersetup
docker stop postgres
docker-compose up -d
sleep 3
docker exec -it postgres psql -U myuser -d mydb -f /shared_volume/2a_test_pg_es_lucene_cross_lateral_join.sql
docker exec -it postgres psql -U myuser -d mydb -f /shared_volume/2b_test_pg_es_dsl_cross_lateral_join.sql