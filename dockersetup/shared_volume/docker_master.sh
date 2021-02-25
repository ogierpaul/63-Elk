docker exec postgres /bin/bash -c "cd /multicorn_fdw_for_es/suricate_fdw; pip install --upgrade ."
# docker exec postgres /bin/bash -c "cd /multicorn_fdw_for_es/pg_es_fdw; pip install --upgrade ."
cd /Users/pogier/Documents/63-Elk/dockersetup
docker stop postgres
# docker stop elasticsearch
docker-compose up -d
sleep 3

docker exec -it postgres psql -U myuser -d mydb -f /shared_volume/3_create_suricate_fdw.sql
docker exec -it postgres psql -U myuser -d mydb -f /shared_volume/4_run_suricate_fdw.sql