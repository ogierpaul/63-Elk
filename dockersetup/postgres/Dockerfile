FROM postgis/postgis:11-2.5
RUN apt-get update &&\
    apt-get install -y libpq-dev && \
    apt-get install -y python3 python-pip &&\
     apt-get install -y postgresql-11-python-multicorn
COPY
#WORKDIR /multicorn_fdw_for_es
#RUN pip install --requirement requirements.txt
#WORKDIR /multicorn_fdw_for_es/pg_es_fdw
#RUN pip install --upgrade .
#WORKDIR /multicorn_fdw_for_es/suricate_fdw
#RUN pip install --upgrade .