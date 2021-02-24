FROM postgis/postgis:11-2.5
RUN apt-get update &&\
    apt-get install -y libpq-dev && \
    apt-get install -y python3 python-pip &&\
     apt-get install -y postgresql-11-python-multicorn
RUN mkdir /requirements
COPY requirements.txt /requirements/requirements.txt
RUN pip install --requirement /requirements/requirements.txt
COPY startup.sql /docker-entrypoint-initdb.d/
WORKDIR /shared_volume
# RUN pip install .