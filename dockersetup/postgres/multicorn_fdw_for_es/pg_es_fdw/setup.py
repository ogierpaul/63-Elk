""" Install file for Postgres Elasticsearch Foreign Data Wrapper """
# pylint: disable=line-too-long
from os.path import dirname, join

from setuptools import setup


if __name__ == "__main__":
    setup(
        name="pg_es_fdw",
        packages=["pg_es_fdw"],
        version="0.10.2",
        description="Connect PostgreSQL and Elastic Search with this Foreign Data Wrapper",
        long_description_content_type="text/markdown",
        author="Matthew Franglen",
        author_email="matthew@franglen.org",
        url="https://github.com/matthewfranglen/postgres-elasticsearch-fdw",
        download_url="https://github.com/matthewfranglen/postgres-elasticsearch-fdw/archive/0.10.2.zip",
        keywords=["postgres", "postgresql", "elastic", "elastic search", "fdw"],
        install_requires=["elasticsearch==7.10.1"],
    )

