""" Install file for Postgres Elasticsearch Foreign Data Wrapper """
from setuptools import setup


if __name__ == "__main__":
    setup(
        name="pg_es_suricate",
        packages=["pg_es_suricate"],
        keywords=["postgres", "postgresql", "elastic", "elastic search", "fdw"],
        install_requires=["elasticsearch"],
        version='0.2'
    )