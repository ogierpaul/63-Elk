""" Install file for Postgres Elasticsearch Foreign Data Wrapper """
# pylint: disable=line-too-long
from os.path import dirname, join

from setuptools import setup


if __name__ == "__main__":
    setup(
        name="suricate_fdw",
        packages=["suricate_fdw"],
        version="0.1",
        description="Launch search queries with JSON from ES",
        author="Paul Ogier",
        keywords=["postgres", "postgresql", "elastic", "elastic search", "fdw"],
        install_requires=["elasticsearch==7.10.1"],
    )

