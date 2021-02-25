""" Elastic Search foreign data wrapper """
# pylint: disable=too-many-instance-attributes, import-error, unexpected-keyword-arg, broad-except, line-too-long

import json
import logging

from elasticsearch import VERSION as ELASTICSEARCH_VERSION
from elasticsearch import Elasticsearch

from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log2pg


class SuricateFDW(ForeignDataWrapper):
    """ Elastic Search Foreign Data Wrapper """

    @property
    def rowid_column(self):
        """ Returns a column name which will act as a rowid column for
            delete/update operations.

            This can be either an existing column name, or a made-up one. This
            column name should be subsequently present in every returned
            resultset. """

        return self._rowid_column

    def __init__(self, options, columns):
        super(SuricateFDW, self).__init__(options, columns)

        self.index = options.pop("index", "")
        self.query_column = options.pop("query_column", None)
        self.response_column = options.pop("response_column", None)
        self.pg_id_column = options.pop("pg_id_column", None)
        self.size = int(options.pop("size", 10))
        self.explain = (
            options.pop("explain", "false").lower() == "true"
        )
        self._rowid_column = options.pop("rowid_column", "id")
        username = options.pop("username", None)
        password = options.pop("password", None)
        # self.score_column = options.pop("score_column", None)
        # self.default_sort = options.pop("default_sort", "")
        # self.sort_column = options.pop("sort_column", None)
        # self.scroll_size = int(options.pop("scroll_size", "1000"))
        # self.scroll_duration = options.pop("scroll_duration", "10m")




        self.path = "/{index}".format(index=self.index)


        if (username is None) != (password is None):
            raise ValueError("Must provide both username and password")
        if username is not None:
            auth = (username, password)
        else:
            auth = None

        host = options.pop("host", "localhost")
        port = int(options.pop("port", "9200"))
        timeout = int(options.pop("timeout", "10"))
        self.client = Elasticsearch(
            [{"host": host, "port": port}], http_auth=auth, timeout=timeout, **options
        )
        self.scroll_id = None


    def get_rel_size(self, quals, columns):
        """ Helps the planner by returning costs.
            Returns a tuple of the form (number of rows, average row width) """

        try:
            query = self._get_query(quals)
            # pg_id = self._get_pg_id(quals)
            # log2pg("log-info: pg_id:{}".format(pg_id), logging.INFO)
            # log2pg("log-info: query:{}".format(query), logging.INFO)
            # log2pg("log-info: quals:{}".format(quals), logging.INFO)
            q_dict = json.loads(query.encode('utf-8'))
            response = self.client.count(body=q_dict,  index=self.index)
            return (response["count"], len(columns) * 100)
        except Exception as exception:
            log2pg(
                "COUNT Rel_size for {path} failed: {exception}".format(
                    path=self.path, exception=exception
                ),
                logging.ERROR,
            )
            return (0, 0)

    def execute(self, quals, columns):
        """ Execute the query """

        try:
            query = self._get_query(quals)
            q_dict = json.loads(query.encode('utf-8'))
            pg_id = self._get_pg_id(quals)
            response = self.client.search(
                body=q_dict,
                index=self.index,
                size=self.size,
                explain=self.explain
            )
            while True:
                for result in response["hits"]["hits"]:
                    yield self._format_out(result, pg_id=pg_id, query=query)

                return
        except Exception as exception:
            log2pg(
                "SEARCH for {path} failed: {exception}".format(
                    path=self.path, exception=exception
                ),
                logging.ERROR,
            )
            return

    def _get_pg_id(self, quals):
        if not self.query_column:
            return None

        return next(
            (
                qualifier.value
                for qualifier in quals
                if qualifier.field_name == self.pg_id_column
            ),
            None,
        )

    def end_scan(self):
        """ Hook called at the end of a foreign scan. """
        if self.scroll_id:
            self.client.clear_scroll(scroll_id=self.scroll_id)
            self.scroll_id = None

    def _format_out(self, response, pg_id, query):
        result_dict = {
            self.response_column:json.dumps(response),
            self.pg_id_column:pg_id,
            self.query_column:query
        }
        return result_dict

    def _get_query(self, quals):
        return next(
            (
                qualifier.value
                for qualifier in quals
                if qualifier.field_name == self.query_column
            ),
            None,
        )
    def _convert_response_row(self, row_data, columns, query, sort):
        return_dict = {
            column: self._convert_response_column(column, row_data)
            for column in columns
            if column in row_data["_source"]
            or column == self.rowid_column
            or column == self.score_column
        }
        if query:
            return_dict[self.query_column] = query
        return_dict[self.sort_column] = sort
        return return_dict

    def _read_by_id(self, row_id):
        try:
            results = self.client.search(
                body={"query": {"ids": {"values": [row_id]}}}, index=self.index
            )["hits"]["hits"]
            if results:
                return self._convert_response_row(results[0], self.columns, None, None)
            log2pg(
                "SEARCH for {path} row_id {row_id} returned nothing".format(
                    path=self.path, row_id=row_id
                ),
                logging.WARNING,
            )
            return {self.rowid_column: row_id}
        except Exception as exception:
            log2pg(
                "SEARCH for {path} row_id {row_id} failed: {exception}".format(
                    path=self.path, row_id=row_id, exception=exception
                ),
                logging.ERROR,
            )
            return {}
