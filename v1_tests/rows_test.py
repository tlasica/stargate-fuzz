import logging
import datetime
import collections

from cassandra.cqlengine.columns import Date
from cassandra.cqltypes import UUID
from cassandra.util import OrderedMapSerializedKey, SortedSet

from cql_tools import cql_keyspaces, cql_tables, cql_table_metadata, cql_table_pk_columns
from fixtures import *
from v1_tests.test_config import TestConfig


class TestRestV1Rows:

    LOG = logging.getLogger(__name__)

    def test_rows_by_pk(self, cql_session, rest_v1):
        for ks in cql_keyspaces(cql_session):
            for table in cql_tables(cql_session, ks):
                if TestConfig.skip_table(ks, table):
                    self.LOG.info("skipping {ks}.{t} as requested by test config".format(ks=ks, t=table))
                    continue
                self.LOG.info("comparing rows for {ks}.{t}".format(ks=ks, t=table))
                self._compare_rows_by_pk(cql_session, rest_v1, ks, table)

    def _compare_rows_by_pk(self, cql_session, rest_v1, ks, table):
        # TODO: select only key columns
        cql_data = cql_session.execute('SELECT * FROM "{ks}"."{t}" LIMIT 100'.format(ks=ks, t=table))
        partk_columns, clustk_columns = cql_table_pk_columns(cql_session, ks, table)
        self.LOG.debug("PK columns for {ks}.{t}: {pk} / {ck}".format(ks=ks, t=table, pk=partk_columns, ck=clustk_columns))

        for n in range(0, len(clustk_columns) + 1):
            key_columns = partk_columns + clustk_columns[0:n]
            for row in cql_data:
                self.compare_rest_by_pk_vs_cql_query(cql_session, rest_v1, ks, table, row, key_columns)

    def compare_rest_by_pk_vs_cql_query(self, cql_session, rest_v1, ks, table, row, key_columns):
        """
        Compare CQL query using PK and CK columns vs REST V1 query by key
        """
        pk_values = [row[c] for c in key_columns]
        rest_res = rest_v1.get_table_rows_by_pk(ks, table, pk_values)
        assert rest_res.ok
        rest_rows_raw = rest_res.value['rows']
        rest_rows = [self._normalize_row(x) for x in rest_rows_raw]

        cql_query = 'SELECT * FROM "{ks}"."{t}" WHERE '.format(ks=ks, t=table)
        cql_query += ' AND '.join(["{}=%s".format(c) for c in key_columns])
        self.LOG.info(cql_query)
        cql_rows_raw = cql_session.execute(cql_query, pk_values).all()
        cql_rows = [self._normalize_row(x) for x in cql_rows_raw]

        def explanation():
            return "Results from CQL Query: {q}\nand REST {url}\ndiffers.\n\n" \
            "CQL Results: {cql}\nREST Results: {rest}\nREST Raw: {rest_raw}".format(
                q=cql_query,
                url=rest_res.url,
                cql=cql_rows,
                rest=rest_rows,
                rest_raw=rest_rows_raw
            )

        assert len(rest_rows) == len(cql_rows), explanation()
        assert rest_rows == cql_rows, explanation()

        # TODO: we need more smarter way to compare, possibly row by row

        # TODO: maybe collect issues and then dump instead of fail on 1st

    def _normalize_value(self, value):
        if isinstance(value, UUID):
            return str(value)
        elif isinstance(value, datetime.datetime):
            return str(value)
        elif isinstance(value, OrderedMapSerializedKey):
            return str(value)
        elif isinstance(value, dict):
            return str(collections.OrderedDict(value).items())
        elif isinstance(value, collections.OrderedDict):
            return str(value.items())
        elif isinstance(value, SortedSet):
            return str(list(value))
        elif isinstance(value, Date):
            return str(value)
        else:
            return value

    def _normalize_row(self, row):
        return dict(map(lambda x: (x[0], self._normalize_value(x[1])), row.items()))
