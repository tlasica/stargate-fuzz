import logging
import datetime
import collections

from cassandra.cqltypes import UUID
from cassandra.util import OrderedMapSerializedKey, SortedSet, Date

from cql_tools import cql_keyspaces, cql_tables, cql_table_metadata, cql_table_pk_columns, cql_table_column
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
        rest_rows = rest_res.value['rows']

        cql_query = 'SELECT * FROM "{ks}"."{t}" WHERE '.format(ks=ks, t=table)
        cql_query += ' AND '.join(["{}=%s".format(c) for c in key_columns])
        self.LOG.info(cql_query)
        cql_rows = cql_session.execute(cql_query, pk_values).all()

        def explanation(cql_out, rest_out):
            return "Results from CQL Query: {q}\nand REST {url}\ndiffers.\n\n" \
            "CQL Results: {cql}\nREST Results: {rest}".format(
                q=cql_query,
                url=rest_res.url,
                cql=cql_out,
                rest=rest_out
            )

        assert len(rest_rows) == len(cql_rows), explanation(cql_out=cql_rows, rest_out=rest_rows)

        # let's compare row by row and column by columns
        for cql_row, rest_row in zip(cql_rows, rest_rows):
            # all colums should be present
            assert cql_row.keys() == rest_row.keys(), explanation(cql_row, rest_row)
            # let's compare by column values
            for column in cql_row.keys():
                column_metadata = cql_table_column(cql_session, ks, table, column)
                if self._ignore_column(column_metadata):
                    self.LOG.info("ignoring {}".format(column_metadata))
                    continue
                rest_val = self._normalize_value(rest_row[column])
                cql_val = self._normalize_value(cql_row[column])
                assert cql_val == rest_val, "{c}({t}):\n".format(c=column, t=column_metadata.cql_type) \
                                            + explanation(cql_val, rest_val) \
                                            + "\ncql_raw: {}".format(cql_row[column]) \
                                            + "\nrest_raw: {}".format(rest_row[column]) \
                                            + "\ncql_type: {}".format(type(cql_row[column])) \
                                            + "\nrest_type: {}".format(type(rest_row[column]))

                    # TODO: maybe collect issues and then dump instead of fail on 1st

    def _ignore_column(self, column_metadata):
        column_type = str(column_metadata.cql_type)
        if 'blob' in column_type:
            return True
        if 'map<double, text>' in column_type:
            return True
        return False

    def _normalize_value(self, value):
        if isinstance(value, UUID):
            return str(value)
        elif isinstance(value, datetime.datetime):
            n = str(value).replace(' ', 'T')
            if not n.endswith('Z'):
                n += 'Z'
            return n
        elif isinstance(value, OrderedMapSerializedKey):
            items =  list(value.items())
            return self._normalize_items_list(items)
        elif isinstance(value, dict):
            items = list(collections.OrderedDict(value).items())
            return self._normalize_items_list(items)
        elif isinstance(value, collections.OrderedDict):
            items = list(value.items())
            return self._normalize_items_list(items)
        elif isinstance(value, SortedSet):
            return list(value)
        elif isinstance(value, Date):
            return str(value)
        elif isinstance(value, float):
            # floats we should cut after 6 significant digits
            return round(value, 6)
        else:
            return value

    def _normalize_items_list(self, items):
        """
        We need to normalize both keys and values e.g. Date(1110002) to "YYYY-MM-DD"
        :param items:
        :return:
        """
        return sorted([(self._normalize_value(key), self._normalize_value(val)) for key, val in items])

    def _normalize_row(self, row):
        return dict(map(lambda x: (x[0], self._normalize_value(x[1])), row.items()))
