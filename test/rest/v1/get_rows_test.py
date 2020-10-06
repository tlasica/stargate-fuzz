import logging

from test.common.test.checks import cql_row_columns_to_rest_values, compare_cql_with_rest
from test.common.cql.cql_tools import cql_table_column
from test.common.test.fixtures import *
from test.common.test.objects import select_tables_for_test, some_table_data_with_keys


class TestRestV1Rows:

    LOG = logging.getLogger(__name__)

    def test_get_rows_by_pk(self, cql_session, rest_v1):
        for ks, table in select_tables_for_test(cql_session, skip_system=True):
            self.LOG.info("testing GET by key for {ks}.{t}".format(ks=ks, t=table))
            self._compare_rows_by_pk(cql_session, rest_v1, ks, table)

    def _compare_rows_by_pk(self, cql_session, rest_v1, ks, table):
        data = some_table_data_with_keys(cql_session, ks, table, 100)
        for row, keys in data:
            self.compare_rest_by_pk_vs_cql_query(cql_session, rest_v1, ks, table, row, keys)

    def compare_rest_by_pk_vs_cql_query(self, cql_session, rest_v1, ks, table, row, key_columns):
        """
        Compare CQL query using PK and CK columns vs REST V1 query by key
        """
        cql_values = [row[c] for c in key_columns]
        rest_values = cql_row_columns_to_rest_values(cql_session, ks, table, row, key_columns)

        rest_res = rest_v1.get_table_rows_by_pk(ks, table, rest_values)
        assert rest_res.ok
        rest_rows = rest_res.value['rows']

        cql_query = 'SELECT * FROM "{ks}"."{t}" WHERE '.format(ks=ks, t=table)
        cql_query += ' AND '.join(["{}=%s".format(c) for c in key_columns])
        self.LOG.info(cql_query)
        cql_rows = cql_session.execute(cql_query, cql_values).all()

        def explanation(cql_out, rest_out, cql_column=None):
            lines = [
                "Results from CQL Query: {}".format(cql_query),
                "and REST call {}".format(rest_res.url),
                "differ"
            ]
            if cql_column:
                lines.append("on column {n}: {t}".format(n=cql_column.name, t=cql_column.cql_type))
            if cql_out:
                lines.append("CQL result: {}".format(cql_out))
            if rest_out:
                lines.append("REST result: {}".format(rest_out))
            return '\n'.join(lines)

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
                rest_val = rest_row[column]
                cql_val = cql_row[column]

                assert compare_cql_with_rest(cql_val, rest_val, column_metadata),\
                    explanation(cql_out=cql_val, rest_out=rest_val, cql_column=column_metadata)

    def _ignore_column(self, column_metadata):
        column_type = str(column_metadata.cql_type)
        if 'blob' in column_type:  # reported issue
            return True
        # if 'map<double, text>' in column_type:
        #     return True
        if column_type == 'decimal':  # reported issue
            return True
        return False

