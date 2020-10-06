import logging

from test.common.test.checks import cql_row_columns_to_rest_values
from test.common.test.fixtures import *
from test.common.test.objects import select_tables_for_test, some_table_data_with_keys


class TestRestV1Rows:

    LOG = logging.getLogger(__name__)

    def test_delete_rows_by_pk(self, cql_session, rest_v1):
        for ks, table in select_tables_for_test(cql_session, skip_system=True):
            self.LOG.info("testing DELETE by key for {ks}.{t}".format(ks=ks, t=table))
            self._delete_rows_by_pk(cql_session, rest_v1, ks, table)

    def _delete_rows_by_pk(self, cql_session, rest_v1, ks, table):
        data = some_table_data_with_keys(cql_session, ks, table, 100)
        for row, keys in data:
            self._delete_rows(cql_session, rest_v1, ks, table, row, keys)

    def _delete_rows(self, cql_session, rest_v1, ks, table, row, key_columns):
        rest_values = cql_row_columns_to_rest_values(cql_session, ks, table, row, key_columns)

        # rows can be already removed by previous step
        rest_res = rest_v1.get_table_rows_by_pk(ks, table, rest_values)
        assert rest_res.ok
        if len(rest_res.value['rows']) == 0:
            self.LOG.debug("no rows found in {ks}.{t} for key {key}".format(ks=ks, t=table, key=rest_values))
            return

        # delete rows and it should be OK
        rest_res = rest_v1.delete_table_rows_by_pk(ks, table, rest_values)
        assert rest_res.ok

        # get rows and expected to have them gone
        rest_res = rest_v1.get_table_rows_by_pk(ks, table, rest_values)
        assert rest_res.ok
        assert len(rest_res.value['rows']) == 0

