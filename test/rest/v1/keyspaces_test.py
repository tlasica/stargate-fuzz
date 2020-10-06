import logging

from test.common.cql.cql_tools import cql_keyspaces, cql_tables, cql_table_metadata
from test.common.test.fixtures import *


class TestRestV1Keyspaces:
    """
    Tests if GET on existing C* tables is working correctly
    by comparing output with metadata collected using python driver
    # GET / v1 / keyspaces(io.stargate.web.resources.KeyspaceResource)
    # GET / v1 / keyspaces / {keyspaceName} / tables(io.stargate.web.resources.TableResource)
    # GET / v1 / keyspaces / {keyspaceName} / tables / {tableName}(io.stargate.web.resources.TableResource)
    """

    LOG = logging.getLogger(__name__)

    def _rest_keyspaces(self, rest_v1):
        res = rest_v1.list_keyspaces()
        assert res.ok, res.error
        ks_names = set(res.value)
        if 'stargate_system' in ks_names:  # C2-306
            ks_names.remove('stargate_system')
        return ks_names

    def _rest_tables(self, rest_v1, ks):
        resp = rest_v1.list_keyspace_tables(ks)
        assert resp.ok, resp.error
        return set(resp.value)

    def test_keyspaces_names(self, cql_session, rest_v1):
        cql_ks_names = set(cql_keyspaces(cql_session).keys())
        rest_ks_names = self._rest_keyspaces(rest_v1)
        assert cql_ks_names == rest_ks_names

    def test_keyspace_tables(self, cql_session, rest_v1):
        ks_names = self._rest_keyspaces(rest_v1)
        for ks in ks_names:
            tables = set(cql_tables(cql_session, ks).keys())
            ks_tables = self._rest_tables(rest_v1, ks)
            assert tables == ks_tables

    def test_table_definitions(self, cql_session, rest_v1):
        ks_names = self._rest_keyspaces(rest_v1)

        def check(ks, table):
            self.LOG.info("Checking {ks}.{t}".format(ks=ks, t=table))
            cql_metadata = cql_table_metadata(cql_session, ks, table)
            resp = rest_v1.get_table(ks, table)
            assert resp.ok, resp.error
            rest_metadata = resp.value
            # all columns are present (compare sets)
            rest_columns = set([x['name'] for x in rest_metadata['columnDefinitions']])
            cql_colums = set(cql_metadata.columns.keys())
            assert rest_columns == cql_colums
            # TODO: for every column check definition (static and type)

        for ks in ks_names:
            for table in self._rest_tables(rest_v1, ks):
                check(ks, table)
