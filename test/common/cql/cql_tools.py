import os

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from test.common.config.test_config import TestConfig


class CQLConfig:
    def __init__(self):
        self.host = TestConfig.test_host()
        self.username = os.environ.get('CQL_USERNAME', 'cassandra')
        self.password = os.environ.get('CQL_PASSWORD', 'cassandra')


class CQLConnection:

    def connect(self):
        config = CQLConfig()
        # TODO: support authentication
        # auth_provider = PlainTextAuthProvider(username=config.username, password=config.password)
        # cluster = Cluster([config.host], auth_provider=auth_provider)
        cluster = Cluster([config.host],
                          protocol_version=4,
                          load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        from cassandra.query import dict_factory
        session.row_factory = dict_factory
        return session


def cql_keyspaces(cql_session):
    return cql_session.cluster.metadata.keyspaces


def cql_tables(cql_session, ks_name):
    ks = cql_session.cluster.metadata.keyspaces.get(ks_name, None)
    return ks.tables if ks else None


def cql_table_metadata(cql_session, ks_name, table):
    ks = cql_session.cluster.metadata.keyspaces.get(ks_name, None)
    return ks.tables.get(table, None) if ks else None


def cql_table_pk_columns(cql_session, ks_name, table):
    metadata = cql_table_metadata(cql_session, ks_name, table)
    if metadata:
        partition_key_cols = [x.name for x in metadata.partition_key]
        clustering_key_cols = [x.name for x in metadata.clustering_key]
        return partition_key_cols, clustering_key_cols
    else:
        return None, None


def cql_table_column(cql_session, ks_name, table, column):
    metadata = cql_table_metadata(cql_session, ks_name, table)
    if metadata:
        return metadata.columns.get(column, None)
    else:
        return None
