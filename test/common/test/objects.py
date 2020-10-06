import random

from test.common.config.test_config import TestConfig
from test.common.cql.cql_tools import cql_keyspaces, cql_tables, cql_table_pk_columns


def select_tables_for_test(cql_session, skip_system):
    """
    Return list of pairs (ks,table) without skipped and (if requested) system tables
    """
    res = []
    for ks in cql_keyspaces(cql_session):
        for table in cql_tables(cql_session, ks):
            if TestConfig.skip_table(ks, table):
                continue
            if skip_system and TestConfig.is_system_table(ks, table):
                continue
            res.append((ks, table))
    return res


def some_table_data_with_keys(cql_session, ks, table, n):
    """
    From given table returns list of [row, keys] for random rows and random valid subset of primary key
    """
    res = []
    cql_data = cql_session.execute('SELECT * FROM "{ks}"."{t}" LIMIT {n}'.format(ks=ks, t=table, n=n))
    partk_columns, clustk_columns = cql_table_pk_columns(cql_session, ks, table)
    for row in cql_data:
        n_cols = random.randint(0, len(clustk_columns) + 1)
        key_columns = partk_columns + clustk_columns[0:n_cols]
        res.append((row, key_columns))
    return res
