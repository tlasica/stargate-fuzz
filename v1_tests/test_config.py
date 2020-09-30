import logging
import os


class TestConfig:
    """
    Test configuration e.g.
    - list of tables to skip from SKIP_TABLES variable e.g. SKIP_TABLES="system.local"
    """

    LOG = logging.getLogger(__name__)

    tables_to_skip = None

    @classmethod
    def skip_table(cls, keyspace, table):
        return (keyspace, table) in cls.get_tables_to_skip()

    @classmethod
    def get_tables_to_skip(cls):
        if cls.tables_to_skip is None:
            cls.tables_to_skip = cls._build_skip_tables_from_env()
        return cls.tables_to_skip

    @classmethod
    def _build_skip_tables_from_env(cls):
        res = set()
        list_of_tables = os.environ.get("SKIP_TABLES", "").split(',')
        for ks, table in [x.split('.') for x in list_of_tables if x]:
            res.add((ks, table))
        cls.LOG.info("skip tables: {}".format(res))
        return res
