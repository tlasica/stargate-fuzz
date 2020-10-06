import pytest

from test.common.config.rest_api_config import RESTApiConfig
from test.common.cql.cql_tools import CQLConnection
from test.common.rest.rest_auth import RESTAuth
from test.common.rest.rest_v1_api import RESTApiV1


@pytest.fixture(scope="class")
def rest_v1():
    config = RESTApiConfig()
    auth = RESTAuth(config)
    auth.authenticate()
    return RESTApiV1(config, auth.token)


@pytest.fixture(scope="class")
def cql_session():
    return CQLConnection().connect()
