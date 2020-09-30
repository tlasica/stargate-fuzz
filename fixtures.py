import pytest

from cql_tools import CQLConnection
from rest_api import RESTApiV1, RESTAuth
from rest_api_config import RESTApiConfig


@pytest.fixture(scope="class")
def rest_v1():
    config = RESTApiConfig()
    auth = RESTAuth(config)
    auth.authenticate()
    return RESTApiV1(config, auth.token)


@pytest.fixture(scope="class")
def cql_session():
    return CQLConnection().connect()
