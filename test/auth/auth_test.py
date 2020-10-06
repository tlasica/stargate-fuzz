from test.common.config.rest_api_config import RESTApiConfig
from test.common.cql.cql_tools import CQLConnection
from test.common.rest.rest_auth import RESTAuth


class TestAuthentication:

    def test_rest_authentication(self):
        auth = RESTAuth(RESTApiConfig())
        resp = auth.authenticate()
        assert resp.ok, resp.error
        assert auth.is_authenticated()

    def test_cql_connection(self):
        sess = CQLConnection().connect()
        sess.shutdown()
