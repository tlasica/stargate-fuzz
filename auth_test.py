from cql_tools import CQLConnection
from rest_api import RESTAuth
from rest_api_config import RESTApiConfig


class TestAuthentication:

    def test_rest_authentication(self):
        auth = RESTAuth(RESTApiConfig())
        resp = auth.authenticate()
        assert resp.ok, resp.error
        assert auth.is_authenticated()

    def test_cql_connection(self):
        sess = CQLConnection().connect()
        sess.shutdown()
