import os

from test.common.config.test_config import TestConfig


class RESTApiConfig:

    def __init__(self):
        self.host = TestConfig.test_host()
        self.username = TestConfig.username()
        self.password = TestConfig.password()
        self.auth_api_port = int(os.environ.get('REST_AUTH_PORT', '8081'))
        self.rest_api_port = int(os.environ.get('REST_API_PORT', '8082'))
        self.api_prefix = os.environ.get('REST_API_PREFIX', '')
        if self.api_prefix and not self.api_prefix.startswith('/'):
            self.api_prefix = '/' + self.api_prefix

    def auth_api_url_base(self):
        return 'http://{host}:{port}{prefix}/v1/auth'.format(
            host=self.host, port=self.auth_api_port, prefix=self.api_prefix)

    def v1_url_base(self):
        return 'http://{host}:{port}{prefix}/v1'.format(
            host=self.host, port=self.rest_api_port, prefix=self.api_prefix)
