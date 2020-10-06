import logging
import requests

from test.common.rest.rest_util import RESTResult, build_resp_error


class RESTAuth:

    LOG = logging.getLogger(__name__)

    def __init__(self, config):
        self.config = config
        self.token = None

    def authenticate(self):
        auth_url = self.config.auth_api_url_base()
        resp = requests.post(auth_url,
                             json={'username': self.config.username, 'password': self.config.password})
        self.token = resp.json().get('authToken')
        print("authenticated with token", self.token)
        return RESTResult(ok=resp.ok, value=self.token,
                          status_code=resp.status_code,
                          error=build_resp_error(resp),
                          url=auth_url)

    def is_authenticated(self):
        return self.token is not None

    def token(self):
        return self.token
