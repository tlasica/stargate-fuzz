from collections import namedtuple

import logging
import requests


LOG = logging.getLogger(__name__)


RESTResult = namedtuple('RESTResult', ['ok', 'value', 'status_code', 'error', 'url'])


class RESTAuth:

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



def build_resp_error(resp):
    if resp.ok:
        return None
    else:
        return '{url} returned {status} with:\n {out}'.format(
            url=resp.url,
            status=resp.status_code,
            out=resp.text
        )


class RESTApiV1:

    def __init__(self, config, token):
        self.config = config
        self.token = token
        self.session = requests.Session()
        if token:
            print("using token:", token)
            self.session.headers.update({'x-cassandra-token': token})

    def url(self, resource):
        return self.config.v1_url_base() + resource

    def _get(self, resource):
        resp = self.session.get(self.url(resource))
        error = build_resp_error(resp)
        return RESTResult(ok=resp.ok, value=resp.json(), status_code=resp.status_code, error=error, url=resp.url)

    def list_keyspaces(self):
        return self._get('/keyspaces')

    def list_keyspace_tables(self, ks_name):
        return self._get('/keyspaces/{}/tables'.format(ks_name))

    def get_table(self, ks_name, table):
        return self._get('/keyspaces/{ks}/tables/{t}'.format(ks=ks_name, t=table))

    def get_table_rows_by_pk(self, ks_name, table, pk_values):
        pk = ';'.join([str(x) for x in pk_values])
        resource = '/keyspaces/{ks}/tables/{t}/rows/{pk}'.format(ks=ks_name, t=table, pk=pk)
        return self._get(resource)
