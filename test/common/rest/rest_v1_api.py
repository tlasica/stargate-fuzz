import logging
import requests

from test.common.rest.rest_util import RESTResult, build_resp_error

LOG = logging.getLogger(__name__)


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

    def _delete(self, resource):
        resp = self.session.delete(self.url(resource))
        error = build_resp_error(resp)
        return RESTResult(ok=resp.ok, value=None, status_code=resp.status_code, error=error, url=resp.url)

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

    def delete_table_rows_by_pk(self, ks_name, table, pk_values):
        pk = ';'.join([str(x) for x in pk_values])
        resource = '/keyspaces/{ks}/tables/{t}/rows/{pk}'.format(ks=ks_name, t=table, pk=pk)
        return self._delete(resource)
