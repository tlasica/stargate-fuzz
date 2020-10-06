from collections import namedtuple


RESTResult = namedtuple('RESTResult', ['ok', 'value', 'status_code', 'error', 'url'])


def build_resp_error(resp):
    """
    Builds resp error string based on the response, if ok returns None
    :param resp: response content
    :return: None if ok else string containing essential request + response info
    """
    if resp.ok:
        return None
    else:
        return '{url} returned {status} with:\n {out}'.format(
            url=resp.url,
            status=resp.status_code,
            out=resp.text
        )
