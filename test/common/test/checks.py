import datetime
import collections
from decimal import Decimal

from cassandra.cqltypes import UUID
from cassandra.util import OrderedMapSerializedKey, SortedSet, Date

from test.common.cql.cql_tools import cql_table_column


def timestamp_rest_format(value):
    n = str(value).replace(' ', 'T')
    if not n.endswith('Z'):
        n += 'Z'
    millis = n.split('.')[-1]
    if len(millis) == 7:
        n = n.replace('000Z', 'Z')
    return n


def compare_cql_with_rest(cql_value, rest_value, cql_column_metadata):
    column_type = cql_column_metadata.cql_type
    if 'decimal' == str(column_type):
        # result = Decimal.from_float(rest_value).compare(cql_value)
        return str(cql_value).strip("0") == str(rest_value).strip("0")
    else:
        rest_normal = normalize_value(rest_value)
        cql_normal = normalize_value(cql_value)
        if isinstance(rest_normal, list):
            rest_normal = sorted(rest_normal)
        if isinstance(cql_normal, list):
            cql_normal = sorted(cql_normal)
        return str(cql_normal) == str(rest_normal), '\n'.join([
            "CQL raw: {}".format(cql_value),
            "CQL normal: {}".format(cql_normal),
            "REST raw: {}".format(rest_value),
            "REST normal: {}".format(rest_normal)
        ])


def normalize_value(value, cql_column_metadata=None):
    """
    We need to normalize values because those reported from test and from REST apis can differ
    Examples are dictionaries or the fact that in REST dict keys needs to be texts
    :param value: value to normalize
    :param cql_column_metadata: optional information that can help to normalize
    :return: value that is comparable between different APIs (cql vs REST e.g.)
    """
    if cql_column_metadata:
        the_type = cql_column_metadata.cql_type
        if 'decimal' == str(the_type):
            if isinstance(value, float):
                return Decimal.from_float(value)

    if isinstance(value, UUID):
        return str(value)
    elif isinstance(value, datetime.datetime):
        return timestamp_rest_format(value)
    elif isinstance(value, OrderedMapSerializedKey):
        items = list(value.items())
        return normalize_items_list(items)
    elif isinstance(value, dict):
        items = list(collections.OrderedDict(value).items())
        return normalize_items_list(items)
    elif isinstance(value, collections.OrderedDict):
        items = list(value.items())
        return normalize_items_list(items)
    elif isinstance(value, SortedSet):
        n = [normalize_value(x) for x in list(value)]
        print("before", str(value))
        print("after", str(n))
    elif isinstance(value, Date):
        return str(value)
    # elif isinstance(value, Decimal):
    #     return str(value)
    elif isinstance(value, float):
        # floats we should cut after 6 significant digits
        return round(value, 6)
    else:
        return value


def normalize_items_list(dict_items):
    """
    We need to normalize both keys and values e.g. Date(1110002) to "YYYY-MM-DD"
    and also str() key as only text are valid for json dictionaries
    :param dict_items: list of pairs (key,val) that constructs a dictionary
    :return: list of pairs with keys and values normalized
    """
    return sorted([(str(normalize_value(key)), normalize_value(val)) for key, val in dict_items])


def cql_row_columns_to_rest_values(cql_session, ks, table, cql_row, columns):
    values = []
    for col_name in columns:
        val = cql_row[col_name]
        col_metadata = cql_table_column(cql_session, ks, table, col_name)
        if col_metadata:
            if 'timestamp' in col_metadata.cql_type:
                val = timestamp_rest_format(val)
        values.append(val)
    return values
