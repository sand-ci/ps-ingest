""" few generally useful functions """
import os
import sys
import time
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers


def get_es_connection():
    """
    establishes es connection.
    """
    print("make sure we are connected to ES...")
    try:
        if 'ES_USER' in os.environ and 'ES_PASS' in os.environ and 'ES_HOST' in os.environ:
            es_conn = Elasticsearch(
                [{'host': os.environ['ES_HOST'], 'port': 9200}],
                http_auth=(os.environ['ES_USER'], os.environ['ES_PASS'])
            )
        else:
            es_conn = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}])
        print("connected OK!")
    except es_exceptions.ConnectionError as error:
        print('ConnectionError in get_es_connection: ', error)
    except:
        print('Something seriously wrong happened in getting ES connection.')
    else:
        return es_conn

    time.sleep(70)
    get_es_connection()


def bulk_index(data, es_conn=None, thread_name=''):
    """
    sends the data to ES for indexing.
    if successful returns True.
    """
    success = False
    if es_conn is None:
        es_conn = get_es_connection()
    try:
        res = helpers.bulk(es_conn, data, raise_on_exception=True, request_timeout=60)
        print(thread_name, "inserted:", res[0], 'errors:', res[1])
        success = True
    except es_exceptions.ConnectionError as error:
        print('ConnectionError ', error)
    except es_exceptions.TransportError as error:
        print('TransportError ', error)
    except helpers.BulkIndexError as error:
        print(error)
    except:
        print('Something seriously wrong happened.')
    return success


def get_RMQ_connection_parameters():
    """ read vhost, user, pass from the environment """
    ret = {'RMQ_VHOST': '', 'RMQ_USER': '', 'RMQ_PASS': ''}
    for var in ret:
        val = os.environ[var]
        if val:
            ret[var] = val
        else:
            print('environment variable', var, 'not defined. Exiting.')
            sys.exit(1)
    return ret


# MQ connection - here to make it global
connection = None

TOPIC = 'no topic defined.'
index_prefix = 'network_weather-'
