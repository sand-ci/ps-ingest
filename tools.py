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
    print("make sure we are connected right...")
    try:
        es_conn = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}])
        print("connected OK!")
    except es_exceptions.ConnectionError as error:
        print('ConnectionError in GetESConnection: ', error)
    except:
        print('Something seriously wrong happened.')
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
        print(error[0])
        for i in error[1]:
            print(i)
    except:
        print('Something seriously wrong happened.')
    return success


def get_pass():
    """ read pass from the environment """
    pas = os.environ['AMQ_PASS']
    if pas:
        return pas
    else:
        sys.exit(1)
