
import socket
import threading
from threading import Thread
import time
import os
import sys
import stomp
import traceback
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers
from datetime import datetime

try: 
    import queue
except ImportError:
    import Queue as queue

import siteMapping

class Collector(object):

    class MyListener(object):

        def __init__(self, q, collector):
            self.q = q
            self.collector = collector

        def on_message(self, headers, message):
            self.q.put([message, headers])

        def on_error(self, headers, message):
            print('received an error %s' % message)
            os._exit(1)

        def on_heartbeat_timeout(self):
            print('AMQ - lost heartbeat. Needs a reconnect!')
            self.collector.connect_to_MQ(reset=True)

        def on_disconnected(self):
            print('AMQ - no connection. Needs a reconnect!')
            self.collector.connect_to_MQ(reset=True)

    def __init__(self):
        siteMapping.reload()
        # MQ connection
        self.connection = None
        self.q = queue.Queue()
        self.RMQ_parameters = self.get_RMQ_connection_parameters()

        self.es_index_prefix = os.environ.get("ES_INDEX_PREFIX", "")
        self.aLotOfData = None
        self.last_flush = time.time()

    def start(self):
        # start eventCreator threads
        self.t = Thread(target=self.watchMessages)
        self.t.daemon = True
        self.t.start()


        while True:
            self.connect_to_MQ()
            time.sleep(55)
            print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "threads:", threading.active_count(), "qsize:", self.q.qsize())


    def watchMessages(self):
        """
        Managing creating and sending messages
        """
        while True:
            try:
                (msg, headers) = self.q.get(timeout=10)
            except queue.Empty as qe:
                # Try to flush the data
                self.flushData()
                continue
            
            try:
                self.eventCreator(msg)
            except Exception as e:
                # Failed to create the event
                traceback.print_exc()
                # Flush the current data
                self.flushData()
                # Send a nack on the current headers
                self.connection.nack(headers['message-id'], self.RMQ_parameters['RMQ_ID'])
                continue


            # Set the last successful headers
            self.last_headers = headers
            self.flushData()
            self.q.task_done()


    def flushData(self):
        """
        Flush the data, if it's time
        """
        if self.aLotOfData is None or len(self.aLotOfData) == 0:
            return
        
        if len(self.aLotOfData) > 100 or (time.time() - self.last_flush) > 10:
            success = False
            while not success:
                success = self.bulk_index(self.aLotOfData, es_conn=es_conn, thread_name=threading.current_thread().name)
                if success is True:
                    self.aLotOfData = []
                    self.connection.ack(self.last_headers['message-id'], self.RMQ_parameters['RMQ_ID'])
                    self.last_flush = time.time()
                    break
                else:
                    print("Unable to post to ES")
                    time.sleep(10)
                



    def eventCreator(self):
        pass

    def connect_to_MQ(self, reset=False):

        if self.connection is not None:
            if reset and self.connection.is_connected():
                self.connection.disconnect()
                self.connection = None

            if self.connection.is_connected():
                return

        print("connecting to MQ")
        self.connection = None

        addresses = socket.getaddrinfo('clever-turkey.rmq.cloudamqp.com', 61614)
        ip = addresses[0][4][0]
        host_and_ports = [(ip, 61614)]
        print(host_and_ports)

        self.connection = stomp.Connection(
            host_and_ports=host_and_ports,
            use_ssl=True,
            vhost=self.RMQ_parameters['RMQ_VHOST']
        )
        self.connection.set_listener('MyConsumer', Collector.MyListener(self.q, self))
        self.connection.start()
        self.connection.connect(self.RMQ_parameters['RMQ_USER'], self.RMQ_parameters['RMQ_PASS'], wait=True)
        self.connection.subscribe(destination=self.TOPIC, ack='client', id=self.RMQ_parameters['RMQ_ID'], headers={"durable": True, "auto-delete": False})

    def get_es_connection(self):
        """
        establishes es connection.
        """
        print("make sure we are connected to ES...")
        while True:
            try:
                if 'ES_USER' in os.environ and 'ES_PASS' in os.environ and 'ES_HOST' in os.environ:
                    self.es_conn = Elasticsearch(
                        [{'host': os.environ['ES_HOST'], 'port': 9200}],
                        http_auth=(os.environ['ES_USER'], os.environ['ES_PASS'])
                    )
                else:
                    self.es_conn = Elasticsearch([{'host': 'atlas-kibana.mwt2.org', 'port': 9200}])
                print("connected OK!")
            except es_exceptions.ConnectionError as error:
                print('ConnectionError in get_es_connection: ', error)
            except:
                print('Something seriously wrong happened in getting ES connection.')
            else:
                return es_conn
            time.sleep(70)


    def bulk_index(self, data, es_conn=None, thread_name=''):
        """
        sends the data to ES for indexing.
        if successful returns True.
        """
        success = False
        if self.es_conn is None:
            self.es_conn = get_es_connection()
        try:
            res = helpers.bulk(es_conn, data, raise_on_exception=True, request_timeout=120)
            print(thread_name, "inserted:", res[0], 'errors:', res[1])
            success = True
        except es_exceptions.ConnectionError as error:
            print('ConnectionError ', error)
        except es_exceptions.TransportError as error:
            print('TransportError ', error)
        except helpers.BulkIndexError as error:
            print(error)
        except Exception as e:
            traceback.print_exc()
            print('Something seriously wrong happened.')
        return success


    def get_RMQ_connection_parameters(self):
        """ read vhost, user, pass from the environment """
        ret = {'RMQ_VHOST': '', 'RMQ_USER': '', 'RMQ_PASS': '', 'RMQ_ID': ''}
        for var in ret:
            val = os.environ[var]
            if val:
                ret[var] = val
            else:
                print('environment variable', var, 'not defined. Exiting.')
                sys.exit(1)
        return ret





