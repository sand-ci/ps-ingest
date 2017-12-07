#!/usr/bin/env python

import queue
import socket
import time
import threading
from threading import Thread
import copy
import json
from datetime import datetime

import stomp
import tools
import siteMapping

tools.TOPIC = "/topic/netflow.lhcopn"
siteMapping.reload()


class MyListener(object):

    def on_message(self, headers, message):
        q.put(message)

    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_heartbeat_timeout(self):
        print('AMQ - lost heartbeat. Needs a reconnect!')
        connect_to_MQ(reset=True)

    def on_disconnected(self):
        print('AMQ - no connection. Needs a reconnect!')
        connect_to_MQ(reset=True)


def connect_to_MQ(reset=False):

    if tools.connection is not None:
        if reset and tools.connection.is_connected():
            tools.connection.disconnect()
            tools.connection = None

        if tools.connection.is_connected():
            return

    print("connecting to MQ")
    tools.connection = None

    addresses = socket.getaddrinfo('clever-turkey.rmq.cloudamqp.com', 61614)
    ip = addresses[0][4][0]
    host_and_ports = [(ip, 61614)]
    print(host_and_ports)

    tools.connection = stomp.Connection(
        host_and_ports=host_and_ports,
        use_ssl=True,
        vhost=RMQ_parameters['RMQ_VHOST']
    )
    tools.connection.set_listener('MyConsumer', MyListener())
    tools.connection.start()
    tools.connection.connect(RMQ_parameters['RMQ_USER'], RMQ_parameters['RMQ_PASS'], wait=True)
    tools.connection.subscribe(destination=tools.TOPIC, ack='auto', id="1", headers={})
    return


def eventCreator():
    aLotOfData = []
    es_conn = tools.get_es_connection()
    while True:
        d = q.get()
        m = json.loads(d)
        data = {
            '_type': 'netflow_lhcopn'
        }
        if not 'data'in m:
            print(threading.current_thread().name, 'no data in this message!')
            q.task_done()
            continue

        source = m['data']['src_site']
        destination = m['data']['dst_site']
        data['MA'] = 'capc.cern'
        data['srcInterface'] = source
        data['dstInterface'] = destination
        ts = m['data']['timestamp']
        th = m['data']['throughput']
        dati = datetime.utcfromtimestamp(float(ts))
        data['_index'] = tools.index_prefix + \
            str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
        data['timestamp'] = int(float(ts) * 1000)
        data['utilization'] = int(th)
        # print(data)
        aLotOfData.append(copy.copy(data))

        q.task_done()
        if len(aLotOfData) > 10:
            succ = tools.bulk_index(aLotOfData, es_conn=es_conn, thread_name=threading.current_thread().name)
            if succ is True:
                aLotOfData = []


RMQ_parameters = tools.get_RMQ_connection_parameters()
tools.set_index_prefix()


q = queue.Queue()
# start eventCreator threads
for i in range(1):
    t = Thread(target=eventCreator)
    t.daemon = True
    t.start()


while True:
    connect_to_MQ()
    time.sleep(55)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "qsize:", q.qsize())
