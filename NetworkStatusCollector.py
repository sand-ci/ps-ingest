#!/usr/bin/env python

# TO BE RUN ONLY AT CERN

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

TOPIC = "/topic/perfsonar.summary.status"
INDEX_PREFIX = 'ps_status-'

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
    tools.connection.subscribe(destination=TOPIC, ack='auto', id=RMQ_parameters['RMQ_ID'], headers={"durable": True, "auto-delete": False})
    return


def eventCreator():
    aLotOfData = []
    es_conn = tools.get_es_connection()
    while True:
        d = q.get()
        m = json.loads(d)
        data = {
            '_type': 'doc'
        }

        metrics = ['perfSONAR services: ntp', 'perfSONAR esmond freshness', 'OSG datastore freshness',
                   'perfSONAR services: pscheduler']
        if not any(pattern in m['metric'] for pattern in metrics):
            q.task_done()
            continue
        if 'perf_metrics' in m.keys() and not m['perf_metrics']:
            q.task_done()
            continue
        data['host'] = m['host']
        prefix = m['metric'].replace("perfSONAR", "ps").replace(":", "").replace(" ", "_").lower()
        for k in m['perf_metrics'].keys():
            data[prefix + "_" + k] = m['perf_metrics'][k]
        dati = datetime.utcfromtimestamp(float(m['timestamp']))
        data['_index'] = INDEX_PREFIX + str(dati.year) + "." + str(dati.month)
        data['timestamp'] = int(float(m['timestamp']) * 1000)
        data['_id'] = hash((m['meta']['org_metadata_key'], data['timestamp']))
        # print(data)
        aLotOfData.append(copy.copy(data))
        q.task_done()

        if len(aLotOfData) > 100:
            succ = tools.bulk_index(aLotOfData, es_conn=es_conn, thread_name=threading.current_thread().name)
            if succ is True:
                aLotOfData = []


RMQ_parameters = tools.get_RMQ_connection_parameters()

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
