#!/usr/bin/env python

import Queue
import socket
import time
import threading
import copy
import json
from datetime import datetime

import stomp

import siteMapping
import tools

topic = '/topic/netflow.lhcopn'

siteMapping.reload()

conns = []


class MyListener(object):
    def on_message(self, headers, message):
        q.put(message)

    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_heartbeat_timeout(self):
        print('AMQ - lost heartbeat. Needs a reconnect!')
        connectToAMQ()

    def on_disconnected(self):
        print('AMQ - no connection. Needs a reconnect!')
        connectToAMQ()


def connectToAMQ():
    global conns
    for conn in conns:
        if conn.is_connected():
            print('disconnecting first ...')
            conn.disconnect()
    conns = []

    addresses = socket.getaddrinfo('netmon-mb.cern.ch', 61513)
    ips = set()
    for a in addresses:
        ips.add(a[4][0])
    allhosts = []
    for ip in ips:
        allhosts.append([(ip, 61513)])

    for host in allhosts:
        conn = stomp.Connection(host, user='psatlflume', passcode=AMQ_PASS)
        conn.set_listener('MyConsumer', MyListener())
        conn.start()
        conn.connect()
        conn.subscribe(destination=topic, ack='auto', id="1", headers={})
        conns.append(conn)


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
        data['_index'] = "network_weather-" + \
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


AMQ_PASS = tools.get_pass()

connectToAMQ()

q = Queue.Queue()
# start eventCreator threads
for i in range(3):
    t = threading.Thread(target=eventCreator)
    t.daemon = True
    t.start()

while True:
    time.sleep(60)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "qsize:", q.qsize())
    for conn in conns:
        if not conn.is_connected():
            print('problem with connection. try reconnecting...')
            connectToAMQ()
            break
