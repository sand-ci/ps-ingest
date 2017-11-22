#!/usr/bin/env python

import Queue
import time
import threading
from threading import Thread
import socket
import copy
import json
from datetime import datetime

import stomp
import tools
import siteMapping

topic = '/topic/perfsonar.raw.packet-loss-rate'
es = None

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
    print('connecting to AMQ')
    global conns
    for conn in conns:
        if conn.is_connected():
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
            '_type': 'packet_loss_rate'
        }

        source = m['meta']['source']
        destination = m['meta']['destination']
        data['MA'] = m['meta']['measurement_agent']
        data['src'] = source
        data['dest'] = destination
        data['src_host'] = m['meta']['input_source']
        data['dest_host'] = m['meta']['input_destination']
        data['ipv6'] = False
        if ':' in source or ':' in destination:
            data['ipv6'] = True
        so = siteMapping.getPS(source)
        de = siteMapping.getPS(destination)
        if so is not None:
            data['srcSite'] = so[0]
            data['srcVO'] = so[1]
        if de is not None:
            data['destSite'] = de[0]
            data['destVO'] = de[1]
        data['srcProduction'] = siteMapping.isProductionLatency(source)
        data['destProduction'] = siteMapping.isProductionLatency(destination)
        if 'datapoints' not in m:
            q.task_done()
            print(threading.current_thread().name, "no datapoints found in the message")
            continue
        su = m['datapoints']
        # print(su)
        for ts, th in su.iteritems():
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = "network_weather-" + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            data['packet_loss'] = th
            # print(data)
            aLotOfData.append(copy.copy(data))
        q.task_done()

        if len(aLotOfData) > 500:
            succ = tools.bulk_index(aLotOfData, es_conn=es_conn, thread_name=threading.current_thread().name)
            if succ is True:
                aLotOfData = []


AMQ_PASS = tools.get_pass()

connectToAMQ()

q = Queue.Queue()
# start eventCreator threads
for i in range(3):
    t = Thread(target=eventCreator)
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
