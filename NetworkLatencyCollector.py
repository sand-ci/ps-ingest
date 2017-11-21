#!/usr/bin/env python

import Queue
import socket
import time
import threading
import copy
import math
import json
from datetime import datetime

import stomp
import tools
import siteMapping

topic = '/topic/perfsonar.raw.histogram-owdelay'

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
            '_type': 'latency'
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
        su = m['datapoints']
        for ts, th in su.iteritems():
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = "network_weather-" + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            th_fl = dict((float(k), v) for (k, v) in th.items())

            # mean
            samples = sum([v for k, v in th_fl.items()])
            th_mean = sum(k * v for k, v in th_fl.items()) / samples
            data['delay_mean'] = th_mean
            # std dev
            data['delay_sd'] = math.sqrt(sum((k - th_mean) ** 2 * v for k, v in th_fl.items()) / samples)
            # median
            csum = 0
            ordered_th = [(k, v) for k, v in sorted(th_fl.items())]
            midpoint = samples // 2
            if samples % 2 == 0:  # even number of samples
                for index, entry in enumerate(ordered_th):
                    csum += entry[1]
                    if csum > midpoint + 1:
                        data['delay_median'] = entry[0]
                        break
                    elif csum == midpoint:
                        data['delay_median'] = entry[0] + ordered_th[index + 1][0] / 2
                        break
                    elif csum == midpoint + 1 and index == 0:
                        data['delay_median'] = entry[0]
                        break
                    elif csum == midpoint + 1 and index > 0:
                        data['delay_median'] = entry[0] + ordered_th[index - 1][0] / 2
                        break
            else:  # odd number of samples
                for index, entry in enumerate(ordered_th):
                    csum += entry[1]
                    if csum >= midpoint + 1:
                        data['delay_median'] = entry[0]
                        break
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
