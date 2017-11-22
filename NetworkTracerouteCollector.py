#!/usr/bin/env python

import Queue
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

topic = '/topic/perfsonar.raw.packet-trace'

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
            '_type': 'traceroute'
        }
        # print(m)
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
        if so != None:
            data['srcSite'] = so[0]
            data['srcVO'] = so[1]
        if de != None:
            data['destSite'] = de[0]
            data['destVO'] = de[1]
        data['srcProduction'] = siteMapping.isProductionThroughput(source)
        data['destProduction'] = siteMapping.isProductionThroughput(
            destination)
        if not 'datapoints' in m:
            q.task_done()
            print(threading.current_thread().name,
                  "no datapoints found in the message")
            continue
        dp = m['datapoints']
        # print(su)
        for ts in dp:
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = "network_weather-" + \
                str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            data['hops'] = []
            data['rtts'] = []
            data['ttls'] = []
            hops = dp[ts]
            for hop in hops:
                if 'ttl' not in hop or 'ip' not in hop or 'query' not in hop:
                    continue
                nq = int(hop['query'])
                if nq != 1:
                    continue
                data['hops'].append(hop['ip'])
                data['ttls'].append(int(hop['ttl']))
                if 'rtt' in hop and hop['rtt'] != None:
                    data['rtts'].append(float(hop['rtt']))
                else:
                    data['rtts'].append(0.0)
                # print(data)
            hs = ''
            for h in data['hops']:
                if h == None:
                    hs += "None"
                else:
                    hs += h
            data['n_hops'] = len(data['hops'])
            if len(data['rtts']):
                data['max_rtt'] = max(data['rtts'])
            data['hash'] = hash(hs)
            aLotOfData.append(copy.copy(data))
        q.task_done()

        if len(aLotOfData) > 100:
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
