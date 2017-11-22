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

topic = '/topic/perfsonar.summary.meta'

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


def convert_to_float(d, tags):
    for t in tags:
        if t not in d:
            continue
        v = d[t]
        if not v:
            continue
        if isinstance(v, float):
            continue
        d[t] = float(v)


def convert_to_int(d, tags):
    for t in tags:
        if t not in d:
            continue
        v = d[t]
        if not v:
            continue
        if isinstance(v, int):
            continue
        if v.isdigit():
            d[t] = int(v)
        else:
            d[t] = None


def clean(data):
    toDel = []
    for tag in data.keys():
        if data[tag] == None or data[tag] == 'unknown':
            toDel.append(tag)
        if type(data[tag]) is dict:
            clean(data[tag])
    for tag in toDel:
        del data[tag]


def eventCreator():
    aLotOfData = []
    es_conn = tools.get_es_connection()
    while True:
        d = q.get()
        m = json.loads(d)
        data = {'_type': 'meta'}

        dati = datetime.utcfromtimestamp(float(m['timestamp']))
        data['_index'] = "network_weather-" + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
        data.update(m)
        data.pop('interfaces', None)
        data['timestamp'] = int(float(m['timestamp']) * 1000)
        data['host'] = data.get('external_address', {}).get('dns_name')

        if "services" in data:
            sers = copy.deepcopy(data["services"])
            data["services"] = {}
            for s in sers:
                if "name" in s:
                    service_name = s["name"]
                    del s["name"]
                    tps = {}
                    if "testing_ports" in s:
                        for tp in s["testing_ports"]:
                            if 'type' not in tp:
                                continue
                            tps[tp['type']] = {"min_port": tp["min_port"], "max_port": tp["max_port"]}
                        s['testing_ports'] = tps
                    data["services"][service_name] = s
                else:
                    continue

        clean(data)

        if 'location' in data.keys():
            lat = data['location'].get('latitude', 0)
            lgt = data['location'].get('longitude', 0)
            if lat and lgt:
                data['geolocation'] = "%s,%s" % (lat, lgt)
            del data['location']

        if 'ntp' in data.keys():
            n = data['ntp']
            convert_to_float(n, ['delay', 'dispersion', 'offset'])
            convert_to_int(n, ['synchronized', 'stratum', 'reach', 'polling_interval'])

        if 'external_address' in data.keys():
            ea = data['external_address']
            if 'counters' in ea.keys():
                convert_to_int(ea['counters'], ea['counters'].keys())

        convert_to_int(data, ['cpu_cores', 'cpus'])
        convert_to_float(data, ['cpu_speed'])

        # print('-----------')
        # print(data)

        aLotOfData.append(copy.copy(data))
        q.task_done()

        if len(aLotOfData) > 10:
            succ = tools.bulk_index(aLotOfData, es_conn=es_conn, thread_name=threading.current_thread().name)
            if succ is True:
                aLotOfData = []
            else:
                print(aLotOfData)


AMQ_PASS = tools.get_pass()

connectToAMQ()

q = Queue.Queue()
# start eventCreator threads
for i in range(1):
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
