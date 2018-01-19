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

TOPIC = "/topic/perfsonar.summary.meta"
INDEX_PREFIX = 'perfsonar_meta-'
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
        data = {'_type': 'doc'}

        dati = datetime.utcfromtimestamp(float(m['timestamp']))
        data['_index'] = INDEX_PREFIX + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
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
