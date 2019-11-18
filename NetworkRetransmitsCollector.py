#!/usr/bin/env python

import os
import queue
import socket
import time
import threading
from threading import Thread
import copy
import json
from datetime import datetime
import hashlib

import stomp
import siteMapping
import collector


class NetworkRetransmitsCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/perfsonar.raw.packet-retransmits"
        self.INDEX_PREFIX = 'ps_retransmits-'
        super(NetworkRetransmitsCollector, self).__init__()

    def eventCreator(self, message):
    
        m = json.loads(message)

        data = {
            '_type': 'doc'
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
            data['src_site'] = so[0]
            data['src_VO'] = so[1]
        if de != None:
            data['dest_site'] = de[0]
            data['dest_VO'] = de[1]
        data['src_production'] = siteMapping.isProductionThroughput(source)
        data['dest_production'] = siteMapping.isProductionThroughput(
            destination)
        if not 'datapoints'in m:
            print(threading.current_thread().name,
                  'no datapoints in this message!')
            return
        su = m['datapoints']
        for ts, th in su.items():
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = self.es_index_prefix + self.INDEX_PREFIX + str(dati.year) + "." + str(dati.month)  # + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            sha1_hash = hashlib.sha1()
            sha1_hash.update((m['meta']['org_metadata_key'].encode())
            sha1_hash.update(str(data['timestamp']).encode())
            data['_id'] = sha1_hash.hexdigest()
            data['retransmits'] = th
            # print(data)
            self.aLotOfData.append(copy.copy(data))



def main():
    collector = NetworkRetransmitsCollector()
    collector.start()

if __name__ == "__main__":
    main()
