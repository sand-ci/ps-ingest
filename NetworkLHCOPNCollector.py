#!/usr/bin/env python

# TO BE RUN ONLY AT CERN

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
import tools
import collector


class NetworkLHCOPNCollector(collector.Collector):
    
    def __init__(self):
        self.TOPIC = "/topic/netflow.lhcopn"
        self.INDEX_PREFIX = 'ps_lhcopn-'
        return super().__init__()


    def eventCreator(self, message):
    
        m = json.loads(message)
        data = {
            '_type': 'doc'
        }
        if not 'data'in m:
            print(threading.current_thread().name, 'no data in this message!')
            return

        source = m['data']['src_site']
        destination = m['data']['dst_site']
        data['MA'] = 'capc.cern'
        data['src_interface'] = source
        data['dest_interface'] = destination
        ts = m['data']['timestamp']
        th = m['data']['throughput']
        dati = datetime.utcfromtimestamp(float(ts))
        data['_index'] = self.es_index_prefix + self.INDEX_PREFIX + str(dati.year) + "." + str(dati.month)  # + "." + str(dati.day)
        data['timestamp'] = int(float(ts) * 1000)
        sha1_hash = hashlib.sha1()
        sha1_hash.update((m['meta']['org_metadata_key'].encode())
        sha1_hash.update(str(data['timestamp']).encode())
        data['_id'] = sha1_hash.hexdigest()
        data['utilization'] = int(th)
        # print(data)
        self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkLHCOPNCollector()
    collector.start()

if __name__ == "__main__":
    main()