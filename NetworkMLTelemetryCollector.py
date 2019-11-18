#!/usr/bin/env python

# NOT USED ANYMORE

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

import collector
import siteMapping


class NetworkMLTelementryCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/telemetry.perfsonar"
        self.INDEX_PREFIX = 'ps_telemetry-'
        return super().__init__()


    def eventCreator(self, message):
    
        m = json.loads(message)
        data = {
            '_type': 'doc'
        }

        source = m['meta']['source']
        destination = m['meta']['destination']
        data['MA'] = m['meta']['measurement_agent']
        data['src'] = source
        data['dest'] = destination
        so = siteMapping.getPS(source)
        de = siteMapping.getPS(destination)
        if so is not None:
            data['src_site'] = so[0]
            data['src_VO'] = so[1]
        if de is not None:
            data['dest_site'] = de[0]
            data['dest_VO'] = de[1]
        data['src_production'] = siteMapping.isProductionLatency(source)
        data['dest_production'] = siteMapping.isProductionLatency(destination)
        if 'summaries' not in m:
            print(threading.current_thread().name, "no summaries found in the message")
            return
        su = m['summaries']
        for s in su:
            if s['summary_window'] == '60' and s['summary_type'] == 'statistics':
                results = s['summary_data']
                # print(results)
                for r in results:
                    dati = datetime.utcfromtimestamp(float(r[0]))
                    data['_index'] = self.es_index_prefix + INDEX_PREFIX + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
                    data['timestamp'] = r[0] * 1000
                    sha1_hash = hashlib.sha1()
                    sha1_hash.update(m['meta']['org_metadata_key'].encode())
                    sha1_hash.update(str(data['timestamp']).encode())
                    data['_id'] = sha1_hash.hexdigest()
                    data['sim_util'] = r[1]['ml']
            # print(data)
            self.aLotOfData.append(copy.copy(data))
        
def main():
    collector = NetworkMLTelementryCollector()
    collector.start()

if __name__ == "__main__":
    main()