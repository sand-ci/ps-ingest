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

import stomp
import tools
import siteMapping
import collector


class NetworkStatusCollector(collector.Collector):
    
    def __init__(self):
        TOPIC = "/topic/perfsonar.summary.status"
        INDEX_PREFIX = 'ps_status-'
        return super(NetworkStatusCollector, self).__init__()

    def eventCreator(self, message):
    
        m = json.loads(message)
        data = {
            '_type': 'doc'
        }
        metrics = ['perfSONAR services: ntp', 'perfSONAR esmond freshness', 'OSG datastore freshness',
                   'perfSONAR services: pscheduler']
        found = False
        for met in metrics:
            if not met in m['metric']:
                return
            found = True
        if not found:
            return

        if 'perf_metrics' in m.keys() and not m['perf_metrics']:
            return

        data['host'] = m['host']
        prefix = m['metric'].replace("perfSONAR", "ps").replace(":", "").replace(" ", "_").lower()
        for k in m['perf_metrics'].keys():
            data[prefix + "_" + k] = m['perf_metrics'][k]
        dati = datetime.utcfromtimestamp(float(m['timestamp']))
        data['_index'] = self.es_index_prefix + INDEX_PREFIX + str(dati.year) + "." + str(dati.month)
        data['timestamp'] = int(float(m['timestamp']) * 1000)
        # print(data)
        self.aLotOfData.append(copy.copy(data))



def main():
    collector = NetworkStatusCollector()
    collector.start()

if __name__ == "__main__":
    main()