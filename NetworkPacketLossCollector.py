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

import siteMapping
import collector



class NetworkPacketLossCollector(collector.Collector):


    def __init__(self):

        self.TOPIC = "/topic/perfsonar.raw.packet-loss-rate"
        self.INDEX_PREFIX = 'ps_packetloss-'


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
        data['src_host'] = m['meta']['input_source']
        data['dest_host'] = m['meta']['input_destination']
        data['ipv6'] = False
        if ':' in source or ':' in destination:
            data['ipv6'] = True
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
        if 'datapoints' not in m:
            print(threading.current_thread().name, "no datapoints found in the message")
            return
        su = m['datapoints']
        # print(su)
        for ts, th in su.items():
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = self.es_index_prefix + INDEX_PREFIX + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            data['_id'] = hash((m['meta']['org_metadata_key'], data['timestamp']))
            data['packet_loss'] = th
            # print(data)
            aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkPacketLossCollector()
    collector.start()

if __name__ == "__main__":
    main()