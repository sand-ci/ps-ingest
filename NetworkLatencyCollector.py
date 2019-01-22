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
import math

import siteMapping
import collector

class NetworkLatencyCollector(collector.Collector):


    def __init__(self):
        self.TOPIC = "/topic/perfsonar.raw.histogram-owdelay"
        self.INDEX_PREFIX = 'ps_owd-'
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
        su = m['datapoints']
        for ts, th in su.items():
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = self.es_index_prefix + self.INDEX_PREFIX + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            data['_id'] = hash((m['meta']['org_metadata_key'], ts))

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
            self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkLatencyCollector()
    collector.start()

if __name__ == "__main__":
    main()

