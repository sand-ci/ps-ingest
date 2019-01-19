#!/usr/bin/env python

import os
import time
import copy
import json
from datetime import datetime
import threading

import collector


class NetworkTracerouteCollector(collector.Collector):

    def __init__(self):

        self.TOPIC = "/topic/perfsonar.raw.packet-trace"
        self.INDEX_PREFIX = 'ps_trace-'
        super(NetworkTracerouteCollector, self).__init__()



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
        if not 'datapoints' in m:
            print(threading.current_thread().name,
                "no datapoints found in the message")
            return
        dp = m['datapoints']
        # print(su)
        for ts in dp:
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = self.es_index_prefix + INDEX_PREFIX + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            data['_id'] = hash((m['meta']['org_metadata_key'], data['timestamp']))
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
            self.aLotOfData.append(copy.copy(data))
            


def main():
    collector = NetworkTracerouteCollector()
    collector.start()

if __name__ == "__main__":
    main()