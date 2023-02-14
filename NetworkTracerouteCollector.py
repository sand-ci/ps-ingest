#!/usr/bin/env python

import datetime
import copy
import json
import threading
import hashlib

import collector
import siteMapping


class NetworkTracerouteCollector(collector.Collector):

    def __init__(self):

        self.TOPIC = "/topic/perfsonar.raw.packet-trace"
        self.INDEX = 'ps_trace_'
        super(NetworkTracerouteCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)

        data = {}

        # print(m)
        source = m['meta']['source']
        destination = m['meta']['destination']
        data['push'] = m['meta']['push'] if 'push' in m['meta'] else False
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
        data['src_production'] = siteMapping.isProductionThroughput(source)
        data['dest_production'] = siteMapping.isProductionThroughput(
            destination)
        if 'datapoints' not in m:
            print(threading.current_thread().name,
                  "no datapoints found in the message")
            return
        dp = m['datapoints']
        # print(su)
        for ts in dp:
            data['_index'] = self.INDEX + \
                datetime.date.fromtimestamp(float(ts)).strftime("%Y-%m")
            data['timestamp'] = int(float(ts) * 1000)
            data['_id'] = self.calculateId(m, data['timestamp'])
            data['hops'] = []
            data['asns'] = []
            data['rtts'] = []
            data['ttls'] = []
            hops = dp[ts]
            for hop in hops:
                if 'ttl' not in hop or 'ip' not in hop or 'query' not in hop:
                    continue
                nq = int(hop['query'])
                if nq != 1:
                    continue
                data['hops'].append(hop['ip'].strip())
                data['ttls'].append(int(hop['ttl']))
                if 'rtt' in hop and hop['rtt'] is not None:
                    data['rtts'].append(float(hop['rtt']))
                else:
                    data['rtts'].append(0.0)
                if 'as' in hop:
                    data['asns'].append(hop['as']['number'])
                else:
                    data['asns'].append(0)
                # print(data)
            data['n_hops'] = len(data['hops'])
            if len(data['rtts']):
                data['max_rtt'] = max(data['rtts'])

            if len(data['hops']) == 0:
                print('ERROR: we should have no data without any hops.')
                self.aLotOfData.append(copy.copy(data))
                continue

            data['destination_reached'] = False
            core_path = copy.copy(data['hops'])
            if core_path[-1] == data['dest']:
                core_path.remove(data['dest'])
                # destination has been reached if the last hop is == destination
                data['destination_reached'] = True

            route_hash = hashlib.sha1()
            route_hash.update(";".join(core_path).encode())
            data['route-sha1'] = route_hash.hexdigest()

            # path incomplete means len(ttls) is not equal to the last ttl
            data['path_complete'] = False
            if len(data['ttls']) == data['ttls'][-1]:
                data['path_complete'] = True

            # looping path contains at least one non-unique IP. it includes src and dest.
            core_path.append(data['src'])
            core_path.append(data['dest'])
            data['looping'] = len(set(core_path)) != len(core_path)

            self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkTracerouteCollector()
    collector.start()


if __name__ == "__main__":
    main()
