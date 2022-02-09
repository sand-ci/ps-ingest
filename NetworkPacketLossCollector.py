#!/usr/bin/env python

# import threading
# from threading import Thread
import copy
import json
# import hashlib

import siteMapping
import collector


class NetworkPacketLossCollector(collector.Collector):

    def __init__(self):

        self.TOPIC = "/topic/perfsonar.raw.packet-loss-rate"
        self.INDEX = 'ps_packetloss_write'
        super(NetworkPacketLossCollector, self).__init__()

    def eventCreator(self, message):
        m = json.loads(message)

        data = {
        }

        try:
            source = m['meta']['source']
            destination = m['meta']['destination']
            data['push'] = m['meta']['push'] if 'push' in m['meta'] else False
            data['MA'] = m['meta']['measurement_agent']
            data['src_host'] = m['meta']['input_source']
            data['dest_host'] = m['meta']['input_destination']
            su = m['datapoints']
        except KeyError as e:
            print('an important field is missing:', e.args[0])
            print('full message:', m)
            return

        data['src'] = source
        data['dest'] = destination
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
        for ts, th in su.items():
            data['_index'] = self.INDEX
            data['timestamp'] = int(float(ts) * 1000)
            data['_id'] = self.calculateId(m, data['timestamp'])
            data['packet_loss'] = th
            # print(data)
            self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkPacketLossCollector()
    collector.start()


if __name__ == "__main__":
    main()
