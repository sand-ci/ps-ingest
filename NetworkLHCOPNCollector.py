#!/usr/bin/env python

# TO BE RUN ONLY AT CERN

import threading
from threading import Thread
import copy
import json
import hashlib

import collector


class NetworkLHCOPNCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/netflow.lhcopn"
        self.INDEX = 'ps_lhcopn_write'
        super(NetworkLHCOPNCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)
        data = {
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
        data['_index'] = self.INDEX
        data['timestamp'] = int(float(ts) * 1000)
        data['_id'] = self.calculateId(m, str(data['timestamp']))
        data['utilization'] = int(th)
        # print(data)
        self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkLHCOPNCollector()
    collector.start()


if __name__ == "__main__":
    main()
