#!/usr/bin/env python

import threading
from threading import Thread
import copy
import json
import math
import collector
import hashlib

class EsNetTrafficCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/esnet_traffic"
        self.INDEX = 'esnet_traffic_write'
        super(EsNetCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)
        data = {
        }

        data['_index'] = self.INDEX
        # XXX here get stuff from the message and put it in data
        # data['timestamp'] = int(float(ts) * 1000)
        # data['MA'] = m['meta']
        data['name'] = m['name']
        data['recordType'] = m['recordType']
        data['timestamp'] = m['timestamp']
        data['in'] = m['in']
        data['out'] = m['out']
        sha1_hash = hashlib.sha1()
        sha1_hash.update(str(data['name']).encode())
        sha1_hash.update(str(data['recordType']).encode())
        sha1_hash.update(str(data['timestamp']).encode())
        data['_id'] = sha1_hash.hexdigest()       
 
        self.aLotOfData.append(copy.copy(data))


def main():
    collector = EsNetCollector()
    collector.start()


if __name__ == "__main__":
    main()
