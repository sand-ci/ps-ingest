#!/usr/bin/env python

import threading
from threading import Thread
import copy
import json
import math
import collector
import hashlib

class EsNetInterfaceCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/esnet_interfaces"
        self.INDEX = 'esnet_interfaces_write'
        super(EsNetCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)
        data = {
        }

        data['_index'] = self.INDEX
        data['namespace'] = m['namespace']
        data['resource'] = m['resource']
        data['name'] = m['name']
        data['device'] = m['device']
        data['ifIndex'] = m['ifIndex']
        data['description'] = m['description']
        data['speed'] = m['speed']
        data['vlan'] = m['vlan']
        data['port'] = m['port']
        data['nokiaType'] = m['nokiaType']
        data['visibility'] = m['visibility']
        data['connection'] = m['connection']
        data['link'] = m['link']
        data['tags'] = m['tags']
        data['sector'] = m['sector']
        data['site'] = m['site']
        data['lhcone'] = m['lhcone']
        data['oscars'] = m['oscars']
        data['intercloud'] = m['intercloud']
        data['intracloud'] = m['intracloud']
        data['remoteDevice'] = m['remoteDevice']
        data['remotePort'] = m['remotePort']
        data['timestamp'] = m['timestamp']
        sha1_hash = hashlib.sha1()
        data['_id'] = sha1_hash.hexdigest()        

        self.aLotOfData.append(copy.copy(data))


def main():
    collector = EsNetCollector()
    collector.start()


if __name__ == "__main__":
    main()
