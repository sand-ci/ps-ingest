#!/usr/bin/env python

import threading
from threading import Thread
import copy
import json
import math
import collector


class EsNetInterfaceCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/esnet_interface"
        self.INDEX = 'esnet_interface_write'
        super(EsNetCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)
        data = {
        }

        data['_index'] = self.INDEX
        # XXX here get stuff from the message and put it in data
        # data['timestamp'] = int(float(ts) * 1000)
        # data['MA'] = m['meta']
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

        self.aLotOfData.append(copy.copy(data))


def main():
    collector = EsNetCollector()
    collector.start()


if __name__ == "__main__":
    main()
