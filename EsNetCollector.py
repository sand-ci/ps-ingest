#!/usr/bin/env python

import threading
from threading import Thread
import copy
import json
import math
import collector


class EsNetCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/XXX"
        self.INDEX = 'esnet_write'
        super(EsNetCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)
        data = {
        }

        data['_index'] = self.INDEX
        # XXX here get stuff from the message and put it in data
        # data['timestamp'] = int(float(ts) * 1000)
        # data['MA'] = m['meta']

        self.aLotOfData.append(copy.copy(data))


def main():
    collector = EsNetCollector()
    collector.start()


if __name__ == "__main__":
    main()
