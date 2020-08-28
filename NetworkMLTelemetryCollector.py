#!/usr/bin/env python

# NOT USED ANYMORE

import threading
from threading import Thread
import copy
import json
import hashlib

import collector
import siteMapping


class NetworkMLTelementryCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/telemetry.perfsonar"
        self.INDEX = 'ps_telemetry_write'
        super(NetworkMLTelementryCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)
        data = {}

        source = m['meta']['source']
        destination = m['meta']['destination']
        data['MA'] = m['meta']['measurement_agent']
        data['src'] = source
        data['dest'] = destination
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
        if 'summaries' not in m:
            print(threading.current_thread().name, "no summaries found in the message")
            return
        su = m['summaries']
        for s in su:
            if s['summary_window'] == '60' and s['summary_type'] == 'statistics':
                results = s['summary_data']
                # print(results)
                for r in results:
                    data['_index'] = self.INDEX
                    data['timestamp'] = r[0] * 1000
                    data['_id'] = self.calculateId(m, data['timestamp'])
                    data['sim_util'] = r[1]['ml']
            # print(data)
            self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkMLTelementryCollector()
    collector.start()


if __name__ == "__main__":
    main()
