#!/usr/bin/env python

import threading
from threading import Thread
import copy
import json

import siteMapping
import collector


class NetworkStatusCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/perfsonar.summary.status"
        self.INDEX = 'ps_status_write'
        super(NetworkStatusCollector, self).__init__()

    def eventCreator(self, message):

        m = json.loads(message)
        data = {}

        metrics = ['perfSONAR services: ntp', 'perfSONAR esmond freshness', 'OSG datastore freshness',
                   'perfSONAR services: pscheduler stats']
        found = False
        for met in metrics:
            if not met in m['metric']:
                continue
            found = True
        if not found:
            return

        if 'perf_metrics' in m.keys() and not m['perf_metrics']:
            return

        data['host'] = m['host']
        prefix = m['metric'].replace("perfSONAR", "ps").replace(":", "").replace(" ", "_").lower()
        for k in m['perf_metrics'].keys():
            data[prefix + "_" + k] = m['perf_metrics'][k]
        data['_index'] = self.INDEX
        data['timestamp'] = int(float(m['timestamp']) * 1000)
        # print(data)
        self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkStatusCollector()
    collector.start()


if __name__ == "__main__":
    main()
