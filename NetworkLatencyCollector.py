#!/usr/bin/env python

# import threading
# from threading import Thread
import copy
import json
import math
# import hashlib

import siteMapping
import collector


class NetworkLatencyCollector(collector.Collector):

    def __init__(self):
        self.TOPIC = "/topic/perfsonar.raw.histogram-owdelay"
        self.INDEX = 'ps_owd_write'
        super(NetworkLatencyCollector, self).__init__()

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

            if len(th) == 0:
                print('empty measurement.')
                print(data)
                continue

            data['_index'] = self.INDEX
            data['timestamp'] = int(float(ts) * 1000)
            data['_id'] = self.calculateId(m, data['timestamp'])

            th_fl = dict((float(k), v) for (k, v) in th.items())

            # mean
            samples = sum([v for k, v in th_fl.items()])
            th_mean = sum(k * v for k, v in th_fl.items()) / samples
            data['delay_mean'] = th_mean
            # std dev
            data['delay_sd'] = math.sqrt(
                sum((k - th_mean) ** 2 * v for k, v in th_fl.items()) / samples)
            # median
            csum = 0
            ordered_th = [(k, v) for k, v in sorted(th_fl.items())]
            midpoint = samples // 2
            if samples % 2 == 0:  # even number of samples
                for index, entry in enumerate(ordered_th):
                    csum += entry[1]
                    if csum > midpoint + 1:
                        data['delay_median'] = entry[0]
                        break
                    elif csum == midpoint:
                        data['delay_median'] = entry[0] + ordered_th[index + 1][0] / 2
                        break
                    elif csum == midpoint + 1 and index == 0:
                        data['delay_median'] = entry[0]
                        break
                    elif csum == midpoint + 1 and index > 0:
                        data['delay_median'] = entry[0] + ordered_th[index - 1][0] / 2
                        break
            else:  # odd number of samples
                for index, entry in enumerate(ordered_th):
                    csum += entry[1]
                    if csum >= midpoint + 1:
                        data['delay_median'] = entry[0]
                        break
            self.aLotOfData.append(copy.copy(data))


def main():
    collector = NetworkLatencyCollector()
    collector.start()


if __name__ == "__main__":
    main()
