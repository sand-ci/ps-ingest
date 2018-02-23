""" maps ips to sites """

import sys
import socket
import time
import requests

try:
    import simplejson as json
except ImportError:
    import json

ot = 0
meshes = []
PerfSonars = {}
throughputHosts = []
latencyHosts = []


class ps:
    hostname = ''
    sitename = ''
    VO = ''
    ip = ''
    flavor = ''

    def prnt(self):
        print('ip:', self.ip, '\thost:', self.hostname, '\tVO:', self.VO, '\tflavor:', self.flavor)


def getIP(host):
    ip = None
    try:
        ip = socket.getaddrinfo(host, 80, 0, 0, socket.IPPROTO_TCP)
    except:
        print("Could not get ip for", host)
    return ip


def reload():
    print('starting mapping reload')
    global ot
    global throughputHosts
    global latencyHosts

    timeout = 60
    socket.setdefaulttimeout(timeout)

    ot = time.time() - 86300  # in case it does not succeed in updating it will try again in 100 seconds.
    try:
        r = requests.get('http://atlas-agis-api.cern.ch/request/site/query/list/?json&vo_name=atlas&state=ACTIVE')
        res = r.json()
        sites = []
        for s in res:
            sites.append(s["rc_site"])
        # print(res)
        print('Sites reloaded.')
    except:
        print("Could not get sites from AGIS. Exiting...")
        print("Unexpected error: ", str(sys.exc_info()[0]))

    try:
        r = requests.get('http://atlas-agis-api.cern.ch/request/service/query/list/?json&state=ACTIVE&type=PerfSonar')
        res = r.json()
        for s in res:
            p = ps()
            p.hostname = s['endpoint']
            if s['status'] == 'production':
                p.production = True
            p.flavor = s['flavour']
            p.sitename = s['rc_site']
            if p.sitename in sites:
                p.VO = "ATLAS"
            ips = getIP(p.hostname)
            if not ips:
                continue
            p.ip = [i[4][0] for i in ips]
            for ip in ips:
                PerfSonars[ip[4][0]] = p
            sites.append(s["rc_site"])
            p.prnt()
        print('Perfsonars reloaded.')
    except:
        print("Could not get perfsonars from AGIS. Exiting...")
        print("Unexpected error: ", str(sys.exc_info()[0]))

    # loading meshes ===================================

    try:
        r = requests.get('http://meshconfig.grid.iu.edu/pub/config/')
        res = r.json()
        for r in res:
            inc = r['include'][0]
            inc = inc.replace("https://", "http://")
            if not inc.startswith('http://'):
                inc = 'http://' + inc
            meshes.append(inc)
        print('All defined meshes:', meshes)
    except:
        print("Could not load meshes  Exiting...")
        print("Unexpected error: ", str(sys.exc_info()[0]))

    throughputHosts = []
    latencyHosts = []
    for m in meshes:
        print('Loading mesh:', m)
        try:
            r = requests.get(m, verify=False)
            res = r.json()
            for o in res['organizations']:
                for s in o['sites']:
                    for h in s['hosts']:
                        types = []
                        for ma in h['measurement_archives']:
                            if ma['type'].count('owamp') > 0:
                                types.append('owamp')
                            if ma['type'].count('bwctl') > 0:
                                types.append('bwctl')
                        for a in h['addresses']:
                            print(a)
                            ips = getIP(a)
                            if ips and 'bwctl' in types:
                                for ip in ips:
                                    throughputHosts.append(ip[0][4])
                            if ips and 'owamp' in types:
                                for ip in ips:
                                    latencyHosts.append(ip[0][4])
        except:
            print("Could not load mesh,", m, " Exiting...")
            print("Unexpected error: ", str(sys.exc_info()[0]))

    print('throughputHosts reloaded:\n', throughputHosts)
    print('latencyHosts reloaded:\n', latencyHosts)

    print('All done.')
    ot = time.time()  # all updated so the next one will be in one day.


def getPS(ip):
    global ot
    if (time.time() - ot) > 86400:
        print(ot)
        reload()
    if ip in PerfSonars:
        return [PerfSonars[ip].sitename, PerfSonars[ip].VO]


def isProductionLatency(ip):
    if ip in latencyHosts:
        return True
    return False


def isProductionThroughput(ip):
    if ip in throughputHosts:
        return True
    return False
