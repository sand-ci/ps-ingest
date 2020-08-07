""" maps ips to sites """

import sys
import socket
import time
import requests
import xml.etree.ElementTree as ET
import ipaddress

# suppress InsecureRequestWarning: Unverified HTTPS request is being made.
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

try:
    import simplejson as json
except ImportError:
    import json

ot = 0
meshes = []
PerfSonars = {}
throughputHosts = []
latencyHosts = []

GOCDB_FEED = "https://goc.egi.eu/gocdbpi/public/?method=get_service_endpoint"
OIM_FEED = "https://my.opensciencegrid.org/rgsummary/xml?summary_attrs_showservice=on&summary_attrs_showfqdn=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&bdiitree_type=total_jobs&bdii_object=service&bdii_server=is-osg&start_type=7daysago&start_date=11%2F17%2F2014&end_type=now&all_resources=on&facility_sel%5B%5D=10009&gridtype=on&gridtype_1=on&active=on&active_value=1&disable_value=0"


class ps:
    hostname = ''
    sitename = ''
    VO = ''
    ip = ''
    flavor = ''

    def prnt(self):
        print('ip:', self.ip, '\thost:', self.hostname, '\tVO:', self.VO,
              '\tflavor:', self.flavor, '\tsite:', self.sitename)


def request(url, hostcert=None, hostkey=None, verify=False):
    if hostcert and hostkey:
        req = requests.get(url, verify=verify, timeout=120,
                           cert=(hostcert, hostkey))
    else:
        req = requests.get(url, timeout=120, verify=verify)
    req.raise_for_status()
    return req.content


def get_gocdb_sonars(response):
    if not response:
        return None

    tree = ET.fromstring(response)
    gocdb_set = set([(x.findtext('HOSTNAME').strip(),
                      x.findtext('SERVICE_TYPE').strip(),
                      x.findtext('SITENAME').strip(),
                      x.findtext('IN_PRODUCTION')) for x in tree.findall('SERVICE_ENDPOINT')])
    gocdb_sonars = set([(host, stype, site) for host, stype, site, state in gocdb_set if
                        (stype == 'net.perfSONAR.Bandwidth' or stype == 'net.perfSONAR.Latency')])
    return gocdb_sonars


def get_oim_sonars(response):
    if not response:
        return None

    tree = ET.fromstring(response)
    oim_resources = list()
    res_groups = tree.findall('ResourceGroup')
    for res in res_groups:
        site = res.findtext('GroupName')
        try:
            oim_resources.extend([(x.findtext('FQDN').strip(),
                                   x.findtext('Services/Service/Name').strip(),
                                   site) for x in res.findall('Resources/Resource')])
            for r in res.findall('Resources/Resource'):
                oim_resources.extend([(x.findtext('Details/endpoint').strip(),
                                       x.findtext('Name').strip(),
                                       site) for x in r.findall('Services/Service')])
        except AttributeError:
            continue
    oim_sonars = set([(host, stype, site) for host, stype, site in oim_resources if
                      stype == 'net.perfSONAR.Bandwidth' or stype == 'net.perfSONAR.Latency'])
    return oim_sonars


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

    # in case it does not succeed in updating it will try again in 100 seconds.
    ot = time.time() - 86300
    try:
        r = requests.get(
            'https://atlas-cric.cern.ch/api/core/site/query/?json&vo_name=atlas&state=ACTIVE', verify=False)
        res = r.json()
        # print('whole json:', res)
        sites = []
        for key, val in res.items():
            sites.append(val["rc_site"])
        # print('Sites reloaded.')
    except:
        print("Could not get sites from CRIC. Exiting...")
        print("Unexpected error: ", str(sys.exc_info()[0]))

    try:
        r = requests.get(
            'https://atlas-cric.cern.ch/api/core/service/query/?json&state=ACTIVE&type=PerfSonar', verify=False)
        res = r.json()
        for key, val in res.items():
            # print(key, val)
            p = ps()
            p.hostname = val['endpoint']
            if val['status'] == 'production':
                p.production = True
            p.flavor = val['flavour']
            p.sitename = val['rcsite']
            if p.sitename in sites:
                p.VO = "ATLAS"
            ips = getIP(p.hostname)
            if not ips:
                continue
            p.ip = [i[4][0] for i in ips]
            for ip in ips:
                if ':' in ip[4][0]:
                    try:
                        PerfSonars[ipaddress.IPv6Address(
                            ip[4][0]).exploded] = p
                    except ipaddress.AddressValueError:
                        print('Failed to parse IPv6 address:', ip)
                        continue
                else:
                    PerfSonars[ip[4][0]] = p
            sites.append(val["rcsite"])
            p.prnt()
        print('Perfsonars reloaded.')
    except:
        print("Could not get perfsonars from CRIC. Exiting...")
        print("Unexpected error: ", str(sys.exc_info()[0]))

    # gocdb/oim processing =============================

    try:
        print("Retrieving GOCDB sonars ...")
        sonars = list(get_gocdb_sonars(request(GOCDB_FEED, verify=False)))
        print("Retrieving OIM sonars ...")
        oim_sonars = list(get_oim_sonars(request(OIM_FEED)))
        sonars.extend(oim_sonars)

        for host, stype, site in sonars:
            ips = getIP(host)
            if not ips:
                continue
            if ips[0][4][0] in PerfSonars.keys():
                continue
            p = ps()
            p.hostname = host
            p.production = False
            p.VO = "UNKNOWN"
            p.flavor = stype
            p.sitename = site
            p.ip = [i[4][0] for i in ips]
            p.prnt()
            for ip in ips:
                if ':' in ip[4][0]:
                    try:
                        PerfSonars[ipaddress.IPv6Address(
                            ip[4][0]).exploded] = p
                    except ipaddress.AddressValueError:
                        print('Failed to parse IPv6 address:', ip)
                        continue
                else:
                    PerfSonars[ip[4][0]] = p
        print('Done')
    except:
        print("Could not get perfSONARs from GOCDB/OIM ...")
        print("Unexpected error: ", str(sys.exc_info()[0]))

    # loading meshes ===================================

    try:
        r = requests.get(
            'http://psconfig.opensciencegrid.org/pub/config/', verify=False, timeout=10)
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
    params = {
        "format": "meshconfig"
    }
    for m in meshes:
        print('Loading mesh:', m)
        try:
            r = requests.get(m, verify=False, params=params, timeout=10)
            res = r.json()
            for o in res['organizations']:
                for s in o['sites']:
                    for h in s['hosts']:
                        types = []
                        if 'measurement_archives' not in h.keys():
                            print("No measurement archive defined for ", h)
                            continue
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
                                    throughputHosts.append(ip[4][0])
                            if ips and 'owamp' in types:
                                for ip in ips:
                                    latencyHosts.append(ip[4][0])
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
    if ':' in ip:
        ip = ipaddress.IPv6Address(ip).exploded
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
