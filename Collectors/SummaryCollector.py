#!/usr/bin/env python3
"""
A module for processing the summary packet data format from Xrootd.

For information about the packet formatting, see:

   http://xrootd.org/doc/dev44/xrd_monitoring.htm
"""

import collections
import json
import os
import socket
import sys
import time
from datetime import datetime
from xml.parsers.expat import ExpatError

import requests
import xmltodict

import UdpCollector
import decoding


class ProcessState(object):
    """
    Object representing the last known state of a process on a host.
    """

    def __init__(self):
        self.pid = -1
        self.tod = 0  #this will be used to check if the packet came out of order.
        #self.link_num  =0 # Current connections.
        #self.link_maxn =0 # Maximum number of simultaneous connections. it is cumulative but not interesting for ES.
        self.link_total = 0 # Connections since startup
        self.link_in    = 0 # Bytes received.
        self.link_out   = 0 # Bytes sent
        self.link_ctime = 0 # Cumulative number of connect seconds. ctime/tot gives the average session time per connection
        self.link_tmo   = 0 # timouts
        # self.link_stall=0 # Number of times partial data was received.
        # self.link_sfps =0 # Partial sendfile() operations.
        self.proc_usr    = 0
        self.proc_sys    = 0
        self.xrootd_err  = 0
        self.xrootd_dly  = 0
        self.xrootd_rdr  = 0
        self.ops_open    = 0
        self.ops_preread = 0
        self.ops_read    = 0
        self.ops_readv   = 0
        self.ops_sync    = 0
        self.ops_write   = 0
        self.lgn_num     = 0
        self.lgn_af      = 0
        self.lgn_au      = 0
        self.lgn_ua      = 0


    def prnt(self):
        logger.info("pid: {} \ttotal: {} \tin: {} \tout: {} \tctime: {} \ttmo: {}".format(
                    self.pid, self.link_total, self.link_in, self.link_out,
                    self.link_ctime, self.link_tmo))


def setDiff(attr_name, json_data, currState, prevState):
    cur_val = getattr(currState, attr_name)
    prev_val = getattr(prevState, attr_name)
    json_data[attr_name] = cur_val - prev_val
    if json_data[attr_name] < 0:
        json_data[attr_name] = cur_val


class SummaryCollector(UdpCollector.UdpCollector):

    DEFAULT_PORT = 9931


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._states = collections.defaultdict(lambda: collections.defaultdict(ProcessState))


    def process(self, data, addr, port):

        summary = {}
        try:
            summary = xmltodict.parse(data)
        except ExpatError:
            self.logger.error("Could not parse received data: %s", d)
            return
        except:
            self.logger.exception("Unexpected error. Original data was: %s", d)
            return

        currState = ProcessState()

        statistics = summary['statistics'] # top level
        pgm = statistics['@pgm'] # program name
        self.logger.debug("Program: %s", pgm)
        if pgm != 'xrootd':
            self.logger.warning("Program: %s should not be sending summary information. Source: %s", pgm, s['@src'])
            return

        tos = int(statistics['@tos'])  # Unix time when the program was started.
        tod = int(statistics['@tod'])  # Unix time when statistics gathering started.
        pid = int(statistics['@pid'])

        currState.pid = pid
        currState.tod = tod

        rmq_data = {}
        rmq_data['server_ip'] = addr
        rmq_data['type'] = 'summary'
        rmq_data['pid'] = pid
        rmq_data['timestamp'] = datetime.utcfromtimestamp(float(tod)).isoformat()
        rmq_data['tos'] = datetime.utcfromtimestamp(float(tos)).isoformat()
        rmq_data['cstart'] = datetime.utcfromtimestamp(float(tod)).isoformat()
        rmq_data['version']  = statistics['@ver'] # version name of the servers 

        if '@site' in statistics:
            rmq_data['site'] = statistics['@site'] # site name specified in the configuration
        else:
            log.debug('Server {} has no site name defined!'.format(addr))
            rmq_data['site'] = 'UnknownSite'
            self.publish('summary', rmq_data)
            return

        hasPrev = True
        if addr not in self._states:
            self.logger.info("New host detected: {}".format(addr))
        pids = self._states[addr]
        if pid not in pids:
            self.logger.info("New PID ({}) detect for host {}.".format(pid, addr))
            hasPrev = False
        previousState = pids[pid]

        stats = statistics['stats']
        for st in stats:
            sw = st['@id']
            if sw == 'info':
                rmq_data['host'] = st['host']
                rmq_data['location'] = decoding.getLongLat(addr)
            elif sw == 'link':
                rmq_data['link_num']     = int(st['num']) # not cumulative
                currState.link_total = int(st['tot'])
                currState.link_in    = int(st['in'])
                currState.link_out   = int(st['out'])
                currState.link_ctime = int(st['ctime'])
                currState.link_tmo   = int(st['tmo'])
            elif sw == 'proc':
                currState.proc_sys = int(st['sys']['s'])
                currState.proc_usr = int(st['usr']['s'])
                self.logger.debug("proc %s", st)
            elif sw == 'xrootd':
                currState.xrootd_err = int(st['err'])
                currState.xrootd_dly = int(st['dly'])
                currState.xrootd_rdr = int(st['rdr'])
                ops = st['ops']
                currState.ops_open    = int(ops['open'])
                currState.ops_preread = int(ops['pr'])
                currState.ops_read    = int(ops['rd'])
                currState.ops_readv   = int(ops['rv'])
                currState.ops_sync    = int(ops['sync'])
                currState.ops_write   = int(ops['wr'])
                lgn = st['lgn']
                currState.lgn_num = int(lgn['num'])
                currState.lgn_af  = int(lgn['af'])
                currState.lgn_au  = int(lgn['au'])
                currState.lgn_ua  = int(lgn['ua'])
                self.logger.debug("xrootd %s", st)
            elif sw=='sched':
                rmq_data['sched_in_queue']  = int(st['inq'])
                rmq_data['sched_threads']  = int(st['threads'])
                rmq_data['sched_idle_threads']  = int(st['idle'])
                self.logger.debug("sched %s", st)
            elif sw=='sgen':
                rmq_data['sgen_as']  = int(st['as'])
                # data['sgen_et']  = int(st['et']) # always 0
                rmq_data['cend'] = datetime.utcfromtimestamp(float(st['toe'])).isoformat()
            elif sw=='ofs':
                pass
                # TODO: fixup this information
                #print 'ofs    >>>',st

        if hasPrev:
            if currState.tod < previousState.tod:
                self.logger.warning("UDP packet came out of order; skipping the message.")
                return

            for attr in ['link_total', 'link_in', 'link_out', 'link_ctime', 'link_tmo',
                         'proc_usr', 'proc_sys', 'ops_open', 'ops_preread', 'ops_read',
                         'ops_readv', 'ops_sync', 'ops_write']:
                setDiff(attr, rmq_data, currState, previousState)

            # data['link_stall'] = currState.link_stall - previousState.link_stall
            # data['link_sfps']  = currState.link_sfps  - previousState.link_sfps

            rmq_data['xrootd_errors'] = currState.xrootd_err - previousState.xrootd_err # these should not overflow
            rmq_data['xrootd_delays'] = currState.xrootd_dly - previousState.xrootd_dly 
            rmq_data['xrootd_redirections'] = currState.xrootd_rdr - previousState.xrootd_rdr 
            rmq_data['login_attempts'] = currState.lgn_num - previousState.lgn_num
            rmq_data['authentication_failures'] = currState.lgn_af - previousState.lgn_af
            rmq_data['authentication_successes'] = currState.lgn_au - previousState.lgn_au
            rmq_data['unauthenticated_successes'] = currState.lgn_ua - previousState.lgn_ua
            self.publish('summary', rmq_data)

        self._states[addr][pid] = currState


if __name__ == '__main__':
    SummaryCollector.main()
