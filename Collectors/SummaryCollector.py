#!/usr/bin/env python3
"""
A module for processing the summary packet data format from Xrootd.

For information about the packet formatting, see:

   http://xrootd.org/doc/dev44/xrd_monitoring.htm
"""

import collections
from datetime import datetime
from xml.parsers.expat import ExpatError

import xmltodict

import UdpCollector
import decoding


class ProcessState(object):
    """
    Object representing the last known state of a process on a host.
    """

    def __init__(self):
        self.pid = -1
        self.tod = 0  # this will be used to check if the packet came out of order.
        # self.link_num  =0 # Current connections.
        # self.link_maxn =0 # Maximum number of simultaneous connections. it is cumulative but not interesting for ES.
        self.link_total = 0  # Connections since startup
        self.link_in    = 0  # Bytes received.
        self.link_out   = 0  # Bytes sent
        self.link_ctime = 0  # Cumulative number of connect seconds. ctime/tot gives the average session time per connection
        self.link_tmo   = 0  # timouts
        # self.link_stall=0  # Number of times partial data was received.
        # self.link_sfps =0  # Partial sendfile() operations.
        self.proc_usr    = 0
        self.proc_sys    = 0
        self.xrootd_num  = 0
        self.xrootd_err  = 0
        self.xrootd_dly  = 0
        self.xrootd_rdr  = 0
        self.ops_open    = 0
        self.ops_preread = 0
        self.ops_read    = 0
        self.ops_readv   = 0
        self.ops_sync    = 0
        self.ops_write   = 0
        self.ops_getf    = 0
        self.ops_misc    = 0
        self.ops_rf      = 0
        self.ops_rs      = 0
        self.lgn_num     = 0
        self.lgn_af      = 0
        self.lgn_au      = 0
        self.lgn_ua      = 0
        self.aio_max     = 0
        self.aio_num     = 0
        self.aio_rej     = 0


    def prnt(self):
        self.logger.info("pid: {} \ttotal: {} \tin: {} \tout: {} \tctime: {} \ttmo: {}".format(
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
    UDP_MON_PORT = 8001

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._states = collections.defaultdict(lambda: collections.defaultdict(ProcessState))

        self.report_raw_data = not bool(self.config.get('AMQP', 'process_metrics'))


    def process(self, data, addr, port):

        summary = {}
        try:
            summary = xmltodict.parse(data)
        except ExpatError:
            self.logger.error("Could not parse received data: %s", data)
            return
        except:
            self.logger.exception("Unexpected error. Original data was: %s", data)
            return

        self.logger.debug("Summary Data:\n{}".format(data))
        self.logger.debug("Summary XML:\n{}".format(summary))

        currState = ProcessState()

        statistics = summary['statistics']  # top level
        pgm = statistics['@pgm']  # program name
        self.logger.debug("Program: %s", pgm)
        if pgm != 'xrootd':
            self.logger.warning("Program: %s should not be sending summary information. Source: %s", pgm, statistics['@src'])
            #return

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
        rmq_data['version'] = statistics['@ver']  # version name of the servers

        if '@site' in statistics:
            rmq_data['site'] = statistics['@site']  # site name specified in the configuration
        else:
            self.logger.debug('Server {} has no site name defined!'.format(addr))
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
            elif sw == 'link':
                rmq_data['link_num'] = int(st['num'])  # not cumulative
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
                currState.xrootd_num = int(st['num'])
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
                currState.ops_getf    = int(ops['getf'])
                currState.ops_misc    = int(ops['misc'])
                currState.ops_rf      = int(ops['rf'])
                currState.ops_rs      = int(ops['rs'])
                lgn = st['lgn']
                currState.lgn_num = int(lgn['num'])
                currState.lgn_af  = int(lgn['af'])
                currState.lgn_au  = int(lgn['au'])
                currState.lgn_ua  = int(lgn['ua'])
                aio = st['aio']
                currState.aio_max = int(aio['max'])
                currState.aio_num = int(aio['num'])
                currState.aio_rej = int(aio['rej'])
                self.logger.debug("xrootd %s", st)
            elif sw == 'sched':
                rmq_data['sched_in_queue']  = int(st['inq'])
                rmq_data['sched_threads']  = int(st['threads'])
                rmq_data['sched_idle_threads']  = int(st['idle'])
                self.logger.debug("sched %s", st)
            elif sw == 'sgen':
                rmq_data['sgen_as']  = int(st['as'])
                # data['sgen_et']  = int(st['et']) # always 0
                rmq_data['cend'] = datetime.utcfromtimestamp(float(st['toe'])).isoformat()
            elif sw == 'cache':
                self.logger.debug('Cache Data: {}'.format(str(st)))
                self.logger.debug('Cache Opened: {}'.format(st['files']['opened']))

                rmq_data['cache_type']=            st['@type']
                rmq_data['cache_preread_in']=      int(st['prerd']['in'])
                rmq_data['cache_preread_hits']=    int(st['prerd']['hits'])
                rmq_data['cache_preread_miss']=    int(st['prerd']['miss'])
                rmq_data['cache_read_in']=         int(st['rd']['in'])
                rmq_data['cache_read_out']=        int(st['rd']['out'])
                if st['rd']['hits'][-1] == '>':
                    # Fix annoying bug where XRootD 5.0.3 at least returns a non STR object here...
                    # BAD XROOTD SERVER, BAD!!!
                    st['rd']['hits'] = st['rd']['hits'][:-1]
                rmq_data['cache_read_hits']=       int(st['rd']['hits'])
                rmq_data['cache_read_miss']=       int(st['rd']['miss'])
                rmq_data['cache_read_nocache']=    int(st['pass']['#text'])
                rmq_data['cache_read_bypass']=     int(st['pass']['cnt'])
                rmq_data['cache_write_out']=       int(st['wr']['out'])
                rmq_data['cache_write_in']=        int(st['wr']['updt'])
                rmq_data['cache_write_frommem']=   int(st['saved'])
                rmq_data['cache_purge']=           int(st['purge'])
                rmq_data['cache_count_open']=      int(st['files']['opened'])
                rmq_data['cache_count_closed']=    int(st['files']['closed'])
                rmq_data['cache_count_new']=       int(st['files']['new'])
                rmq_data['cache_size']=            int(st['store']['size'])
                rmq_data['cache_used']=            int(st['store']['used'])
                rmq_data['cache_minuse']=          int(st['store']['min'])
                rmq_data['cache_maxuse']=          int(st['store']['max'])
                rmq_data['cache_mem_size']=        int(st['mem']['size'])
                rmq_data['cache_mem_used']=        int(st['mem']['used'])
                rmq_data['cache_mem_queued']=      int(st['mem']['wq'])
                rmq_data['cache_open_defertotal']= int(st['opcl']['odefer'])
                rmq_data['cache_open_deferopen']=  int(st['opcl']['defero'])
                rmq_data['cache_close_defer']=     int(st['opcl']['cdefer'])
                rmq_data['cache_close_deferopen']= int(st['opcl']['clost'])

            elif sw == 'ofs':

                rmq_data['ofs_role']=             st['role']
                rmq_data['ofs_bkgtask']=          int(st['bxq'])
                rmq_data['ofs_delay']=            int(st['dly'])
                rmq_data['ofs_errors']=           int(st['err'])
                rmq_data['ofs_handles']=          int(st['han'])
                rmq_data['ofs_count_posc']=       int(st['opp'])
                rmq_data['ofs_count_read']=       int(st['opr'])
                rmq_data['ofs_count_rw']=         int(st['opw'])
                rmq_data['ofs_count_redir']=      int(st['rdr'])
                rmq_data['ofs_count_reply']=      int(st['rep'])
                rmq_data['ofs_count_fail']=       int(st['ser'])
                rmq_data['ofs_count_succ']=       int(st['sok'])
                rmq_data['ofs_count_unposc']=     int(st['ups'])
                rmq_data['ofs_count_tpc_allow']=  int(st['tpc']['grnt'])
                rmq_data['ofs_count_tpc_deny']=   int(st['tpc']['deny'])
                rmq_data['ofs_count_tpc_error']=  int(st['tpc']['err'])
                rmq_data['ofs_count_tpc_expire']= int(st['tpc']['exp'])

                #pass
                # TODO: fixup this information
                # print 'ofs    >>>',st

            elif sw == "buff":

                rmq_data['buff_count_adj']=   int(st['adj'])
                rmq_data['buff_count_alloc']= int(st['buffs'])
                rmq_data['buff_mem_size']=    int(st['mem'])
                rmq_data['buff_count_reqs']=  int(st['reqs'])

            elif sw == "poll":

                rmq_data['poll_num_desc']=     int(st['att'])
                rmq_data['poll_count_enops']=  int(st['en'])
                rmq_data['poll_count_events']= int(st['ev'])
                rmq_data['poll_count_badevt']= int(st['int'])

            elif sw == "proc":

                rmq_data['proc_systimesec']=  int(st['sys']['s'])
                rmq_data['proc_systimeusec']= int(st['sys']['us'])
                rmq_data['proc_usrtimesec']=  int(st['usr']['s'])
                rmq_data['prox_usrtimeusec']= int(st['usr']['us'])

            elif sw == "oss":

                oss_path_num = int(getattr(st['paths'], '#text', '0'))
                oss_space_num = int(getattr(st['space'], '#text', '0'))

                rmq_data['oss_num_paths']=  oss_path_num
                rmq_data['oss_num_spaces']= oss_space_num

                i=0
                for _path in getattr(st['paths'], 'stats', []):
                    rmq_data['oss_path_'+str(i)+'_total_size']=   int(_path['tot'])
                    rmq_data['oss_path_'+str(i)+'_total_inode']=  int(_path['ino'])
                    rmq_data['oss_path_'+str(i)+'_free_size']=    int(_path['free'])
                    rmq_data['oss_path_'+str(i)+'_free_inode']=   int(_path['ifr'])
                    rmq_data['oss_path_'+str(i)+'_path_logical']= _path['lp']
                    rmq_data['oss_path_'+str(i)+'_path_real']=    _path['rp']
                    i=i+1

                i=0
                for _path in getattr(st['space'], 'stats', []):
                    rmq_data['oss_space_'+str(i)+'_total_alloc']= int(_path['tot'])
                    rmq_data['oss_space_'+str(i)+'_total_quota']= int(_path['qta'])
                    rmq_data['oss_space_'+str(i)+'_max_extant']=  int(_path['maxf'])
                    rmq_data['oss_space_'+str(i)+'_num_fsn']=     int(_path['fsn'])
                    rmq_data['oss_space_'+str(i)+'_free']=        int(_path['free'])
                    rmq_data['oss_space_'+str(i)+'_name_usg']=    int(_path['usg'])
                    rmq_data['oss_space_'+str(i)+'_name']=        _path['name']
                    i=i+1

            elif sw == "cmsm":

                rmq_data['cmsm_role'] = st['role']
                rmq_data['cmsm_sel_count_t'] = st['sel']['t']
                rmq_data['cmsm_sel_count_r'] = st['sel']['r']
                rmq_data['cmsm_sel_count_w'] = st['sel']['w']

                if 'paths' not in st:
                    rmq_data['cmsm_num_nodes'] = 0
                    continue

                num_nodes = int(getattr(st['paths'], '#text', '0'))
                rmq_data['cmsm_num_nodes'] = num_nodes
                
                i=0
                for i in range(0, num_nodes):
                    rmq_data['cmsm_'+str(i)+'_host']= st['host']
                    rmq_data['cmsm_'+str(i)+'_role']= st['role']
                    rmq_data['cmsm_'+str(i)+'_run_status']= st['run']
                    if 'ref' in st:
                        rmq_data['cmsm_'+str(i)+'_count_ref_r']= int(st['ref']['r'])
                        rmq_data['cmsm_'+str(i)+'_count_ref_w']= int(st['ref']['w'])
                    if 'shr' in st:
                        rmq_data['cmsm_'+str(i)+'_count_shr_req']= int(getattr(st['shr'], '#text', 0))
                        rmq_data['cmsm_'+str(i)+'_count_shr_exh']= int(getattr(st['shr'], 'use', 0))
                    i=i+1

                if 'frq' in st:
                    self.logger.warning("Need Parser for: cmsm.frq !")

            elif sw == "pss":

                rmq_data['pss_count_open']=      int(st['open']['#text'])
                rmq_data['pss_count_open_err']=  int(st['open']['errs'])
                rmq_data['pss_count_close']=     int(st['close']['#text'])
                rmq_data['pss_count_close_err']= int(st['close']['errs'])

            elif sw == "dpmoss":

                # Site is using DPM... not much to see here as of DPM 1.14.3...
                pass

            else:
                self.logger.warning("Need Parser for: {}".format(str(sw)))

        if hasPrev and not self.report_raw_data:
            if currState.tod < previousState.tod:
                self.logger.warning("UDP packet came out of order; skipping the message.")
                return

            for attr in ['link_total', 'link_in', 'link_out', 'link_ctime', 'link_tmo',
                         'proc_usr', 'proc_sys', 'ops_open', 'ops_preread', 'ops_read',
                         'ops_readv', 'ops_sync', 'ops_write', 'aio_max', 'aio_num', 'aio_rej',
                         'ops_getf', 'ops_rf', 'ops_rs', 'ops_misc']:
                setDiff(attr, rmq_data, currState, previousState)

            # data['link_stall'] = currState.link_stall - previousState.link_stall
            # data['link_sfps']  = currState.link_sfps  - previousState.link_sfps

            rmq_data['xrootd_errors'] = currState.xrootd_err - previousState.xrootd_err  # these should not overflow
            rmq_data['xrootd_delays'] = currState.xrootd_dly - previousState.xrootd_dly
            rmq_data['xrootd_redirections'] = currState.xrootd_rdr - previousState.xrootd_rdr
            rmq_data['login_attempts'] = currState.lgn_num - previousState.lgn_num
            rmq_data['authentication_failures'] = currState.lgn_af - previousState.lgn_af
            rmq_data['authentication_successes'] = currState.lgn_au - previousState.lgn_au
            rmq_data['unauthenticated_successes'] = currState.lgn_ua - previousState.lgn_ua
            self.publish('summary', rmq_data)

        elif self.report_raw_data:

            for attr in ['link_total', 'link_in', 'link_out', 'link_ctime', 'link_tmo',
                         'proc_usr', 'proc_sys', 'ops_open', 'ops_preread', 'ops_read',
                         'ops_readv', 'ops_sync', 'ops_write', 'aio_max', 'aio_num', 'aio_rej',
                         'ops_getf', 'ops_rf', 'ops_rs', 'ops_misc']:
                rmq_data[attr] = getattr(currState, attr)

            rmq_data['xrootd_errors'] = currState.xrootd_err
            rmq_data['xrootd_delays'] = currState.xrootd_dly
            rmq_data['xrootd_redirections'] = currState.xrootd_rdr
            rmq_data['login_attempts'] = currState.lgn_num
            rmq_data['authentication_failures'] = currState.lgn_af
            rmq_data['authentication_successes'] = currState.lgn_au
            rmq_data['unauthenticated_successes'] = currState.lgn_ua
            self.publish('summary', rmq_data)

        self._states[addr][pid] = currState


if __name__ == '__main__':
    SummaryCollector.main()

