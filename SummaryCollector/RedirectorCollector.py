#!/usr/bin/env python

import decoding

import xmltodict
from xml.parsers.expat import ExpatError

import Queue, os, sys, time
import threading
from threading import Thread
import socket, requests

import json
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers


import logging
import logging.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('RedirectorCollector')


#hostIP="192.170.227.128"
hostIP=socket.gethostbyname(socket.gethostname())

SUMMARY_PORT = 9932

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
sock.bind((hostIP, SUMMARY_PORT))

def RefreshConnection():
    global es
    global lastReconnectionTime
    if ( time.time()-lastReconnectionTime < 60 ):
        return
    lastReconnectionTime=time.time()
    logger.info('make sure we are connected right...')
    res = requests.get('http://uct2-es-door.mwt2.org:9200')
    logger.info(res.content)
    es = Elasticsearch([{'host':'uct2-es-door.mwt2.org', 'port':9200}])


class state:
    def __init__(self):
        self.pid = -1
        self.tod = 0  #this will be used to check if the packet came out of order.
        #self.link_num  =0 # Current connections.
        #self.link_maxn =0 # Maximum number of simultaneous connections. it is cumulative but not interesting for ES.
        self.link_total=0 # Connections since startup
        self.link_in   =0 # Bytes received.
        self.link_out  =0 # Bytes sent
        self.link_ctime=0 # Cumulative number of connect seconds. ctime/tot gives the average session time per connection
        self.link_tmo  =0 # timouts
        # self.link_stall=0 # Number of times partial data was received.
        # self.link_sfps =0 # Partial sendfile() operations.
        self.proc_usr  = 0
        self.proc_sys  = 0
        self.xrootd_err = 0
        self.xrootd_dly = 0
        self.xrootd_rdr = 0
        self.ops_open = 0
        self.ops_pr   = 0
        self.ops_rd   = 0
        self.ops_rv   = 0
        self.ops_sync = 0
        self.ops_wr   = 0
        self.lgn_num  = 0
        self.lgn_af   = 0
        self.lgn_au   = 0
        self.lgn_ua   = 0
        
        
    def prnt(self):
        logger.info("pid: %i \ttotal: %i \tin: %i \tout: %i \tctime: %i \ttmo: %i",self.pid, self.link_total, self.link_in, self.link_out,self.link_ctime, self.link_tmo)
        #print "pid:",self.pid, "\ttotal:",self.link_total, "\tin:",self.link_in, "\tout:",self.link_out, "\tctime:",self.link_ctime, "\ttmo:",self.link_tmo
        
#([(u'@id', u'info'), (u'host', u'ceph36'), (u'port', u'1094'), (u'name', u'anon')])
#([(u'@id', u'link'), (u'num', u'0'), (u'maxn', u'0'), (u'tot', u'0'), (u'in', u'0'), (u'out', u'0'), (u'ctime', u'0'), (u'tmo', u'0'), (u'stall', u'0'), (u'sfps', u'0')])
#([(u'@id', u'proc'), (u'usr', ([(u's', u'0'), (u'u', u'46467')])), (u'sys', ([(u's', u'0'), (u'u', u'38248')]))])
#([(u'@id', u'xrootd'), (u'num', u'0'), (u'ops', ([(u'open', u'0'), (u'rf', u'0'), (u'rd', u'0'), (u'pr', u'0'), (u'rv', u'0'), (u'rs', u'0'), (u'wr', u'0'), (u'sync', u'0'), (u'getf', u'0'), (u'putf', u'0'), (u'misc', u'0')])), (u'aio', ([(u'num', u'0'), (u'max', u'0'), (u'rej', u'0')])), (u'err', u'0'), (u'rdr', u'0'), (u'dly', u'0'), (u'lgn', ([(u'num', u'0'), (u'af', u'0'), (u'au', u'0'), (u'ua', u'0')]))])
#([(u'@id', u'ofs'), (u'role', u'server'), (u'opr', u'0'), (u'opw', u'0'), (u'opp', u'0'), (u'ups', u'0'), (u'han', u'0'), (u'rdr', u'0'), (u'bxq', u'0'), (u'rep', u'0'), (u'err', u'0'), (u'dly', u'0'), (u'sok', u'0'), (u'ser', u'0'), (u'tpc', ([(u'grnt', u'0'), (u'deny', u'0'), (u'err', u'0'), (u'exp', u'0')]))])
#([(u'@id', u'sched'), (u'jobs', u'2201'), (u'inq', u'0'), (u'maxinq', u'3'), (u'threads', u'5'), (u'idle', u'3'), (u'tcr', u'5'), (u'tde', u'0'), (u'tlimr', u'0')])
#([(u'@id', u'sgen'), (u'as', u'0'), (u'et', u'0'), (u'toe', u'1457470123')])

AllState={}

def eventCreator():
    aLotOfData=[]
    while(True):
        [d,addr]=q.get()
        m={}
        try:
            m=xmltodict.parse(d)
        except ExpatError:
            logger.error ("could not parse: %s", d)
            q.task_done()
            continue
        except:
            logger.error ("unexpected error. messsage was: %s", d)
            print sys.exc_info()[0]
            q.task_done()
            continue
            
        d = datetime.now()
        ind="xrd_redirectors-"+str(d.year)+"."+str(d.month)
        data = {
            '_index': ind,
            '_type': 'summary',
            'IP':addr
        }

        previousState=state()
        currState=state()   
                 
        # print m
        s=m['statistics'] # top level
        pgm         = s['@pgm'] # program name
        logger.debug("Program: %s", pgm)
        if (pgm != 'xrootd'):
            logger.debug("Program: %s should not be sending summary information. Source: %s", pgm, s['@src'])
            q.task_done()
            continue
            
        tos         = int(s['@tos'])  # Unix time when the program was started.
        tod         = int(s['@tod'])  # Unix time when statistics gathering started.
        pid         = int(s['@pid'])

        currState.pid = pid
        currState.tod = tod
        data['pid'] = pid
        data['timestamp'] = datetime.utcfromtimestamp(float(tod)).isoformat()
        data['tos'] = datetime.utcfromtimestamp(float(tos)).isoformat()
        data['cstart'] = datetime.utcfromtimestamp(float(tod)).isoformat()
        data['version']  = s['@ver'] # version name of the servers 
        data['site'] = s['@site'] # site name specified in the configuration
        

        hasPrev=False
        if (addr in AllState):
            pids=AllState[addr]
            if (pid in pids):
                hasPrev=True
                previousState=AllState[addr][pid]
                #print "Previous ----"
                #AllState[addr][pid].prnt()
                #print "IP has previous values."
            else:
                logger.warning("seen this IP (%s) before, but not PID (%i).", addr, pid)
        else:
            logger.info("new IP: %s", addr)
        
        stats=s['stats']
        for st in stats:
            sw=st['@id']
            if sw=='info':
                # print 'host >>>', st
                data['host']=st['host']
                data['location'] = decoding.getLongLat(addr)
            elif sw=='link':
                data['link_num']     = int(st['num']) # not cumulative
                currState.link_total = int(st['tot'])
                currState.link_in    = int(st['in'])
                currState.link_out   = int(st['out'])
                currState.link_ctime = int(st['ctime'])
                currState.link_tmo   = int(st['tmo'])
                # currState.link_stall = int(st['stall'])
                # currState.link_sfps  = int(st['sfps'])
            elif sw=='proc':
                currState.proc_sys = int(st['sys']['s'])
                currState.proc_usr = int(st['usr']['s'])
                logger.debug("proc %s", st)
            elif sw=='xrootd':
                currState.xrootd_err = int(st['err'])
                currState.xrootd_dly = int(st['dly'])
                currState.xrootd_rdr = int(st['rdr'])
                ops=st['ops']
                currState.ops_open = int(ops['open'])
                currState.ops_pr   = int(ops['pr'])
                currState.ops_rd   = int(ops['rd'])
                currState.ops_rv   = int(ops['rv'])
                currState.ops_sync = int(ops['sync'])
                currState.ops_wr   = int(ops['wr'])
                lgn=st['lgn']
                currState.lgn_num = int(lgn['num'])
                currState.lgn_af  = int(lgn['af'])
                currState.lgn_au  = int(lgn['au'])
                currState.lgn_ua  = int(lgn['ua'])
                logger.debug("xrootd %s", st)
            elif sw=='sched':
                data['sched_in_queue']  = int(st['inq'])
                data['sched_threads']  = int(st['threads'])
                data['sched_idle_threads']  = int(st['idle'])
                logger.debug("sched %s", st)
            elif sw=='sgen':
                data['sgen_as']  = int(st['as'])
                # data['sgen_et']  = int(st['et']) # always 0
                data['cend'] = datetime.utcfromtimestamp(float(st['toe'])).isoformat()
            # elif sw=='ofs':
                #print 'ofs    >>>',st
         

        q.task_done()
               
        if (hasPrev):        
            if (currState.tod<previousState.tod):
                logger.warning("package came out of order. Skipping the message.")
                continue
            data['link_total'] = currState.link_total - previousState.link_total
            data['link_in']    = currState.link_in    - previousState.link_in
            data['link_out']   = currState.link_out   - previousState.link_out
            data['link_ctime'] = currState.link_ctime - previousState.link_ctime
            data['link_tmo']   = currState.link_tmo   - previousState.link_tmo
            # data['link_stall'] = currState.link_stall - previousState.link_stall
            # data['link_sfps']  = currState.link_sfps  - previousState.link_sfps
            data['proc_usr']  = currState.proc_usr  - previousState.proc_usr
            data['proc_sys']  = currState.proc_sys  - previousState.proc_sys
            data['xrootd_errors'] = currState.xrootd_err - previousState.xrootd_err 
            data['xrootd_delays'] = currState.xrootd_dly - previousState.xrootd_dly 
            data['xrootd_redirections'] = currState.xrootd_rdr - previousState.xrootd_rdr 
            data['ops_open'] = currState.ops_open - previousState.ops_open
            data['ops_preread']   = currState.ops_pr   - previousState.ops_pr  
            data['ops_read']   = currState.ops_rd   - previousState.ops_rd  
            data['ops_readv']   = currState.ops_rv   - previousState.ops_rv  
            data['ops_sync'] = currState.ops_sync - previousState.ops_sync
            data['ops_write']   = currState.ops_wr   - previousState.ops_wr  
            data['login_attempts']  = currState.lgn_num  - previousState.lgn_num 
            data['authentication_failures']   = currState.lgn_af   - previousState.lgn_af  
            data['authentication_successes']   = currState.lgn_au   - previousState.lgn_au  
            data['unauthenticated_successes']   = currState.lgn_ua   - previousState.lgn_ua  
            aLotOfData.append(data)    
        else:
            if addr not in AllState:
                AllState[addr]={}
                
        AllState[addr][pid]=currState
        
        # print "current state ----"
        # currState.prnt()
        if len(aLotOfData)%50==49:
            logger.error('Some problem in sending data to ES. Trying to reconnect.')
            RefreshConnection()
            
        if len(aLotOfData)%21==20:
            try:
                res = helpers.bulk(es, aLotOfData, raise_on_exception=True,request_timeout=60)
                logger.info("%s \tinserted: %i \terrors: %s", threading.current_thread().name, res[0], str(res[1]) )
                aLotOfData=[]
            except es_exceptions.ConnectionError as e:
                logger.error('ConnectionError %s', e)
            except es_exceptions.TransportError as e:
                logger.error('TransportError %s ', e)
            except helpers.BulkIndexError as e:
                logger.error('%s',e[0])
                errcount=0
                for i in e[1]:
                    errcount+=1
                    if errcount>5: break
                    logger.error('%s',i)
            except:
                logger.error('Something seriously wrong happened:')
                e = sys.exc_info()[0]
                logger.error(e)
                




lastReconnectionTime=0
es = None
while (not es):
    RefreshConnection()

q=Queue.Queue()
#start eventCreator threads
for i in range(1):
     t = Thread(target=eventCreator)
     t.daemon = True
     t.start()
     
nMessages=0
while (True):
    message, addr = sock.recvfrom(4096) # buffer size is 1024 bytes
    # print ("received message:", message, "from:", addr)
    q.put([message,addr[0]])
    nMessages+=1
    if (nMessages%100==0):
        logger.info("messages received: %i   qsize: %i", nMessages, q.qsize())