#!/usr/bin/env python

import decoding
import struct
from collections import namedtuple
import os, sys, time
import threading
from threading import Thread
import socket, requests
from multiprocessing import Process, Queue
import json
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

import logging
import logging.config

import ConfigParser
import pika
import re

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('DetailedCollector')


#hostIP="192.170.227.128"
hostIP=socket.gethostbyname(socket.gethostname())

DETAILED_PORT = 9930

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
sock.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,1024*1024*4) 
sock.bind(("0.0.0.0", DETAILED_PORT))

AllTransfers={}
AllServers={}
AllUsers={}

connect_config = ConfigParser.ConfigParser()
connect_config.read('connection.conf')

# Set stdout / stderr to the logger
# https://stackoverflow.com/questions/19425736/how-to-redirect-stdout-and-stderr-to-logger-in-python
class LoggerWriter:
    def __init__(self, level):
        # self.level is really like using log.debug(message)
        # at least in my case
        self.level = level

    def write(self, message):
        # if statement reduces the amount of newlines that are
        # printed to the logger
        if message != '\n':
            self.level(message)

    def flush(self):
        # create a flush method so things can be flushed when
        # the system wants to. Not sure if simply 'printing'
        # sys.stderr is the correct way to do it, but it seemed
        # to work properly for me.
        self.level(sys.stderr)


def CreateRabbitConnection():
    global channel
    parameters = pika.URLParameters(connect_config.get('AMQP', 'url'))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()




def addRecord(sid, userID, fileClose, timestamp, addr):
    rec={}
    rec['timestamp']=timestamp*1000
    
    rec['server_hostname'] = socket.gethostbyaddr(addr)[0]
    rec['server_ip'] = addr
    if sid in AllServers:
        s = AllServers[sid]
        rec['serverID'] = sid
        rec['server'] = s.addr
        rec['site'] = s.site
    else:
        rec['server'] = addr
        #logger.warning('server still not identified: %s',sid) 
        
    try:
        u = AllUsers[sid][userID]
        if u is not None:
            rec['user']=u.username
            rec['host']=u.host
            if not re.match("^[\[\:f\d\.]+", u.host):
                rec['user_domain'] = ".".join(u.host.split('.')[-2:])
            rec['location']=decoding.getLongLat(u.host)
    except KeyError:
        logger.error("File close record from unknown UserID=%i, SID=%s", userID, sid)
        AllUsers.setdefault(sid, {})[userID] = None
    transfer_key = str(sid) + "." + str(fileClose.fileID)
    if transfer_key in AllTransfers:
        f = AllTransfers[transfer_key][1]
        rec['filename'] = f.fileName
        rec['filesize'] = f.fileSize
        rec['dirname1'] = "/".join(f.fileName.split('/', 2)[:2])
        rec['dirname2'] = "/".join(f.fileName.split('/', 3)[:3])
        if f.fileName.startswith('/user'):
            rec['logical_dirname'] = rec['dirname2']
        elif f.fileName.startswith('/pnfs/fnal.gov/usr'):
            rec['logical_dirname'] = "/".join(f.fileName.split('/')[:5])
    else:
        rec['filename'] = "unknown"
        rec['filesize'] = "-1"
        rec['logical_dirname'] = "unknown"
    rec['read']     = fileClose.read
    rec['readv']    = fileClose.readv
    rec['write']    = fileClose.write

            
    #print rec
    try:
    	channel.basic_publish(connect_config.get('AMQP', 'exchange'),
                      "file-close",
                      json.dumps(rec),
                      pika.BasicProperties(content_type='application/json',
                                           delivery_mode=1))
    except Exception as e:
        logger.exception('Error while sending rabbitmq message')
        CreateRabbitConnection()
        channel.basic_publish(connect_config.get('AMQP', 'exchange'),
                      "file-close",
                      json.dumps(rec),
                      pika.BasicProperties(content_type='application/json',
                                           delivery_mode=1))


    return rec

def eventCreator():
    
    aLotOfData=[]
    last_flush = time.time()
    seq_data = {}
    sys.stdout = LoggerWriter(logger.debug)
    sys.stderr = LoggerWriter(logger.error)
    while(True):
        [d,addr,port]=q.get()
        
        #print "Byte Length of Message: {0}".format(len(d)) 
        h=decoding.header._make(struct.unpack("!cBHI",d[:8])) # XrdXrootdMonHeader
        #print "Byte Length of Message: {0}, expected: {1}".format(len(d), h.plen)
        if len(d) != h.plen:
            logger.error("Packet Length incorrect: expected={0}, got={1}".format(h.plen, len(d)))
        
        print h
        logger.debug(h)    
        d=d[8:]
        # Summarize current datastructure
        #num_servers = len(AllTransfers)
        #num_users = 0
        #num_files = 0
        #for sid in AllTransfers:
        #    num_users += len(AllTransfers[sid])
        #    for user in AllTransfers[sid]:
        #        num_files += len(AllTransfers[sid][user])

        #print "Servers: {0}, Users: {1}, Files: {2}".format(num_servers, num_users, num_files)

        sid = str(h.server_start) + "#" + str(addr) + "#" + str(port)
         
        if (h.code=='f'):
            logger.debug("Got fstream object")
            TimeRecord=decoding.MonFile(d) # first one is always TOD
            logger.debug(TimeRecord)
            d=d[TimeRecord.recSize:]
            #sid=(h.server_start << 32) + TimeRecord.sid
            #Esid=(h.server_start << 32) + 0
            #sid = TimeRecord.sid
            if sid not in seq_data:
                seq_data[sid] = h.pseq
                logger.debug("New SID found.  sid={0}, addr={1}".format(str(sid), addr))
            else:
                # What is the last seq number we got
                last_seq = seq_data[sid]
                expected_seq = (last_seq + 1)
                if expected_seq == 256:
                    expected_seq = 0
                if expected_seq != h.pseq:
                    missed_packets = abs(h.pseq - expected_seq)
                    logger.error("Missed packet(s)!  Expected seq={0}, got={1}.  Missed {2} packets! from {3}".format(expected_seq, h.pseq, missed_packets, addr))
                seq_data[sid] = h.pseq
            logger.debug("Size of seq_data: {0}".format(len(seq_data)))
            now = time.time()

            for i in range(TimeRecord.total_recs): 
                hd=decoding.MonFile(d)
                d=d[hd.recSize:]
                
                if isinstance(hd, decoding.fileDisc):
                    try:
                        user_info = AllUsers.setdefault(sid, {})
                        del user_info[hd.userID]
                    except KeyError:
                        logger.error('Disconnect event for unknown UserID=%i with SID=%s', hd.userID, sid)
                
                elif isinstance(hd, decoding.fileOpen):
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    logger.debug('%i %s', i, hd)
                    AllTransfers[transfer_key]=((now, addr), hd)
                    
                elif isinstance(hd, decoding.fileClose):
                    #logger.debug('%i %s', i, hd)
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    if transfer_key in AllTransfers:
                        u = AllTransfers[transfer_key][1].userID
                        rec = addRecord(sid, u, hd, TimeRecord.tEnd, addr)
                        logger.debug("Record to send: %s", str(rec))
                        #aLotOfData.append( rec  )
                        del AllTransfers[transfer_key]
                        logger.debug('%i %s', i, hd)
                    else:
                        rec = addRecord(sid, 0, hd, TimeRecord.tEnd, addr)
                        logger.error("file to close not found. fileID: %i, serverID: %s. close=%s", hd.fileID, sid, str(hd))
                elif isinstance(hd, decoding.fileXfr):
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    if transfer_key in AllTransfers:
                        cur_value = AllTransfers[transfer_key]
                        AllTransfers[transfer_key] = ((now, cur_value[0][1]), cur_value[1], hd)
                        logger.debug("{0} Known xfrInfo: {1}. sid={2}".format(i, str(hd), sid))
                    else:
                        logger.debug("{0} Unknown xfrInfo: {1}. sid={2}".format(i, str(hd), sid))

            d = d[hd.recSize:]
            if len(d) != 0:
                logger.error("Bytes leftover! {0} bytes left!".format(len(d)))
        elif (h.code=='r'):
            logger.debug("r - stream message.")

        elif (h.code=='t'):
            logger.warning("t - stream message. Server at %s should remove files, io, iov from the monitoring configuration.", addr)

        else: 
            infolen=len(d)-4
            mm = decoding.mapheader._make(struct.unpack("!I"+str(infolen)+"s",d))
            try:
                (u,rest) = mm.info.split('\n',1)
            except ValueError as e:
                if (not h.code=='u'):
                    logger.error('%s',e[0])
                    logger.warning("Strange >>%s<< mapping message from %s mm: %s", h.code, addr, mm)
                u=mm.info
                rest=''

            userInfo=decoding.userInfo(u)
            logger.debug('%i %s', mm.dictID, userInfo)

            if (h.code=='='):
                serverInfo=decoding.serverInfo(rest,addr)
                if sid not in AllServers:
                    AllServers[sid]=serverInfo
                    logger.info('Adding new server info: %s started at %i', serverInfo, h.server_start)

            elif h.code == 'd':
                path=rest
                logger.warning('Path information sent. Server at %s should remove "files" directive from the monitoring configuration.', addr)

            elif h.code == 'i':
                appinfo=rest
                logger.info('appinfo:%s', appinfo)

            elif h.code == 'p':
                purgeInfo=decoding.purgeInfo(rest)
                logger.info('purgeInfo:%s', purgeInfo)

            elif h.code == 'u':
                authorizationInfo=decoding.authorizationInfo(rest)
                if sid not in AllUsers:
                    AllUsers[sid]={}
                if mm.dictID not in AllUsers[sid]:
                    AllUsers[sid][mm.dictID]=userInfo #authorizationInfo
                    logger.debug("Adding new user:%s", authorizationInfo)
                elif AllUsers[sid][mm.dictID] is None:
                    logger.warning("Received a user ID (%i) from sid %s after corresponding f-stream usage information.", mm.dictID, sid)
                    AllUsers[sid][mm.dictID] = userInfo
                elif AllUsers[sid][mm.dictID]:
                    logger.warning("%sThere is a problem. We already have this sid: %i and userID:%s (%s).%s",decoding.bcolors.FAIL, sid, mm.dictID, userInfo, decoding.bcolors.ENDC)

            elif (h.code=='x'):
                xfrInfo=decoding.xfrInfo(rest)
                #transfer_key = str(sid) + "." + str(xfrInfo.fileID)
                #if transfer_key in AllTransfers:
                #    cur_value = AllTransfers[transfer_key]
                #    AllTransfers[transfer_key] = (time.time(), cur_value[1], xfrInfo)
                #    print "Adding xfrInfo"
                
                # print xfrInfo
        
        #q.task_done()
        
        # Check if we have to flush the AllTransfer
        now_time = time.time()
        if (now_time - last_flush) > (60*5):
            for key in AllTransfers.keys():
                cur_value = AllTransfers[key]
                if (now_time - cur_value[0][0]) > (3600*5):
                    if len(cur_value) == 3:
                        (sid, fileID) = key.rsplit(".", 1)
                        u = cur_value[1].userID
                        addr = cur_value[0][1]
                        rec = addRecord(sid, u, cur_value[2], now_time, addr)
                    del AllTransfers[key]

        if q.qsize() > 200:
            logger.error('QSize is large: {0}'.format(q.qsize()))

        if len(aLotOfData) > 50:
            try:
                #res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
                #logger.info('%s  inserted: %i  errors: %s',threading.current_thread().name, res[0], str(res[1]))
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
                logger.exception('Something serious happened.')
                e = sys.exc_info()[0]
                logger.error(e)
    logger.error('SOMEHOW WENT OUT OF THE LOOP!')

es = None
lastReconnectionTime=0

CreateRabbitConnection()

q = Queue()

p = Process(target=eventCreator)
p.daemon = True
p.start()

#start eventCreator threads
#for i in range(1):
#     t = Thread(target=eventCreator)
#     t.daemon = True
#     t.start()
    

debug_file = open('stuff.err', 'a')
nMessages=0
debug=False
while (True):
    message, addr = sock.recvfrom(65536) # buffer size is 1024 bytes
    if len(message) > 50000:
        debug_file.write("Size of packet is large: {0}\n".format(len(message)))

    print addr
    #print ("received message:", message, "from:", addr)
    q.put([message,addr[0],addr[1]])
    nMessages+=1
    #if (nMessages%100==0):
    #    logger.info("messages received: %i   qsize: %i", nMessages, q.qsize())
    #    if debug:
    #        print "All Servers:", AllServers
    #        print "All Users:"
    #        for sid in AllUsers:
    #            print sid, len(AllUsers[sid]) 
    #            for uid in AllUsers[sid]:  # enabling this can crash it if size change during iteration
    #                print uid, AllUsers[sid][uid]
    #        print "All Transfers:"
    #        for sid in AllTransfers:
    #            print sid
    #            for uid in AllTransfers[sid]:
    #                print uid
    #                for fid in AllTransfers[sid][uid]:
    #                    print fid, AllTransfers[sid][uid][fid]

