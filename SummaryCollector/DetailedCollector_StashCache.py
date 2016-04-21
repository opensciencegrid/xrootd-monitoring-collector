#!/usr/bin/env python

import decoding
import struct
from collections import namedtuple
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
logger = logging.getLogger('DetailedCollector')


#hostIP="192.170.227.128"
hostIP=socket.gethostbyname(socket.gethostname())

DETAILED_PORT = 9930

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
sock.bind((hostIP, DETAILED_PORT))

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

AllTransfers={}
AllServers={}
AllUsers={}

def addRecord(sid,userID,fileClose,timestamp):
    rec={
        '_type': 'detailed'
    }
    rec['timestamp']=timestamp*1000
    
    if sid in AllServers:
        s = AllServers[sid]
        rec['serverID'] = sid
        rec['server'] = s.addr
        rec['site'] = s.site
    else:
        logger.warning('server still not identified: %i',sid) 
        
    try:
        u = AllUsers[sid][userID]
        rec['user']=u.username
        rec['host']=u.host
        rec['location']=decoding.getLongLat(u.host)
    except KeyError:
        logger.error( '%suser %i missing.%s',decoding.bcolors.WARNING, userID, decoding.bcolors.ENDC)
        # print decoding.bcolors.WARNING + 'user ' + str(userID) + ' missing.' + decoding.bcolors.ENDC

    f = AllTransfers[sid][userID][fileClose.fileID]
    rec['filename'] = f.fileName
    rec['filesize'] = f.fileSize
    rec['read']     = fileClose.read
    rec['readv']    = fileClose.readv
    rec['write']    = fileClose.write
    
    d = datetime.now()
    ind="sc_xrd_detailed-"+str(d.year)+"."+str(d.month)
    rec['_index']=ind
    return rec
    
def eventCreator():
    
    aLotOfData=[]
    while(True):
        [d,addr]=q.get()
        
        # print "\nByte Length of Message :", len(d)
        
        h=decoding.header._make(struct.unpack("!cBHI",d[:8])) # XrdXrootdMonHeader
        
        # if h[3]!=1457990510:
        #     q.task_done()
        #     continue
        
        # if debug:
        #     print '------------------------------------------------'
        #     print h
        # else:
        #     print '*',
                        
        logger.debug(h)    
        
        d=d[8:]
        
        
        if (h.code=='f'):
            TimeRecord=decoding.MonFile(d) # first one is always TOD
            logger.debug(TimeRecord)
            d=d[TimeRecord.recSize:]
            sid=(h.server_start << 32) + TimeRecord.sid
            for i in range(TimeRecord.total_recs): 
                hd=decoding.MonFile(d)
                d=d[hd.recSize:]
                
                logger.debug('%i %s', i, hd)
                # if debug: print i, hd
                
                if isinstance(hd, decoding.fileDisc):
                    try:
                        #print "Disconnecting: ", AllUsers[sid][hd.userID]
                        del AllUsers[sid][hd.userID]
                    except KeyError:
                        logger.error('%sUser that disconnected was unknown.%s', decoding.bcolors.WARNING, decoding.bcolors.ENDC)
                        #print decoding.bcolors.WARNING + 'User that disconnected was unknown.' + decoding.bcolors.ENDC
                
                elif isinstance(hd, decoding.fileOpen):
                    if sid not in AllTransfers:
                        AllTransfers[sid]={}
                    if hd.userID not in AllTransfers[sid]:
                        AllTransfers[sid][hd.userID]={}
                    AllTransfers[sid][hd.userID][hd.fileID]=hd
                    
                elif isinstance(hd, decoding.fileClose):
                    logger.debug('%i %s', i, hd)
                    if sid in AllTransfers:
                        found=0
                        for u in AllTransfers[sid]:
                            if hd.fileID in AllTransfers[sid][u]:
                                found=1
                                rec = addRecord(sid, u, hd, TimeRecord.tEnd)
                                aLotOfData.append( rec  )
                                del AllTransfers[sid][u][hd.fileID]
                                if len(AllTransfers[sid][u])==0: del AllTransfers[sid][u]
                                break
                        if not found:
                            logger.error("%sfile to close not found.%s", decoding.bcolors.WARNING, decoding.bcolors.ENDC)
                            # print decoding.bcolors.WARNING + "file to close not found." + decoding.bcolors.ENDC
                    else:
                        logger.error("%sfile closed on server that's not found%s", decoding.bcolors.WARNING, decoding.bcolors.ENDC)
                        # print decoding.bcolors.WARNING + "file closed on server that's not found" + decoding.bcolors.ENDC
                        AllTransfers[sid]={}
                                
                
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
                logger.error('%s',e[0])
                logger.warning("Strange >>%s<< mapping message from %s mm: %s", h.code, addr, mm)
                u=mm.info
                rest=''
                
            userInfo=decoding.userInfo(u)
            logger.debug('%i %s', mm.dictID, userInfo)
            
            sid=(h.server_start << 32) + 0 #userInfo.sid - this has to go in place of 0 when the new version of server is there.
            
            if (h.code=='='):
                serverInfo=decoding.serverInfo(rest,addr)
                if sid not in AllServers:
                    AllServers[sid]=serverInfo
                    logger.info('Adding new server info: %s started at %i', serverInfo, h.server_start)
                    
            elif (h.code=='d'):
                path=rest
                # print 'path: ', path
                logger.warning('path information. Server at %s should remove files from the monitoring configuration.', addr)
                
            elif (h.code=='i'):
                appinfo=rest
                logger.info('appinfo:%s', appinfo)
                
            elif (h.code=='p'):
                purgeInfo=decoding.purgeInfo(rest)
                logger.info('purgeInfo:%s', purgeInfo)
                
            elif (h.code=='u'):
                authorizationInfo=decoding.authorizationInfo(rest)
                if sid not in AllUsers:
                    AllUsers[sid]={}
                if mm.dictID not in AllUsers[sid]:
                    AllUsers[sid][mm.dictID]=userInfo #authorizationInfo
                    logger.debug("Adding new user:%s", authorizationInfo)
                else:
                    logger.warning("%sThere is a problem. We already have this sid: %i and userID:%s (%s).%s",decoding.bcolors.FAIL, sid, mm.dictID, userInfo, decoding.bcolors.ENDC)
                    
            elif (h.code=='x'):
                xfrInfo=decoding.xfrInfo(rest)
                # print xfrInfo
        
        q.task_done()
        
        if q.qsize()>200:
            logger.error('Some problem in sending data to ES. Trying to reconnect.')
            RefreshConnection()
            
        if len(aLotOfData)>50:
            try:
                res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
                logger.info('%s  inserted: %i  errors: %s',threading.current_thread().name, res[0], str(res[1]))
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
                logger.error('Something serious happened.')
                e = sys.exc_info()[0]
                logger.error(e)
    logger.error('SOMEHOW WENT OUT OF THE LOOP!')

es = None
lastReconnectionTime=0
while (not es):
    RefreshConnection()

q=Queue.Queue()
#start eventCreator threads
for i in range(1):
     t = Thread(target=eventCreator)
     t.daemon = True
     t.start()
     
nMessages=0
debug=False
while (True):
    message, addr = sock.recvfrom(65536) # buffer size is 1024 bytes
    # print ("received message:", message, "from:", addr)
    q.put([message,addr[0]])
    nMessages+=1
    if (nMessages%100==0):
        logger.info("messages received: %i   qsize: %i", nMessages, q.qsize())
        if debug:
            print "All Servers:", AllServers
            print "All Users:"
            for sid in AllUsers:
                print sid, len(AllUsers[sid]) 
                for uid in AllUsers[sid]:  # enabling this can crash it if size change during iteration
                    print uid, AllUsers[sid][uid]
            print "All Transfers:"
            for sid in AllTransfers:
                print sid
                for uid in AllTransfers[sid]:
                    print uid
                    for fid in AllTransfers[sid][uid]:
                        print fid, AllTransfers[sid][uid][fid]