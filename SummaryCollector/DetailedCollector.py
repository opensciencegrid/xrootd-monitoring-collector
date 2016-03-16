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


#hostIP="192.170.227.128"
hostIP=socket.gethostbyname(socket.gethostname())

SUMMARY_PORT = 9931
DETAILED_PORT = 9930

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 
sock.bind((hostIP, DETAILED_PORT))

def GetESConnection(lastReconnectionTime):
    if ( time.time()-lastReconnectionTime < 60 ):
        return
    lastReconnectionTime=time.time()
    print "make sure we are connected right..."
    res = requests.get('http://uct2-es-door.mwt2.org:9200')
    print(res.content)
    es = Elasticsearch([{'host':'uct2-es-door.mwt2.org', 'port':9200}])
    return es

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
        print "pid:",self.pid, "\ttotal:",self.link_total, "\tin:",self.link_in, "\tout:",self.link_out, "\tctime:",self.link_ctime, "\ttmo:",self.link_tmo
        



AllTransfers={}
AllServers={}
AllUsers={}

def addRecord(server_start,userID,fileID):
    rec={
        '_type': 'detailed'
    }
    try:
        rec['server'] = AllServers[server_start]
        rec['user'] = AllUsers[server_start][userID]
        rec['file'] = AllTransfers[server_start][userID][fileID]
    except KeyError:
        print 'Server, user or file info missing.'
    d = datetime.now()
    ind="xrd_detailed-"+str(d.year)+"."+str(d.month)+"."+str(d.day)
    rec['_index']=ind
    return rec
    
def eventCreator():
    
    aLotOfData=[]
    while(True):
        [d,addr]=q.get()
        
        # print "\nByte Length of Message :", len(d)
        
        h=decoding.header._make(struct.unpack("!cBHI",d[:8])) # XrdXrootdMonHeader
        
        if h[3]!=1457990510:
            q.task_done()
            continue
            
        print h
        
        d=d[8:]
        
        
        if (h.code=='f'):
            TimeRecord=decoding.MonFile(d) # first one is always TOD
            print TimeRecord
            d=d[TimeRecord.recSize:]
            sid=(h.server_start << 32) + TimeRecord.sid
            for i in range(TimeRecord.total_recs): 
                hd=decoding.MonFile(d)
                d=d[hd.recSize:]
                
                print i, hd
                
                if isinstance(hd, decoding.fileDisc):
                    try:
                        print "Disconnecting: ", AllUsers[sid][hd.userID]
                        del AllUsers[sid][hd.userID]
                    except KeyError:
                        print 'User that disconnected was unknown.'
                
                elif isinstance(hd, decoding.fileOpen):
                    if sid not in AllTransfers:
                        AllTransfers[sid]={}
                    if hd.userID not in AllTransfers[sid]:
                        AllTransfers[sid][hd.userID]={}
                    AllTransfers[sid][hd.userID][hd.fileID]=hd
                    
                elif isinstance(hd, decoding.fileClose):
                    if sid in AllTransfers:
                        found=0
                        for u in AllTransfers[sid]:
                            if hd.fileID in AllTransfers[sid][u]:
                                found=1
                                aLotOfData.append( addRecord(sid,u,hd.fileID) )
                                del AllTransfers[sid][u][hd.fileID]
                                break
                        if not found:
                            print decoding.bcolors.WARNING +"file to close not found." + decoding.bcolors.ENDC
                    else:
                        print "file closed on server that's not found"
                        AllTransfers[sid]={}
                                
                
        elif (h.code=='r'):
            print "r - stream message."
            
        elif (h.code=='t'):
            print "t - stream message. Server started at", h[3],"should remove files, io, iov from the monitoring configuration."
            
            
        else: 
            infolen=len(d)-4
            mm = decoding.mapheader._make(struct.unpack("!I"+str(infolen)+"s",d))
            (u,rest) = mm.info.split('\n',1)
            userInfo=decoding.userInfo(u)
            print mm.dictID, userInfo
            
            sid=(h.server_start << 32) + 0 #userInfo.sid - this has to go in place of 0 when the new version of server is there.
            
            if (h.code=='='):
                serverInfo=decoding.serverInfo(rest)
                if sid not in AllServers:
                    AllServers[sid]==serverInfo
                    print 'Adding new server info: ', serverInfo
                    
            elif (h.code=='d'):
                path=rest
                # print 'path: ', path
                print 'path information. Server started at', h[3], 'should remove files from the monitoring configuration.'
                
            elif (h.code=='i'):
                appinfo=rest
                print 'appinfo:', appinfo
                
            elif (h.code=='p'):
                purgeInfo=decoding.purgeInfo(rest)
                print purgeInfo
                
            elif (h.code=='u'):
                authorizationInfo=decoding.authorizationInfo(rest)
                if sid not in AllUsers:
                    AllUsers[sid]={}
                if mm.dictID not in AllUsers[sid]:
                    AllUsers[sid][mm.dictID]=authorizationInfo
                    #print "Adding new user:", authorizationInfo
                else:
                    print "There is a problem. We already have this combination of sid and userID."
                    
            elif (h.code=='x'):
                xfrInfo=decoding.xfrInfo(rest)
                # print xfrInfo
        
        
        
        print '------------------------------------------------'
        
        

        
        q.task_done()
        continue
        
        # s=m['statistics'] # top level
        # pgm         = s['@pgm'] # program name
        # #print "PGM >>> ", pgm
        # if (pgm != 'xrootd'):
        #     q.task_done()
        #     continue
        #
        # tos         = int(s['@tos'])  # Unix time when the program was started.
        # tod         = int(s['@tod'])  # Unix time when statistics gathering started.
        # pid         = int(s['@pid'])
        #
        # data['timestamp'] = datetime.utcfromtimestamp(float(tod)).isoformat()
        # data['tos'] = datetime.utcfromtimestamp(float(tos)).isoformat()
        # data['cstart'] = datetime.utcfromtimestamp(float(tod)).isoformat()
        # data['version']  = s['@ver'] # version name of the servers
        # data['site'] = s['@site'] # site name specified in the configuration
        #         data['cend'] = datetime.utcfromtimestamp(float(st['toe'])).isoformat()
        
        if len(aLotOfData)>50:
            try:
                res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
                print threading.current_thread().name, "\t inserted:",res[0], '\tErrors:',res[1]
                aLotOfData=[]
            except es_exceptions.ConnectionError as e:
                print 'ConnectionError ', e
            except es_exceptions.TransportError as e:
                print 'TransportError ', e
            except helpers.BulkIndexError as e:
                print e[0]
                for i in e[1]:
                    print i
            except:
                print 'Something seriously wrong happened. '




lastReconnectionTime=0
es = GetESConnection(lastReconnectionTime)
while (not es):
    es = GetESConnection(lastReconnectionTime)

q=Queue.Queue()
#start eventCreator threads
for i in range(3):
     t = Thread(target=eventCreator)
     t.daemon = True
     t.start()
     
nMessages=0
while (True):
    message, addr = sock.recvfrom(65536) # buffer size is 1024 bytes
    # print ("received message:", message, "from:", addr)
    q.put([message,addr[0]])
    nMessages+=1
    if (nMessages%100==0):
        print ("messages received:", nMessages, " qsize:", q.qsize())
        print "All Servers:", AllServers
        print "All Users:"
        for sid in AllUsers:
            print sid 
            for uid in AllUsers[sid]:
                print uid, AllUsers[sid][uid]
        print "All Transfers:", AllTransfers