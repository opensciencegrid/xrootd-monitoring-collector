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

AllTransfers={}
AllServers={}
AllUsers={}

def addRecord(sid,userID,fileID,timestamp):
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
        print 'server still not identified:',sid 
        
    try:
        u = AllUsers[sid][userID]
        rec['user']=u.username
        rec['host']=u.host
    except KeyError:
        print decoding.bcolors.WARNING + 'user ' + str(userID) + ' or file info ' + str(fileID) + ' missing.' + decoding.bcolors.ENDC

    f = AllTransfers[sid][userID][fileID]
    rec['filename']=f.fileName
    rec['filesize']=f.fileSize
    
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
        
        # if h[3]!=1457990510:
        #     q.task_done()
        #     continue
            
        if debug:
            print '------------------------------------------------' 
            print h
        else:
            print '*',
        
        d=d[8:]
        
        
        if (h.code=='f'):
            TimeRecord=decoding.MonFile(d) # first one is always TOD
            if debug: print TimeRecord
            d=d[TimeRecord.recSize:]
            sid=(h.server_start << 32) + TimeRecord.sid
            for i in range(TimeRecord.total_recs): 
                hd=decoding.MonFile(d)
                d=d[hd.recSize:]
                
                if debug: print i, hd
                
                if isinstance(hd, decoding.fileDisc):
                    try:
                        #print "Disconnecting: ", AllUsers[sid][hd.userID]
                        del AllUsers[sid][hd.userID]
                    except KeyError:
                        print decoding.bcolors.WARNING + 'User that disconnected was unknown.' + decoding.bcolors.ENDC
                
                elif isinstance(hd, decoding.fileOpen):
                    if sid not in AllTransfers:
                        AllTransfers[sid]={}
                    if hd.userID not in AllTransfers[sid]:
                        AllTransfers[sid][hd.userID]={}
                    AllTransfers[sid][hd.userID][hd.fileID]=hd
                    
                elif isinstance(hd, decoding.fileClose):
                    print i, hd
                    if sid in AllTransfers:
                        found=0
                        for u in AllTransfers[sid]:
                            if hd.fileID in AllTransfers[sid][u]:
                                found=1
                                rec = addRecord(sid, u, hd.fileID, TimeRecord.tEnd)
                                print rec
                                aLotOfData.append( rec  )
                                del AllTransfers[sid][u][hd.fileID]
                                if len(AllTransfers[sid][u])==0: del AllTransfers[sid][u]
                                break
                        if not found:
                            print decoding.bcolors.WARNING + "file to close not found." + decoding.bcolors.ENDC
                    else:
                        print decoding.bcolors.WARNING + "file closed on server that's not found" + decoding.bcolors.ENDC
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
            if debug: print mm.dictID, userInfo
            
            sid=(h.server_start << 32) + 0 #userInfo.sid - this has to go in place of 0 when the new version of server is there.
            
            if (h.code=='='):
                serverInfo=decoding.serverInfo(rest,addr)
                if sid not in AllServers:
                    AllServers[sid]=serverInfo
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
                    AllUsers[sid][mm.dictID]=userInfo #authorizationInfo
                    #print "Adding new user:", authorizationInfo
                else:
                    print decoding.bcolors.FAIL + "There is a problem. We already have this combination of sid and userID." + decoding.bcolors.ENDC
                    
            elif (h.code=='x'):
                xfrInfo=decoding.xfrInfo(rest)
                # print xfrInfo
        
        
        
        
        q.task_done()
        #continue
        
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
                errcount=0
                for i in e[1]:
                    errcount+=1
                    if errcount>5: break
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
debug=False
while (True):
    message, addr = sock.recvfrom(65536) # buffer size is 1024 bytes
    # print ("received message:", message, "from:", addr)
    q.put([message,addr[0]])
    nMessages+=1
    if (nMessages%100==0):
        print ("messages received:", nMessages, " qsize:", q.qsize())
        print "All Servers:", AllServers
        if debug:
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