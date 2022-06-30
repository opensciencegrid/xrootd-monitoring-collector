#!/usr/bin/env python

"""
A simple daemon for collecting monitoring packets from a remote XRootD
host, performing simple aggregations, and forwarding the resulting data
to a message queue.
"""

import re
import socket
import struct
import time
import collections
import json

import decoding
import wlcg_converter
import UdpCollector
import ttldict

DEFAULT_TTL = 3600*1

class DetailedCollector(UdpCollector.UdpCollector):

    DEFAULT_PORT = 9930


    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        
        self._transfers = {}
        self._servers = {}
        self._users = {}
        self._dictid_map = {}
        
        self._exchange = self.config.get('AMQP', 'exchange')
        self._wlcg_exchange = self.config.get('AMQP', 'wlcg_exchange')
        self._tcp_exchange = self.config.get('AMQP', 'tcp_exchange')
        self._exchange_cache = self.config.get('AMQP', 'exchange_cache')
        self._wlcg_exchange_cache = self.config.get('AMQP', 'wlcg_exchange_cache')
        self._exchange_tpc = self.config.get('AMQP', 'exchange_tpc')
        self._wlcg_exchange_tpc = self.config.get('AMQP', 'wlcg_exchange_tpc')

        
        self.last_flush = time.time()
        self.seq_data = {}


    def addRecord(self, sid, userID, fileClose, timestamp, addr, openTime):
        """
        Given information to create a record, send it up to the message queue.
        """
        rec = {}
        lcg_record = False
        rec['timestamp'] = int(timestamp*1000)  # expected to be in MS since Unix epoch
        rec['start_time'] = int(openTime*1000)
        rec['end_time'] = int(timestamp*1000)
        rec['operation_time'] = int((rec['end_time'] - rec['start_time']) / 1000)
        path = ""

        try:
            rec['server_hostname'] = socket.gethostbyaddr(addr)[0]
        except:
            pass

        rec['server_ip'] = addr
        if sid in self._servers:
            s = self._servers[sid]
            rec['serverID'] = sid
            rec['server'] = s.addr
            if s.site is not None:
                rec['site'] = s.site.decode('utf-8')
            else:
                rec['site'] = ''

        else:
            rec['server'] = addr
            # logger.warning('server still not identified: %s',sid)

        try:
            # Get userinfo from the map
            userInfo = None
            if userID != 0:
                userInfo = self._dictid_map[sid][userID]
            elif fileClose.fileID in self._dictid_map[sid]:
                pathInfo = self._dictid_map[sid][fileClose.fileID]
                userInfo = pathInfo.userinfo
                path = pathInfo.path.decode('utf-8')
            u = self._users[sid][userInfo].get('userinfo', None)
            auth = self._users[sid][userInfo].get('authinfo', None)
            appinfo = self._users[sid][userInfo].get('appinfo', None)

            if u is not None:
                hostname = u.host.decode('idna')
                rec['user'] = u.username.decode('utf-8')
                rec['host'] = hostname
                if not re.match(r"^[\[\:f\d\.]+", hostname):
                    rec['user_domain'] = ".".join(hostname.split('.')[-2:])
                if u.protocol:
                    rec['protocol'] = u.protocol.decode('ascii')
            if auth is not None:
                if auth.inetv != b'':
                    rec['ipv6'] = True if auth.inetv == b'6' else False
                if auth.info != b'':
                    rec['user_dn'] = auth.info.decode('utf-8').split("::")[0]
                if auth.on != b'':
                    if auth.on == b"cms" or auth.on == b"dteam":
                        lcg_record = True
                    rec['vo'] = auth.on.decode('utf-8')
            if appinfo is not None:
                rec['appinfo'] = appinfo
                    
        except KeyError:
            self.logger.exception("File close record from unknown UserID=%i, SID=%s", userID, sid)
            #self._users.setdefault(sid, {})[userID] = None
        except TypeError as e:
            self.logger.exception("File close record from unknown UserID=%i, SID=%s", userID, sid)
            #self._users.setdefault(sid, {})[userID] = None
        except AttributeError:
            self.logger.exception("File close record from unknown UserID=%i, SID=%s", userID, sid)
            #self._users.setdefault(sid, {})[userID] = None
        transfer_key = str(sid) + "." + str(fileClose.fileID)
        if transfer_key in self._transfers:
            f = self._transfers[transfer_key][1]
            fname = f.fileName.decode('utf-8')
            if fname == "":
                fname = path
            rec['filename'] = fname
            rec['filesize'] = f.fileSize
            rec['dirname1'] = "/".join(fname.split('/', 2)[:2])
            rec['dirname2'] = "/".join(fname.split('/', 3)[:3])
            if fname.startswith('/user'):
                rec['logical_dirname'] = rec['dirname2']
            elif fname.startswith('/osgconnect/public'):
                rec['logical_dirname'] = "/".join(fname.split('/', 4)[:4])
            elif fname.startswith('/hcc'):
                rec['logical_dirname'] = "/".join(fname.split('/', 6)[:6])
            elif fname.startswith('/pnfs/fnal.gov/usr'):
                rec['logical_dirname'] = "/".join(f.fileName.decode('utf-8').split('/')[:5])
            elif fname.startswith('/gwdata'):
                rec['logical_dirname'] = rec['dirname2']
            elif fname.startswith('/chtc/'):
                rec['logical_dirname'] = '/chtc'
            elif fname.startswith('/icecube/'):
                rec['logical_dirname'] = '/icecube'

            # Check for CMS files
            elif fname.startswith('/store') or fname.startswith('/user/dteam'):
                rec['logical_dirname'] = rec['dirname2']
                lcg_record = True
            else:
                rec['logical_dirname'] = 'unknown directory'
        else:
            rec['filename'] = "missing directory"
            rec['filesize'] = "-1"
            rec['logical_dirname'] = "missing directory"
        rec['read'] = fileClose.read
        rec['readv'] = fileClose.readv
        rec['write'] = fileClose.write

        if isinstance(fileClose, decoding.fileClose) and fileClose.ops:
            # read_average (optional)
            try:
                rec['read_average'] = (fileClose.read + fileClose.readv) / (fileClose.ops.read + fileClose.ops.readv)
            except ZeroDivisionError as zde:
                rec['read_average'] = 0
            # read_bytes_at_close (optional)
            rec['read_bytes_at_close'] = fileClose.read + fileClose.readv
            # read_max (optional)
            rec['read_max'] = max(fileClose.ops.rdMax, fileClose.ops.rvMax)
            # read_min (optional)
            rec['read_min'] = min(fileClose.ops.rdMin, fileClose.ops.rvMin)
            # read_operations (optional)
            rec['read_operations'] = fileClose.ops.read + fileClose.ops.readv
            # read_sigma (optional)
            # read_single_average (optional)
            try:
                rec['read_single_average'] = fileClose.read / fileClose.ops.read
            except ZeroDivisionError as zde:
                rec['read_single_average'] = 0
            # read_single_bytes (optional)
            rec['read_single_bytes'] = fileClose.read
            # read_single_max (optional)
            rec['read_single_max'] = fileClose.ops.rdMax
            # read_single_min (optional)
            rec['read_single_min'] = fileClose.ops.rdMin
            # read_single_operations (optional)
            rec['read_single_operations'] = fileClose.ops.read
            # read_single_sigma (optional)
            # read_vector_average (optional)
            try:
                rec['read_vector_average'] = fileClose.readv / fileClose.ops.readv
            except ZeroDivisionError as zde:
                rec['read_vector_average'] = 0
            # read_vector_count_average (optional)
            try:
                rec['read_vector_count_average'] = fileClose.ops.rsegs / fileClose.readv
            except ZeroDivisionError as zde:
                rec['read_vector_count_average'] = 0
            # read_vector_count_max (optional)
            rec['read_vector_count_max'] = fileClose.ops.rsMax
            # read_vector_count_min (optional)
            rec['read_vector_count_min'] = fileClose.ops.rsMin
            # read_vector_count_sigma (optional)
            # read_vector_max (optional)
            rec['read_vector_max'] = fileClose.ops.rvMax
            # read_vector_min (optional)
            rec['read_vector_min'] = fileClose.ops.rvMin
            # read_vector_operations (optional)
            rec['read_vector_operations'] = fileClose.ops.readv
            # read_vector_sigma (optional)
            # server_username (optional)
            # throughput (optional)
            try:
                rec['throughput'] = (fileClose.read + fileClose.readv + fileClose.write) / rec['operation_time']
            except ZeroDivisionError as zde:
                rec['throughput'] = 0
            # user_fqan (optional)
            # user_role (optional)
            # write_average (optional)
            try:
                rec['write_average'] = fileClose.write / fileClose.ops.write
            except ZeroDivisionError as zde:
                rec['write_average'] = 0
            # write_bytes_at_close (optional)
            rec['write_bytes_at_close'] = fileClose.write
            # write_max (optional)
            rec['write_max'] = fileClose.ops.wrMax
            # write_min (optional)
            rec['write_min'] = fileClose.ops.wrMin
            # write_operations (optional)
            rec['write_operations'] = fileClose.ops.write
            # write_sigma (optional)

        if 'user' not in rec:
            self.metrics_q.put({'type': 'failed user', 'count': 1})

        if 'filename' not in rec or rec['filename'] == "missing directory":
            self.metrics_q.put({'type': 'failed filename', 'count': 1})

        if not lcg_record:
            self.logger.debug("OSG record to send: %s", str(rec))
            self.publish("file-close", rec, exchange=self._exchange)
            self.metrics_q.put({'type': 'message sent', 'count': 1, 'message_type': 'stashcache'})
        else:
            wlcg_packet = wlcg_converter.Convert(rec)
            self.logger.debug("WLCG record to send: %s", str(wlcg_packet))
            self.publish("file-close", wlcg_packet, exchange=self._wlcg_exchange)
            self.metrics_q.put({'type': 'message sent', 'count': 1, 'message_type': 'wlcg'})

        return rec
        
    # return the VO based on the path    
    def returnVO(self,fname):

       if fname.startswith('/user'):
            return 'osg'
       if fname.startswith('/osgconnect/public'):
            return 'osg'
       elif fname.startswith('/pnfs/fnal.gov/usr'):
            return 'fermilab'
       elif fname.startswith('/hcc'):
            return 'hcc'
       elif fname.startswith('/gwdata'):   
            return 'gwdata'
       elif fname.startswith('/chtc/'):
            return 'chtc'
       elif fname.startswith('/icecube/'):
            return 'icecube'
       else:
            return "noVO"

    def process_gstream_tpc(self, gstream, addr, sid):
         for event in gstream.events:

             self.metrics_q.put({'type': 'gstream_message_tpc', 'count': 1})
             event["tpc_protocol"] = event.pop("TPC")
             event["client"] = event.pop("Client")
             event["xeq"] = event.pop("Xeq")

             self.logger.info(event["xeq"])
             self.logger.info(type(event["xeq"]))
             event["xeq"]["begin_transfer"] = event["xeq"].pop("Beg")
             event["xeq"]["end_transfer"] = event["xeq"].pop("End")
             event["xeq"]["ip_version"] = event["xeq"].pop("IPv")
             event["xeq"]["return_code"] = event["xeq"].pop("RC")
             event["xeq"]["used_streams"] = event["xeq"].pop("Strm")
             event["xeq"]["flow_direction"] = event["xeq"].pop("Type")
             event["source"] = event.pop("Src")
             event["destination"] = event.pop("Dst")
             event["size"] = event.pop("Size")

             if event["source"].startswith('/store') or event["source"].startswith('/user/dteam'):
                  self.logger.debug("Sending WLCG GStream TPC: %s", str(event))
                  self.publish("tpc", event, exchange=self._wlcg_exchange_tpc)
             else:
                  self.logger.debug("Sending GStream TPC: %s", str(event))
                  self.publish("tpc", event, exchange=self._exchange_tpc)


    def process_gstream(self, gstream, addr, sid):                                                      

        hostname = ""                                                                                   
        hostip = ""                                                                                     
        site = ""                                                                                       
        lcg_record = False                                                                              
        try:                                                                                            
            hostip = addr                                                                               
            try:                                                                                        
                hostname = socket.gethostbyaddr(addr)[0]                                                
            except Exception as e:                                                                      
                self.logger.exception("Not able to get gethostnyname")                                           
                                                                                                        
            if sid in self._servers:                                                                    
                s = self._servers[sid]
                if s.site is not None:
                    site = s.site.decode('utf-8')
                                                                                                        
                                                                      
            for event in gstream.events:
                try: 
                     self.metrics_q.put({'type': 'gstream_event_cache', 'count': 1})
                     event["sid"] = sid
                     event["server_ip"] = hostip
                     event["server_hostname"] = hostname
                     event["file_path"] = event.pop("lfn")
                     event["block_size"] = event.pop("blk_size")
                     event["numbers_blocks"] = event.pop("n_blks")
                     event["numbers_blocks_done"] = event.pop("n_blks_done")
                     event["access_count"] = event.pop("access_cnt")
                     event["attach_time"] = event.pop("attach_t")
                     event["detach_time"] = event.pop("detach_t")
                     remote = event.pop("remotes")
                     if(len(remote) > 0):
                          event["remotes_origin"] = remote
                     else:
                          event["remotes_origin"] = ""
                     event["block_hit_cache"] = event.pop("b_hit")
                     event["block_miss_cache"] = event.pop("b_miss")
                     event["block_bypass_cache"] = event.pop("b_bypass")
                     event["site"] = site
                     event["vo"] = self.returnVO(event["file_path"])
                     fname = event["file_path"]
                     
                     if('n_cks_errs' in event):
                         event["numbers_checksum_errors"] = event.pop("n_cks_errs")
                     
                     if fname.startswith('/store') or fname.startswith('/user/dteam'):
                         lcg_record = True
                         self.logger.info("Sending GStream for "+self._wlcg_exchange_cache)
                         self.publish("file-close", event, exchange=self._wlcg_exchange_cache)
                     else:
                         self.logger.info("Sending GStream for "+self._exchange_cache)
                         self.publish("file-close", event, exchange=self._exchange_cache)
                        
                except Exception as e:
                    self.logger.exception("Error on creating Json - event - GStream" + e)

        except Exception as e:
            self.logger.exception("Error on creating Json - GStream" + e)
                                                                                                 
    def process_tcp(self, decoded_packet:decoding.gstream, addr: str):
        """
        Process a TCP stream
        """
        # For each event in the decoded_packet, add the addr, send to the exchange.  
        # The event is already a dict from the parsed JSON
        for event in decoded_packet.events:
            # parse the event from json
            event['from'] = addr
            self.publish('tcp-event', event, exchange=self._tcp_exchange)




    def process(self, data, addr, port):

        self.metrics_q.put({'type': 'packets', 'count': 1})

        # print "Byte Length of Message: {0}".format(len(d))
        header = decoding.header._make(struct.unpack("!cBHI", data[:8]))  # XrdXrootdMonHeader
        # print "Byte Length of Message: {0}, expected: {1}".format(len(d), h.plen)
        if len(data) != header.plen:
            self.logger.error("Packet Length incorrect: expected={}, got={}".format(header.plen, len(data)))

        # print h
        self.logger.debug(header)
        data = data[8:]
        # Summarize current datastructure
        # num_servers = len(AllTransfers)
        # num_users = 0
        # num_files = 0
        # for sid in AllTransfers:
        #     num_users += len(AllTransfers[sid])
        #     for user in AllTransfers[sid]:
        #         num_files += len(AllTransfers[sid][user])

        # print "Servers: {0}, Users: {1}, Files: {2}".format(num_servers, num_users, num_files)

        sid = str(header.server_start) + "#" + str(addr) + "#" + str(port)
        str_header_code = header.code.decode('utf-8')
        if sid not in self.seq_data:
            self.seq_data[sid] = {}
            self.logger.debug("New SID found.  sid=%s, addr=%s", str(sid), addr)

        if str_header_code not in self.seq_data[sid]:
            self.seq_data[sid][str_header_code] = header.pseq
            expected_seq = header.pseq
        else:
            # What is the last seq number we got
            last_seq = self.seq_data[sid][str_header_code]
            expected_seq = (last_seq + 1)

        if expected_seq == 256:
            expected_seq = 0
        if expected_seq != header.pseq:
            if header.pseq < expected_seq:
                # Handle the roll over
                missed_packets = (header.pseq + 255) - expected_seq
            else:
                missed_packets = abs(header.pseq - expected_seq)

            hostname = addr
            try:
                hostname = socket.gethostbyaddr(addr)[0]
            except:
                pass

            # Remove re-ordering errors
            if missed_packets < 253:
                #self.logger.error("Missed packet(s)!  Expected seq=%s, got=%s.  "
                #                    "Missed %s packets! from %s", expected_seq,
                #                    header.pseq, missed_packets, addr)
                self.metrics_q.put({'type': 'missing packets', 'count': missed_packets, 'addr': hostname})
            else:
                #self.logger.error("Packet Reording packet(s)!  Expected seq=%s, got=%s.  "
                #                    "Missed %s packets! from %s", expected_seq,
                #                    header.pseq, missed_packets, addr)
                self.metrics_q.put({'type': 'reordered packets', 'count': 1, 'addr': hostname})
        self.seq_data[sid][str_header_code] = header.pseq

        if header.code == b'f':
            # self.logger.debug("Got fstream object")
            time_record = decoding.MonFile(data)  # first one is always TOD
            self.logger.debug(time_record)
            data = data[time_record.recSize:]

            self.logger.debug("Size of seq_data: %i, Number of sub-records: %i",
                              len(self.seq_data), time_record.total_recs)
            now = time.time()

            for idx in range(time_record.total_recs):
                hd = decoding.MonFile(data)
                data = data[hd.recSize:]
                self.logger.debug(str(hd))

                if isinstance(hd, decoding.fileDisc):
                    try:
                        userInfo = self._dictid_map[sid][hd.userID]
                        del self._users[userInfo]
                        del self._dictid_map[sid][hd.userID]
                    except KeyError:
                        self.logger.error('Disconnect event for unknown UserID=%i with SID=%s',
                                          hd.userID, sid)

                elif isinstance(hd, decoding.fileOpen):
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    self.logger.debug('%i %s', idx, hd)
                    self._transfers[transfer_key] = ((time_record.tBeg, addr), hd)

                elif isinstance(hd, decoding.fileClose):
                    # self.logger.debug('%i %s', i, hd)
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    if transfer_key in self._transfers:
                        userId = self._transfers[transfer_key][1].userID
                        openTime = self._transfers[transfer_key][0][0]
                        rec = self.addRecord(sid, userId, hd, time_record.tBeg, addr, openTime)
                        self.logger.debug("Record to send: %s", str(rec))
                        del self._transfers[transfer_key]
                        self.logger.debug('%i %s', idx, hd)
                    else:
                        rec = self.addRecord(sid, 0, hd, time_record.tBeg, addr, time_record.tBeg)
                        self.logger.error("file to close not found. fileID: %i, serverID: %s. close=%s",
                                          hd.fileID, sid, str(hd))

                elif isinstance(hd, decoding.fileXfr):
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    if transfer_key in self._transfers:
                        cur_value = self._transfers[transfer_key]
                        self._transfers[transfer_key] = ((now, cur_value[0][1]), cur_value[1], hd)
                        self.logger.debug("f-stream index=%i Known xfrInfo: %s. sid=%s", idx, str(hd), sid)
                    else:
                        self.logger.debug("f-stream index=%i Unknown xfrInfo: %s. sid=%s", idx, str(hd),
                                          sid)

            data = data[hd.recSize:]
            if data:
                self.logger.error("Bytes leftover! %i bytes left!", len(data))

        elif header.code == b'r':
            self.logger.debug("r - redirect stream message.")

        elif header.code == b't':
            #self.logger.warning("t - stream message. Server at %s should remove 'files', 'io', and "
            #                    "'iov' directives from the monitoring configuration.", addr)
            pass

        elif header.code == b'g':
        
            # The rest of the message is the gstream event
            self.logger.debug("Received gstream message")
            decoded_gstream = decoding.gStream(data)
            self.metrics_q.put({'type': 'gstream_message', 'count': 1})

            # We only care about the top 8 bits of the ident, which are a character.
            stream_type = chr(decoded_gstream.ident >> 56)
            if stream_type == "T":
                self.process_tcp(decoded_gstream, addr)
            elif stream_type == "C":
                # process the gstream
                self.process_gstream(decoded_gstream, addr,sid)
            elif stream_type == "P":
                self.process_gstream_tpc(decoded_gstream, addr,sid)

        else:
            infolen = len(data) - 4
            mm = decoding.mapheader._make(struct.unpack("!I" + str(infolen) + "s", data))
            try:
                userRec, rest = mm.info.split(b'\n', 1)
            except ValueError:
                if not header.code == b'u':
                    self.logger.exception("Strange >>%s<< mapping message from %s mm: %s",
                                          header.code, addr, mm)
                userRec = mm.info
                rest = b''

            userInfo = decoding.userInfo(userRec)
            self.logger.debug('%i %s', mm.dictID, userInfo)

            if header.code == b'=':
                serverInfo = decoding.serverInfo(rest, addr)
                if sid not in self._servers:
                    self._servers[sid] = serverInfo
                    self.logger.info('Adding new server info: %s started at %i', serverInfo,
                                     header.server_start)

            elif header.code == b'd':
                path = rest
                if sid not in self._dictid_map:
                    self._dictid_map[sid] = ttldict.TTLOrderedDict(default_ttl=DEFAULT_TTL)
                pathInfo = decoding.pathinfo(userInfo, path)
                self.logger.debug("Adding new pathinfo: %s", pathInfo)
                self._dictid_map[sid][mm.dictID] = pathInfo

            elif header.code == b'i':
                appinfo = rest
                if sid not in self._users:
                    self._users[sid] = ttldict.TTLOrderedDict(default_ttl=DEFAULT_TTL)

                if sid not in self._dictid_map:
                    self._dictid_map[sid] = ttldict.TTLOrderedDict(default_ttl=DEFAULT_TTL)
                self._dictid_map[sid][mm.dictID] = userInfo

                # Check if userInfo is available in _users
                if userInfo not in self._users[sid]:
                    self._users[sid][userInfo] = {}
                self._users[sid][userInfo]['appinfo'] = rest
                self.logger.info('appinfo:%s', appinfo)

            elif header.code == b'p':
                purgeInfo = decoding.purgeInfo(rest)
                self.logger.info('purgeInfo:%s', purgeInfo)

            elif header.code == b'u':
                authorizationInfo = decoding.authorizationInfo(rest)
                if authorizationInfo.inetv != b'':
                    self.logger.debug("Inet version detected to be %s", authorizationInfo.inetv)

                # New server seen
                if sid not in self._users:
                    self._users[sid] = ttldict.TTLOrderedDict(default_ttl=DEFAULT_TTL)

                # Add the dictid to the userinfo map
                if sid not in self._dictid_map:
                    self._dictid_map[sid] = ttldict.TTLOrderedDict(default_ttl=DEFAULT_TTL)
                self._dictid_map[sid][mm.dictID] = userInfo
                # New user signed in
                if userInfo not in self._users[sid]:
                    self._users[sid][userInfo] = {
                        'userinfo': userInfo,
                        'authinfo': authorizationInfo
                    }
                    self.logger.debug("Adding new user: %s", authorizationInfo)

                # Seen the user, but not the auth stuff yet
                elif self._users[sid][userInfo] is None:
                    self.logger.warning("Received a user ID (%i) from sid %s after corresponding "
                                        "f-stream usage information.", mm.dictID, sid)
                    self._users[sid][userInfo] = {
                        'userinfo': userInfo,
                        'authinfo': authorizationInfo
                    }

                # Seen the user, and added the stuff
                elif self._users[sid][userInfo]:
                    self._users[sid][userInfo].update({
                        'userinfo': userInfo,
                        'authinfo': authorizationInfo
                    })
                    self.logger.error("Received a repeated userID; SID: %s and UserID: %s (%s).",
                                      sid, mm.dictID, userInfo)

            elif header.code == b'x':
                decoding.xfrInfo(rest)
                # transfer_key = str(sid) + "." + str(xfrInfo.fileID)
                # if transfer_key in AllTransfers:
                #    cur_value = AllTransfers[transfer_key]
                #    AllTransfers[transfer_key] = (time.time(), cur_value[1], xfrInfo)
                #    print "Adding xfrInfo"

                # print xfrInfo

        # Check if we have to flush the AllTransfer
        now_time = time.time()
        if (now_time - self.last_flush) > (60*5):
            self.logger.debug("Flushing data structures")

            dictid_count = 0
            dictid_removed = 0
            # Flush the dictid mapping
            for sid in self._dictid_map:
                removed = self._dictid_map[sid].purge()
                dictid_removed += removed
                dictid_count += len(self._dictid_map[sid])
            self.logger.debug("Removed {} items from DictID Mapping".format(dictid_removed))
            self.metrics_q.put({'type': 'hash size', 'count': dictid_count, 'hash name': 'dictid'})
            self.logger.debug("Size of dictid map: %i", dictid_count)

            # Flush the users data structure
            users_count = 0
            users_removed = 0
            for sid in self._users:
                removed = self._users[sid].purge()
                users_removed += removed
                users_count += len(self._users[sid])
            self.logger.debug("Removed {} items from Users".format(users_removed))
            self.metrics_q.put({'type': 'hash size', 'count': users_count, 'hash name': 'users'})
            self.logger.debug("Size of users map: %i", users_count)

            # Have to make a copy of .keys() because we 'del' as we go
            for key in list(self._transfers.keys()):
                cur_value = self._transfers[key]
                # TODO: since we don't update based on the xfr info, we don't
                # track progress or bump the timestamps... that needs to be done.
                if (now_time - cur_value[0][0]) > DEFAULT_TTL:
                    if len(cur_value) == 3:
                        sid = key.rsplit(".", 1)[0]
                        userId = cur_value[1].userID
                        addr = cur_value[0][1]
                        openTime = cur_value[0][0]
                        rec = self.addRecord(sid, userId, cur_value[2], now_time, addr, openTime)
                    del self._transfers[key]
            self.logger.debug("Size of transfers map: %i", len(self._transfers))
            self.metrics_q.put({'type': 'hash size', 'count': len(self._transfers), 'hash name': 'transfers'})
            self.last_flush = now_time


if __name__ == '__main__':
    DetailedCollector.main()
