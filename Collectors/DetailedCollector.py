#!/usr/bin/env python3

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

import decoding
import wlcg_converter
import UdpCollector
import ttldict

DEFAULT_TTL = 3600*1

class DetailedCollector(UdpCollector.UdpCollector):

    DEFAULT_PORT = 9930
    UDP_MON_PORT = 8000


    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._transfers = {}
        self._servers = {}
        self._users = {}
        self._dictid_map = {}
        self._exchange = self.config.get('AMQP', 'exchange')
        self._wlcg_exchange = self.config.get('AMQP', 'wlcg_exchange')
        self.last_flush = time.time()
        self.seq_data = {}


    def addCacheRecord(self, event, hostname, addr, port):

        rec = {}
        rec['timestamp'] = event.detach_t*1000 # Needed to be in ms

        rec['lfn'] =         event.lfn
        rec['access_cnt'] =  event.access_cnt
        rec['attach_t'] =    event.attach_t
        rec['detach_t'] =    event.detach_t
        rec['size'] =        event.size
        rec['blk_size'] =    event.blk_size
        rec['n_blks'] =      event.n_blks
        rec['n_blks_done'] = event.n_blks_done
        rec['b_hit'] =       event.b_hit
        rec['b_miss'] =      event.b_miss
        rec['b_bypass'] =    event.b_bypass
        rec['hostname'] =    hostname
        rec['addr'] =        addr
        rec['port'] =        port

        self.publish("cache-event", rec, exchange=self._exchange)

        self.logger.debug('Publishing Cache Event: {}'.format(str(rec)))


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

        try:
            rec['server_hostname'] = socket.gethostbyaddr(addr)[0]
        except:
            pass

        rec['server_ip'] = addr
        if sid in self._servers:
            s = self._servers[sid]
            rec['serverID'] = sid
            rec['server'] = s.addr
            rec['site'] = s.site.decode('utf-8')
        else:
            rec['server'] = addr
            # logger.warning('server still not identified: %s',sid)

        try:
            # Get userinfo from the map
            userInfo = self._dictid_map[sid][userID]
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
            self.logger.debug('{}'.format(self._transfers[transfer_key]))
            rec['filename'] = fname
            rec['filesize'] = f.fileSize
        else:
            rec['filename'] = "missing directory"
            rec['filesize'] = "-1"
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
            # TODO break me out into support function
            self.logger.debug("Got fstream object")
            time_record = decoding.MonFile(data)  # first one is always TOD
            self.logger.debug(time_record)
            data = data[time_record.recSize:]

            self.logger.debug("Size of seq_data: %i, Number of sub-records: %i",
                              len(self.seq_data), time_record.total_recs)
            now = time.time()

            for idx in range(time_record.total_recs):
                hd = decoding.MonFile(data)
                data = data[hd.recSize:]

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
            self.logger.warning("t - stream message. Server at %s should remove 'files', 'io', and "
                                "'iov' directives from the monitoring configuration.", addr)
            self.logger.warning("{}".format(data))
            pass

        elif header.code == b'g':
            self.logger.debug('cache header')

            infolen = len(data) - 4
            mm = decoding.mapheader._make(struct.unpack("!I" + str(infolen) + "s", data))

            self.logger.debug(mm)

            cacheInfo = decoding.cacheInfo(mm.info)

            self.logger.debug('Debug Data: {}'.format(str(cacheInfo)))

            try:
                hostname = socket.gethostbyaddr(addr)[0]
            except:
                hostname = 'unresolvable'

            for event in cacheInfo:
                self.addCacheRecord(event, hostname, addr, port)

        else:
            self.logger.debug('Header Code is: {}'.format(header.code.decode('utf-8')))
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
                self.logger.warning('Path information sent (%s). Server at %s should remove "files" '
                                    'directive from the monitoring configuration.', path, addr)
                self.logger.debug('{}'.format(data))

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
                self.logger.debug('Header is x')
                # transfer_key = str(sid) + "." + str(xfrInfo.fileID)
                # if transfer_key in AllTransfers:
                #    cur_value = AllTransfers[transfer_key]
                #    AllTransfers[transfer_key] = (time.time(), cur_value[1], xfrInfo)
                #    print "Adding xfrInfo"

                # print xfrInfo
            else:

                self.logger.debug('Header is now: {}'.format(header.code.decode('utf-8')))

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
