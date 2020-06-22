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

import decoding
import wlcg_converter
import UdpCollector
import ttldict


class DetailedCollector(UdpCollector.UdpCollector):

    DEFAULT_PORT = 9930


    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._transfers = {}
        self._servers = {}
        self._users = {}
        self._exchange = self.config.get('AMQP', 'exchange')
        self._wlcg_exchange = self.config.get('AMQP', 'wlcg_exchange')
        self.last_flush = time.time()


    def addRecord(self, sid, userID, fileClose, timestamp, addr):
        """
        Given information to create a record, send it up to the message queue.
        """
        rec = {}
        lcg_record = False
        rec['timestamp'] = timestamp*1000  # expected to be in MS since Unix epoch

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
            u = self._users[sid][userID].get('userinfo', None)
            auth = self._users[sid][userID].get('authinfo', None)
            appinfo = self._users[sid][userID].get('appinfo', None)

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
            if appinfo is not None:
                rec['appinfo'] = appinfo
                    
        except KeyError:
            self.logger.error("File close record from unknown UserID=%i, SID=%s", userID, sid)
            self._users.setdefault(sid, {})[userID] = None
        except TypeError as e:
            self.logger.exception("File close record from unknown UserID=%i, SID=%s", userID, sid)
            self._users.setdefault(sid, {})[userID] = None
        except AttributeError:
            self.logger.exception("File close record from unknown UserID=%i, SID=%s", userID, sid)
            self._users.setdefault(sid, {})[userID] = None
        transfer_key = str(sid) + "." + str(fileClose.fileID)
        if transfer_key in self._transfers:
            f = self._transfers[transfer_key][1]
            fname = f.fileName.decode('utf-8')
            rec['filename'] = fname
            rec['filesize'] = f.fileSize
            rec['dirname1'] = "/".join(fname.split('/', 2)[:2])
            rec['dirname2'] = "/".join(fname.split('/', 3)[:3])
            if fname.startswith('/user'):
                rec['logical_dirname'] = rec['dirname2']
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


        if not lcg_record:
            self.logger.debug("OSG record to send: %s", str(rec))
            self.publish("file-close", rec, exchange=self._exchange)
        else:
            wlcg_packet = wlcg_converter.Convert(rec)
            self.logger.debug("WLCG record to send: %s", str(wlcg_packet))
            self.publish("file-close", wlcg_packet, exchange=self._wlcg_exchange)

        return rec


    def process(self, data, addr, port):

        seq_data = {}
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

        if header.code == b'f':
            # self.logger.debug("Got fstream object")
            time_record = decoding.MonFile(data)  # first one is always TOD
            self.logger.debug(time_record)
            data = data[time_record.recSize:]

            if sid not in seq_data:
                seq_data[sid] = header.pseq
                self.logger.debug("New SID found.  sid=%s, addr=%s", str(sid), addr)
            else:
                # What is the last seq number we got
                last_seq = seq_data[sid]
                expected_seq = (last_seq + 1)
                if expected_seq == 256:
                    expected_seq = 0
                if expected_seq != header.pseq:
                    missed_packets = abs(header.pseq - expected_seq)
                    self.logger.error("Missed packet(s)!  Expected seq=%s, got=%s.  "
                                      "Missed %s packets! from %s", expected_seq,
                                      header.pseq, missed_packets, addr)
                    self.metrics_q({'type': 'missing packets', 'count': missed_packets})
                seq_data[sid] = header.pseq

            self.logger.debug("Size of seq_data: %i, Number of sub-records: %i",
                              len(seq_data), time_record.total_recs)
            now = time.time()

            for idx in range(time_record.total_recs):
                hd = decoding.MonFile(data)
                data = data[hd.recSize:]

                if isinstance(hd, decoding.fileDisc):
                    try:
                        user_info = self._users.setdefault(sid, {})
                        del user_info[hd.userID]
                    except KeyError:
                        self.logger.error('Disconnect event for unknown UserID=%i with SID=%s',
                                          hd.userID, sid)

                elif isinstance(hd, decoding.fileOpen):
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    self.logger.debug('%i %s', idx, hd)
                    self._transfers[transfer_key] = ((now, addr), hd)

                elif isinstance(hd, decoding.fileClose):
                    # self.logger.debug('%i %s', i, hd)
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    if transfer_key in self._transfers:
                        userId = self._transfers[transfer_key][1].userID
                        rec = self.addRecord(sid, userId, hd, time_record.tEnd, addr)
                        self.logger.debug("Record to send: %s", str(rec))
                        del self._transfers[transfer_key]
                        self.logger.debug('%i %s', idx, hd)
                    else:
                        rec = self.addRecord(sid, 0, hd, time_record.tEnd, addr)
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
                rest = ''

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

            elif header.code == b'i':
                appinfo = rest
                if sid not in self._users:
                    self._users[sid] = ttldict.TTLOrderedDict(default_ttl=3600*5)
                if mm.dictID not in self._users[sid]:
                    self._users[sid][mm.dictID] = {}
                self._users[sid][mm.dictID]['appinfo'] = rest
                self.logger.info('appinfo:%s', appinfo)

            elif header.code == b'p':
                purgeInfo = decoding.purgeInfo(rest)
                self.logger.info('purgeInfo:%s', purgeInfo)

            elif header.code == b'u':
                authorizationInfo = decoding.authorizationInfo(rest)
                if authorizationInfo.inetv != b'':
                    self.logger.debug("Inet version detected to be %s", authorizationInfo.inetv)
                if sid not in self._users:
                    self._users[sid] = ttldict.TTLOrderedDict(default_ttl=3600*5)
                if mm.dictID not in self._users[sid]:
                    self._users[sid][mm.dictID] = {
                        'userinfo': userInfo,
                        'authinfo': authorizationInfo
                    }
                    self.logger.debug("Adding new user: %s", authorizationInfo)
                elif self._users[sid][mm.dictID] is None:
                    self.logger.warning("Received a user ID (%i) from sid %s after corresponding "
                                        "f-stream usage information.", mm.dictID, sid)
                    self._users[sid][mm.dictID] = {
                        'userinfo': userInfo,
                        'authinfo': authorizationInfo
                    }
                elif self._users[sid][mm.dictID]:
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

            # Flush the users data structure
            for sid in self._users:
                removed = self._users[sid].purge()
                self.logger.debug("Removed {} items from Users".format(removed))

            # Have to make a copy of .keys() because we 'del' as we go
            for key in list(self._transfers.keys()):
                cur_value = self._transfers[key]
                # TODO: since we don't update based on the xfr info, we don't
                # track progress or bump the timestamps... that needs to be done.
                if (now_time - cur_value[0][0]) > (3600*5):
                    if len(cur_value) == 3:
                        sid = key.rsplit(".", 1)[0]
                        userId = cur_value[1].userID
                        addr = cur_value[0][1]
                        rec = self.addRecord(sid, userId, cur_value[2], now_time, addr)
                    del self._transfers[key]
            self.last_flush = now_time


if __name__ == '__main__':
    DetailedCollector.main()
