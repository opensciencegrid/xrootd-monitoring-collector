#!/usr/bin/env python

"""
A simple daemon for collecting monitoring packets from a remote XRootD
host, performing simple aggregations, and forwarding the resulting data
to a message queue.
"""

import json
import logging
import logging.config
import re
import socket
import struct
import sys
import time

from multiprocessing import Process, Queue

import six
from six.moves import configparser

import pika
import decoding
import wlcg_converter

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('DetailedCollector')

if len(sys.argv) != 2:
    logger.error('Usage: DetailedCollector.py <connection.conf>')
    sys.exit(1)
connect_config = configparser.ConfigParser()
connect_config.read(sys.argv[1])

DETAILED_PORT = 9930

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024*4)
sock.bind(("0.0.0.0", DETAILED_PORT))

AllTransfers = {}
AllServers = {}
AllUsers = {}

channel = None

class LoggerWriter(object):
    """
    Set stdout / stderr to the logger
    https://stackoverflow.com/questions/19425736/how-to-redirect-stdout-and-stderr-to-logger-in-python
    """

    def __init__(self, level):
        # self.level is really like using log.debug(message)
        # at least in my case
        self.level = level

    def write(self, message):
        """
        if statement reduces the amount of newlines that are
        printed to the logger
        """
        if message != '\n':
            self.level(message)

    def flush(self):
        """
        create a flush method so things can be flushed when
        the system wants to. Not sure if simply 'printing'
        sys.stderr is the correct way to do it, but it seemed
        to work properly for me.
        """
        self.level(sys.stderr)


def CreateRabbitConnection():
    """
    Create a fresh connection to RabbitMQ
    """
    global channel
    parameters = pika.URLParameters(connect_config.get('AMQP', 'url'))
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()


def addRecord(sid, userID, fileClose, timestamp, addr):
    """
    Given information to create a record, send it up to the message queue.
    """
    rec = {}
    rec['timestamp'] = timestamp*1000 # expected to be in MS since Unix epoch

    try:
        rec['server_hostname'] = socket.gethostbyaddr(addr)[0]
    except:
        pass

    rec['server_ip'] = addr
    if sid in AllServers:
        s = AllServers[sid]
        rec['serverID'] = sid
        rec['server'] = s.addr
        rec['site'] = s.site
    else:
        rec['server'] = addr
        # logger.warning('server still not identified: %s',sid)

    try:
        (u, auth) = AllUsers[sid][userID]
        if u is not None:
            rec['user'] = u.username
            rec['host'] = u.host
            if not re.match(r"^[\[\:f\d\.]+", u.host):
                rec['user_domain'] = ".".join(u.host.split('.')[-2:])
            rec['location'] = decoding.getLongLat(u.host)
        if auth is not None:
            if auth.inetv != '':
                rec['ipv6'] = True if auth.inetv == 6 else False
            if auth.dn != '':
                rec['user_dn'] = auth.dn
    except KeyError:
        logger.error("File close record from unknown UserID=%i, SID=%s", userID, sid)
        AllUsers.setdefault(sid, {})[userID] = None
    except TypeError as e:
        logger.error("File close record from unknown UserID=%i, SID=%s: %s", userID, sid, str(e))
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
        elif f.fileName.startswith('/gwdata'):
            rec['logical_dirname'] = rec['dirname2']
        elif f.fileName.startswith('/chtc/'):
            rec['logical_dirname'] = '/chtc'
        else:
            rec['logical_dirname'] = 'unknown directory'
    else:
        rec['filename'] = "missing directory"
        rec['filesize'] = "-1"
        rec['logical_dirname'] = "missing directory"
    rec['read'] = fileClose.read
    rec['readv'] = fileClose.readv
    rec['write'] = fileClose.write
    
    wlcg_packet = wlcg_converter.Convert(rec)
    logger.debug("WLCG record to send: %s", str(wlcg_packet))

    try:
        channel.basic_publish(connect_config.get('AMQP', 'exchange'),
                              "file-close",
                              json.dumps(rec),
                              pika.BasicProperties(content_type='application/json',
                                                   delivery_mode=1))

        channel.basic_publish(connect_config.get('AMQP', 'wlcg_exchange'),
                              "file-close",
                              json.dumps(wlcg_packet),
                              pika.BasicProperties(content_type='application/json',
                                                   delivery_mode=1))
    except Exception:
        logger.exception('Error while sending rabbitmq message')
        CreateRabbitConnection()
        channel.basic_publish(connect_config.get('AMQP', 'exchange'),
                              "file-close",
                              json.dumps(rec),
                              pika.BasicProperties(content_type='application/json',
                                                   delivery_mode=1))
        channel.basic_publish(connect_config.get('AMQP', 'wlcg_exchange'),
                              "file-close",
                              json.dumps(wlcg_packet),
                              pika.BasicProperties(content_type='application/json',
                                                   delivery_mode=1))

    return rec


def eventCreator(message_q):
    """
    Helper process target for parsing incoming packets and sending them to
    the message queue.
    """

    last_flush = time.time()
    seq_data = {}
    sys.stdout = LoggerWriter(logger.debug)
    sys.stderr = LoggerWriter(logger.error)
    while True:
        [d, addr, port] = message_q.get()

        #print "Byte Length of Message: {0}".format(len(d))
        h = decoding.header._make(struct.unpack("!cBHI", d[:8])) # XrdXrootdMonHeader
        #print "Byte Length of Message: {0}, expected: {1}".format(len(d), h.plen)
        if len(d) != h.plen:
            logger.error("Packet Length incorrect: expected=%s, got=%s", h.plen, len(d))

        #print h
        logger.debug(h)
        d = d[8:]
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

        if h.code == 'f':
            #logger.debug("Got fstream object")
            time_record = decoding.MonFile(d) # first one is always TOD
            logger.debug(time_record)
            d = d[time_record.recSize:]

            if sid not in seq_data:
                seq_data[sid] = h.pseq
                logger.debug("New SID found.  sid=%s, addr=%s", str(sid), addr)
            else:
                # What is the last seq number we got
                last_seq = seq_data[sid]
                expected_seq = (last_seq + 1)
                if expected_seq == 256:
                    expected_seq = 0
                if expected_seq != h.pseq:
                    missed_packets = abs(h.pseq - expected_seq)
                    logger.error("Missed packet(s)!  Expected seq=%s, got=%s.  "
                                 "Missed %s packets! from %s", expected_seq, h.pseq,
                                 missed_packets, addr)
                seq_data[sid] = h.pseq

            logger.debug("Size of seq_data: %i, Number of sub-records: %i",
                         len(seq_data), time_record.total_recs)
            now = time.time()

            for i in range(time_record.total_recs):
                hd = decoding.MonFile(d)
                d = d[hd.recSize:]

                if isinstance(hd, decoding.fileDisc):
                    try:
                        user_info = AllUsers.setdefault(sid, {})
                        del user_info[hd.userID]
                    except KeyError:
                        logger.error('Disconnect event for unknown UserID=%i with SID=%s',
                                     hd.userID, sid)

                elif isinstance(hd, decoding.fileOpen):
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    logger.debug('%i %s', i, hd)
                    AllTransfers[transfer_key] = ((now, addr), hd)

                elif isinstance(hd, decoding.fileClose):
                    #logger.debug('%i %s', i, hd)
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    if transfer_key in AllTransfers:
                        u = AllTransfers[transfer_key][1].userID
                        rec = addRecord(sid, u, hd, time_record.tEnd, addr)
                        logger.debug("Record to send: %s", str(rec))
                        del AllTransfers[transfer_key]
                        logger.debug('%i %s', i, hd)
                    else:
                        rec = addRecord(sid, 0, hd, time_record.tEnd, addr)
                        logger.error("file to close not found. fileID: %i, serverID: %s. close=%s",
                                     hd.fileID, sid, str(hd))

                elif isinstance(hd, decoding.fileXfr):
                    transfer_key = str(sid) + "." + str(hd.fileID)
                    if transfer_key in AllTransfers:
                        cur_value = AllTransfers[transfer_key]
                        AllTransfers[transfer_key] = ((now, cur_value[0][1]), cur_value[1], hd)
                        logger.debug("f-stream index=%i Known xfrInfo: %s. sid=%s", i, str(hd), sid)
                    else:
                        logger.debug("f-stream index=%i Unknown xfrInfo: %s. sid=%s", i, str(hd),
                                     sid)

            d = d[hd.recSize:]
            if d:
                logger.error("Bytes leftover! %i bytes left!", len(d))

        elif h.code == 'r':
            logger.debug("r - redirect stream message.")

        elif h.code == 't':
            logger.warning("t - stream message. Server at %s should remove 'files', 'io', and "
                           "'iov' directives from the monitoring configuration.", addr)

        else:
            infolen = len(d) - 4
            mm = decoding.mapheader._make(struct.unpack("!I" + str(infolen) + "s", d))
            try:
                (u, rest) = mm.info.split('\n', 1)
            except ValueError as e:
                if not h.code == 'u':
                    logger.error('%s', e[0])
                    logger.warning("Strange >>%s<< mapping message from %s mm: %s", h.code, addr,
                                   mm)
                u = mm.info
                rest = ''

            userInfo = decoding.userInfo(u)
            logger.debug('%i %s', mm.dictID, userInfo)

            if h.code == '=':
                serverInfo = decoding.serverInfo(rest, addr)
                if sid not in AllServers:
                    AllServers[sid] = serverInfo
                    logger.info('Adding new server info: %s started at %i', serverInfo,
                                h.server_start)

            elif h.code == 'd':
                path = rest
                logger.warning('Path information sent (%s). Server at %s should remove "files" '
                               'directive from the monitoring configuration.', path, addr)

            elif h.code == 'i':
                appinfo = rest
                logger.info('appinfo:%s', appinfo)

            elif h.code == 'p':
                purgeInfo = decoding.purgeInfo(rest)
                logger.info('purgeInfo:%s', purgeInfo)

            elif h.code == 'u':
                authorizationInfo = decoding.authorizationInfo(rest)
                if authorizationInfo.inetv != '':
                    logger.debug("Inet version detected to be %s", authorizationInfo.inetv)
                if sid not in AllUsers:
                    AllUsers[sid] = {}
                if mm.dictID not in AllUsers[sid]:
                    AllUsers[sid][mm.dictID] = (userInfo, authorizationInfo)
                    logger.debug("Adding new user: %s", authorizationInfo)
                elif AllUsers[sid][mm.dictID] is None:
                    logger.warning("Received a user ID (%i) from sid %s after corresponding "
                                   "f-stream usage information.", mm.dictID, sid)
                    AllUsers[sid][mm.dictID] = (userInfo, authorizationInfo)
                elif AllUsers[sid][mm.dictID]:
                    logger.error("Received a repeated userID; SID: %s and UserID: %s (%s).", sid,
                                 mm.dictID, userInfo)

            elif h.code == 'x':
                decoding.xfrInfo(rest)
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
            # Have to use .keys() because we are 'del' keys as we go
            for key in list(AllTransfers.keys()):
                cur_value = AllTransfers[key]
                # TODO: since we don't update based on the xfr info, we don't
                # track progress or bump the timestamps... that needs to be done.
                if (now_time - cur_value[0][0]) > (3600*5):
                    if len(cur_value) == 3:
                        (sid, _) = key.rsplit(".", 1)
                        u = cur_value[1].userID
                        addr = cur_value[0][1]
                        rec = addRecord(sid, u, cur_value[2], now_time, addr)
                    del AllTransfers[key]

        if message_q.qsize() > 200:
            logger.error('QSize is large: %d', message_q.qsize())


def main():
    """
    Main driver for the daemon.
    """

    CreateRabbitConnection()

    message_q = Queue()

    p = Process(target=eventCreator, args=(message_q,))
    p.daemon = True
    p.start()

    #start eventCreator threads
    #for i in range(1):
    #     t = Thread(target=eventCreator)
    #     t.daemon = True
    #     t.start()

    n_messages = 0

    while True:
        message, addr = sock.recvfrom(65536)

        message_q.put([message, addr[0], addr[1]])
        n_messages += 1
        if n_messages % 10000 == 0:
            logger.info("Current UDP packets processed count: %i", n_messages)

if __name__ == '__main__':
    main()
