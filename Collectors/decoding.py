
from collections import namedtuple
import json
import requests
import struct
import sys
import urllib.parse

header = namedtuple("header", ["code", "pseq", "plen", "server_start"])
mapheader = namedtuple("mapheader", ["dictID", "info"])

userid   = namedtuple("userid", ["protocol", "username", "pid", "sid", "host"])
authinfo = namedtuple("authinfo", ["ap", "dn", "hn", "on", "rn", "gn", "info", 'execname', 'moninfo', "inetv"])
cacheinfo = namedtuple("cacheinfo", ["event", "lfn", "size", "blk_size", "n_blks", "n_blks_done", "access_cnt", "attach_t", "detach_t", "b_hit", "b_miss", "b_bypass", "remotes"])
srvinfo  = namedtuple("srvinfo", ["program", "version", "instance", "port", "site", "addr"])
prginfo  = namedtuple("prginfo", ["xfn", "tod", "sz", "at", "ct", "mt", "fn"])
xfrinfo  = namedtuple("xfrinfo", ["lfn", "tod", "sz", "tm", "op", "rc", "pd"])

fileOpen  = namedtuple("fileOpen",  ["rectype", "recFlag", "recSize", "fileID", "fileSize", "userID", "fileName"])
fileXfr   = namedtuple("fileXfr",   ["rectype", "recFlag", "recSize", "fileID", "read", "readv", "write"])
fileClose = namedtuple("fileClose", ["rectype", "recFlag", "recSize", "fileID", "read", "readv", "write", "ops"])
fileTime  = namedtuple("fileTime",  ["rectype", "recFlag", "recSize", "isXfr_recs", "total_recs", "tBeg", "tEnd", "sid"])
fileDisc  = namedtuple("fileDisc",  ["rectype", "recFlag", "recSize", "userID"])
ops       = namedtuple("ops", ["read", "readv", "write", "rsMin", "rsMax", "rsegs", "rdMin", "rdMax", "rvMin", "rvMax", "wrMin", "wrMax"])


def cacheInfo(message):
    #print('cacheInfo message: {}'.format(message))
    this_msg = str(message)
    datum=[]
    #print('msg_len: {}'.format(len(this_msg.split("\\n"))))
    for msg in this_msg.split("\\n"):
        start_json=msg.rfind('{')
        end_json=msg.rfind('}')
        json_content=msg[start_json:end_json+1]
        #print('Json_Content: {}'.format(json_content))
        datum.append(cacheinfo(**(json.loads(json_content))))
    return datum


def userInfo(message):
    #print('userInfo message: {}'.format(message))
    c = message
    if b'/' in message:
        prot, c = message.split(b'/', 1)
    hind = c.rfind(b'@')
    host = c[hind + 1:]
    c = c[:hind]
    c, sid = c.split(b':', 1)
    hind = c.rfind(b'.')
    pid = c[hind + 1:]
    user = c[:hind]
    pi = 0
    si = 0
    try:
        pi = int(pid)
        si = int(sid)
    except ValueError:
        print("serious value error: ", pid, sid, "message was:", message)
    return userid(prot, user, pi, si, host)


def revUserInfo(useridStruct):
    return str.encode("{}/{}.{}:{}@{}".format(useridStruct.protocol, useridStruct.username, useridStruct.pid, useridStruct.sid, useridStruct.host))


def authorizationInfo(message):
    if isinstance(message, str):
        message = message.encode('utf-8')
    r = message.split(b'&')
    ap = dn = hn = on = rn = gn = info = execname = moninfo = inetv = b''
    for i in r:
        kv = i.split(b'=', 1)
        if len(kv) == 2:
            if kv[0] == b'p':
                ap = kv[1]
            elif kv[0] == b'n':
                dn = kv[1]
            elif kv[0] == b'h':
                hn = kv[1]
            elif kv[0] == b'o':
                on = kv[1]
            elif kv[0] == b'r':
                rn = kv[1]
            elif kv[0] == b'g':
                gn = kv[1]
            elif kv[0] == b'm':
                info = kv[1]
            elif kv[0] == b'I':
                inetv = kv[1]
            elif kv[0] == b'x':
                execname = kv[1]
            elif kv[0] == b'y':
                moninfo = kv[1]
    return authinfo(ap, dn, hn, on, rn, gn, info, execname, moninfo, inetv)

def revAuthorizationInfo(authinfo):
    # ["ap", "dn", "hn", "on", "rn", "gn", "info", 'execname', 'moninfo', "inetv"])
    # [&p=ap&n=[dn]&h=[hn]&o=[on]&r=[rn]&g=[gn]&m=[info]][loginfo]
    # &x=[xeqname]&y=[minfo]&I={4|6}
    message = "&p=" + authinfo.ap
    message += "&n=" + authinfo.dn
    message += "&h=" + authinfo.hn
    message += "&o=" + authinfo.on
    message += "&r=" + authinfo.rn
    message += "&g=" + authinfo.gn
    message += "&m=" + authinfo.info
    message += "&x=" + authinfo.execname
    message += "&y=" + authinfo.moninfo
    message += "&I=" + authinfo.inetv

    return str.encode(message)

def serverInfo(message, addr):
    r = dict(urllib.parse.parse_qsl(message))
    pgm  = r.get(b'pgm',  None)
    ver  = r.get(b'ver',  None)
    inst = r.get(b'inst', None)
    port = r.get(b'port', None)
    site = r.get(b'site', None)
    return srvinfo(pgm, ver, inst, port, site, addr)

def revServerInfo(serverInfoStruct):
    """
    &pgm=prog&ver=vname&inst=iname&port=pnum&site=sname
    ["program", "version", "instance", "port", "site", "addr"]
    """
    message = "&pgm=" + serverInfoStruct.program
    message += "&ver=" + serverInfoStruct.version
    message += "&inst=" + serverInfoStruct.instance
    message += "&port=" + serverInfoStruct.port
    message += "&site=" + serverInfoStruct.site
    return str.encode(message)


def purgeInfo(message):
    xfn, rest = message.split(b'\n')
    r = rest.split(b"&")
    tod = r[1].split(b'=')[1]
    sz = r[2].split(b'=')[1]
    at = r[3].split(b'=')[1]
    ct = r[4].split(b'=')[1]
    mt = r[5].split(b'=')[1]
    fn = r[6].split(b'=')[1]
    return prginfo(xfn, tod, sz, at, ct, mt, fn)


def xfrInfo(message):
    lfn, rest = message.split(b'\n')
    r = rest.split(b"&")
    tod = r[1].split(b'=')[1]
    sz = r[2].split(b'=')[1]
    tm = r[3].split(b'=')[1]
    op = r[4].split(b'=')[1]
    rc = r[5].split(b'=')[1]
    if len(r) == 7:
        pd = r[6].split(b'=')[1]
    else:
        pd = b''
    return xfrinfo([lfn, tod, sz, tm, op, rc, pd])


def MonFile(d):
    up = struct.unpack("!BBHI", d[:8])  # XrdXrootdMonHeader

    if up[0] == 0:  # isClose
        recOps = ()
        if up[1] & 0b010:  # hasOPS
            recOps = ops._make(struct.unpack("!IIIHHQIIIIII", d[32:80]))
        #if up[1] & 0b100:  # hasSSQ
        # forced Disconnect prior to close  forced =0x01, hasOPS =0x02, hasSSQ =0x04
        unpacked = struct.unpack("!BBHIQQQ", d[:32])
        unpacked = unpacked + (recOps,)
        return fileClose._make(unpacked)
    elif up[0] == 1:  # isOpen
        fO = struct.unpack("!BBHIQ", d[:16])
        if up[1] == 1:
            userId = struct.unpack("!I", d[16:20])[0]
            fileName = struct.unpack("!" + str(up[2] - 20) + "s", d[20:up[2]])[0].rstrip(b'\0')
        else:
            userId = 0
            fileName = b''
        return fileOpen._make(fO + (userId, fileName))
    elif up[0] == 2:  # isTime
        if up[2] == 16:  # this and next 3 lines can be removed after the fixed montiring stream is deployed.
            t = struct.unpack("!BBHHHII", d[:16])
            return fileTime(t[0], t[1], t[2], t[3], t[4], 0, 0, t[5], t[6])
        else:
            return fileTime._make(struct.unpack("!BBHHHIIQ", d[:24]))
    elif up[0] == 3:  # isXfr
        # print "isXfr ..."
        return fileXfr._make(struct.unpack("!BBHIQQQ", d[:32]))
    else:  # isDisc up[0]==4
        return fileDisc._make(up)


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
