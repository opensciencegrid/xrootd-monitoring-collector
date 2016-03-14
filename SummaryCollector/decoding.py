from collections import namedtuple
import struct

userid   = namedtuple("userid",["username","pid","sid","host"])
authinfo = namedtuple("authinfo",["ap","dn","hn","on","rn","gn","info"])
srvinfo  = namedtuple("srvinfo",["program","version","instance","port","site"])
prginfo  = namedtuple("prginfo",["xfn","tod","sz","at","ct","mt","fn"])
xfrinfo  = namedtuple("xfrinfo",["lfn","tod","sz","tm","op","rc","pd"])

# fileHDR  = namedtuple()
fileHDR   = namedtuple("fileHDR",  ["rectype","recFlag","recSize","fileID"])
fileClose = namedtuple("fileClose",["rectype","recFlag","recSize","fileID","read","readv","write"])
fileOpen  = namedtuple("fileOpen", ["rectype","recFlag","recSize","fileID", "fileSize","user","fileName"])
fileTime  = namedtuple("fileTime", ["rectype","recFlag","recSize","isXfr_recs","total_recs","tBeg","tEnd"])
fileDisc  = namedtuple("fileDisc", ["rectype","recFlag","recSize","userID"])
ops=namedtuple("ops",["read","readv","write","rsMin","rsMax","rsegs","rdMin","rdMax","rvMin","rvMax","wrMin","wrMax"])

def userInfo(message):
    prot,c = message.split('/',1)
    user,c  =c.split('.',1)
    pid,c   =c.split(':',1)
    sid,host=c.split('@',1)
    return userid(user,int(pid),int(sid),host)

def authorizationInfo(message):
    r=message.split('&')
    ap=dn=hn=on=rn=gn=m=''
    for i in r:
        kv=i.split('=')
        if len(kv)==2:
            if kv[0]=='p':   ap=kv[1]
            elif kv[0]=='n': dn=kv[1]
            elif kv[0]=='h': hn=kv[1]
            elif kv[0]=='o': on=kv[1]
            elif kv[0]=='r': rn=kv[1]
            elif kv[0]=='g': gn=kv[1]
            elif kv[0]=='m': m =kv[1]
    return authinfo(ap,dn,hn,on,rn,gn,m)

def serverInfo(message):
    r=message.split('&')
    pgm =r[1].split('=')[1]
    ver =r[2].split('=')[1]
    ints=r[3].split('=')[1]
    port=r[4].split('=')[1]
    site=r[5].split('=')[1]
    return srvinfo(pgm,ver,ints,port,site)
    
def purgeInfo(message):
    xfn,rest=message.split('\n')
    r=rest.split("&")
    tod=r[1].split('=')[1]
    sz =r[2].split('=')[1]
    at =r[3].split('=')[1]
    ct =r[4].split('=')[1]
    mt =r[5].split('=')[1]
    fn =r[6].split('=')[1]
    return prginfo(xfn,tod,sz,at,ct,mt,fn)

def xfrInfo(message):
    lfn, rest==message.split('\n')
    r=rest.split("&")
    tod=r[1].split('=')[1]
    sz =r[2].split('=')[1]
    tm =r[3].split('=')[1]
    op =r[4].split('=')[1]
    rc =r[5].split('=')[1]
    if (len(r)==7):
        pd =r[6].split('=')[1]
    else:
        pd = ''
    return  xfrinfo([lfn,tod,sz,tm,op,rc,pd])
    
def MonFile(d):
    up=struct.unpack("!BBHI",d[:8]) # XrdXrootdMonHeader
    
    if up[0]==0: # isClose
        if up[1] & 0b010:  #hasOPS
            O=ops._make(struct.unpack("!IIIHHQIIIIII",d[32:80]))
            print O
        else:
            O=()
        #forced Disconnect prior to close  forced =0x01, hasOPS =0x02, hasSSQ =0x04,
        #hasOPS XrdXroodMonFileOPS present
        #hasSSQ XrdXroodMonFileSSQ present
        
        return fileClose._make(struct.unpack("!BBHIQQQ",d[:32]))
        
    if up[0]==1: # isOpen
        fO=struct.unpack("!BBHIQ",d[:16])
        if up[1]==1:
            userId=struct.unpack("!I",d[16:20])
            fileName=struct.unpack("!"+str(up[2]-20)+"s",d[20:up[2]])
        else:
            userId=0
            fileName=''
        return fileOpen._make(fO + (userId,fileName))
    if up[0]==2: # isTime
        return fileTime._make(struct.unpack("!BBHHHII",d[:16]))
    if up[0]==4: # isDisc
        return fileDisc._make(up)
    return fileHDR._make(up)
    
    # isClose = 0,   // Record for close
    # isOpen =1,        // Record for open
    # isTime =2 ,        // Record for time
    # isXfr =3,         // Record for transfers
    # isDisc = 4         // Record for disconnection
    