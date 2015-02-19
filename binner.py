#!/usr/bin/python
import sys,os

bs=60

class fax:
    def __init__(self):
        self.sites={}
        self.sumout={}
    def getSite(self,name):
        if not name in self.sites:
            print "NEW site", name
            self.sites[name]=site(name)
        return self.sites[name]
    def prnt(self):
        print '-------- Summary --------'
        print ' Sites:', len(self.sites)
        for s in self.sites:
            self.sites[s].prnt()
    def prntBins(self):
        print '-------- FAX total out ---------'
        for bi in sorted(self.sumout.keys()):
            traf=self.sumout[bi]
            print bi*60, float(traf)/1024/1024,"MB\t", float(traf)/1024/1024/bs, 'MB/s'
    def crunch(self):
        lastMeasurements=''
        for s in self.sites:
            lastMeasurements+=self.sites[s].crunch()
        outF=open('.LastValues','w')
        outF.write(lastMeasurements)
        outF.close()
    def sumup(self):
        self.sumout.clear()
        for s in self.sites:
            self.sites[s].sumup()
            for bi,traf in self.sites[s].sumout.iteritems():
                if bi not in self.sumout:
                    self.sumout[bi]=0
                self.sumout[bi]+=traf
                
        
class site:
    def __init__(self,name):
        self.name=name
        self.servers=[]
        self.sumout={}
    def getServer(self,sname,stime):
        for s in self.servers:
            if s.hostname==sname and s.startedat==stime:
                return s
        print "NEW server:",sname,":",stime
        ns=faxserver(self.name,sname,stime)
        self.servers.append(ns)
        return ns
    def prnt(self):
        print 'site:',self.name, "servers:", len(self.servers)
        for s in self.servers:
            s.prnt()
            # s.checkMe()
    def crunch(self):
        lastMeasurements=''
        for s in self.servers:
            s.binit()
            # s.prnt()
            lastMeasurements+=s.getLast()
        return lastMeasurements
    def sumup(self):
        self.sumout.clear()
        for s in self.servers:
            for bi,traf in s.bin.iteritems():
                if bi not in self.sumout:
                    self.sumout[bi]=0
                self.sumout[bi]+=traf
                
class faxserver:
    def __init__(self, s,h,sat):
        self.site=s
        self.hostname=h
        self.startedat=sat
        self.measurements=[]
        self.bin={}  
        self.missing=0
    def binit(self):
        if len(self.measurements)<2: 
            print 'not enough measurements.'
            return;
        fm=self.getFirstMeasurement()
        lm=self.getLastMeasurement()
        for b in range(fm[0]/bs,lm[0]/bs+1):
            self.bin[b]=0
        
        first=True
        for sm in self.measurements:
            if first:
                first=False
                continue
            rft=divmod(fm[0],bs) # gives [a/b, a%b]
            sft=divmod(sm[0],bs)
            
            dsec=sm[0]-fm[0] # seconds between two measurements
            if dsec==0:
                print "0 sec between measurements!"
                print "f:",fm
                print "s:",sm
                dsec=1
            dout=float(sm[3]-fm[3])/dsec #rates in bytes/second
            #din =float(sm[2]-fm[2])/dsec  not needed for now.
            
            if dout<0:
                print "Rollover in outbytes! ",  fm[3], sm[3]
                dout=float(sm[3])/dsec
            
            fb=rft[0]
            lb=sft[0]
            self.bin[fb]+=dout*(bs-rft[1]) #the first minute is never complete 
            fb+=1
            while(fb<lb):
                self.bin[fb]+=dout*bs
                self.missing+=1
                fb+=1
            self.bin[lb]+=dout*sft[1]  #the last minute is never complete
            
            fm=sm
    def checkMe(self):
        fm=self.getFirstMeasurement()
        lm=self.getLastMeasurement()
        print 'Total bytes:', lm[3]-fm[3]
        stotal=0L
        for bi, Transfered in self.bin.iteritems():
            stotal+=Transfered
        print 'SummedUp bytes:',stotal
    def prnt(self):
        print 'site:', self.site,'\tserver:',self.hostname,'\tstarted at:',self.startedat, '\tmeasurements:',len(self.measurements)
        print 'missing:', self.missing
        print 'first:', self.getFirstMeasurement()
        print 'last:', self.getLastMeasurement()
        # for bi, Transfered in self.bin.iteritems():
            # print bi, int(Transfered/1024),"kB\t", Transfered/1024/1024/60,"MB/s"
        print '---------------------------------------'

    def getFirstMeasurement(self):
        if len(self.measurements)<1: return None
        return self.measurements[0]    
    def getLastMeasurement(self):
        if len(self.measurements)<1: return None
        return self.measurements[len(self.measurements)-1]
    def getLast(self):
        if len(self.measurements)<2: 
            return '' 
        return self.hostname+','+self.site+','+str(self.startedat)+','+str(self.getLastMeasurement()[0])+','+str(self.getLastMeasurement()[1])+','+str(self.getLastMeasurement()[2])+','+str(self.getLastMeasurement()[3])+'\n'
    def addMeasurement(self, m):
        self.measurements.append(m)






FAX=fax()

print 'Loading last seen measurements...'
if os.path.isfile('.LastValues'):
    lv = open('.LastValues')
    lines=lv.readlines()
    for l in lines:
        w=l.split(',')
        FAX.getSite(w[1]).getServer(w[0],int(w[2])).addMeasurement([int(w[3]),int(w[4]),long(w[5]),long(w[6])])
    lv.close()
    os.remove('.LastValues')

print 'Loading new measurements...'
f = open('part-r-00000')
lines = f.readlines()
for l in lines:
    cl=l.replace('{(','').replace(')}','').strip()
    ms=cl.split('),(')
    firstRec=ms[0].split(',')
    s=FAX.getSite(firstRec[1]).getServer(firstRec[0],int(firstRec[2]))
    for m in ms:
        vs=m.split(',')
        s.addMeasurement([int(vs[3]),int(vs[4]),long(vs[5]),long(vs[6])]) #TOD,TOE,IN,OUT


FAX.crunch() 
FAX.prnt()
FAX.sumup()
FAX.prntBins()