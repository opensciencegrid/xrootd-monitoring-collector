#!/usr/bin/python
import datetime,sys

#dan=sys.argv[1].split('-')
#cday=datetime.datetime(int(dan[0]),int(dan[1]),int(dan[2]))
#print "working on:",cday

#bs=datetime.timedelta(0,60)
bs=60

class faxserver:
    def __init__(self):
        self.site=''
        self.hostname=''
        self.startedat=0
        self.measurements=[]
        self.bin={}
        
    def binit(self):
        fm=self.measurements.pop(0)
        lm=self.measurements[len(self.measurements)-1]
        for b in range(fm[0]/bs,lm[0]/bs):
            self.bin[b]=0
            
        for sm in self.measurements:
            rft=divmod(fm[0],bs) # gives [a/b, a%b]
            sft=divmod(sm[0],bs)
            
            dsec=sm[0]-fm[0] # seconds between two measurements
            dout=float(sm[3]-fm[3])/dsec #rates in bytes/second
            din =float(sm[2]-fm[2])/dsec
            
            fb=rft[0]
            lb=sft[0]
            self.bin[fb]+=dout*(bs-rft[1]) #the first minute is never complete 
            fb+=1
            while(fb<lb):
                self.bin[fb]+=dout*bs
                fb+=1
            self.bin[lb]+=dout*sft[1]  #the last minute is never complete
            
            fm=sm
            
    def prnt(self):
        print 'site:',self.site,'\tserver:',self.hostname,'\tstarted at:',self.startedat, '\tmeasurements:',len(self.measurements)
        print 'first:',self.measurements[0]
        print 'last:',self.measurements[len(self.measurements)-1]
        for key, value in self.bin.iteritems():
            print key, value
            
servers=[]

f = open('part-r-00000')
lines=f.readlines()
for l in lines:
    s=faxserver()
    cl=l.replace('{(','').replace(')}','').strip()
    ms=cl.split('),(')
    for m in ms:
        vs=m.split(',')
        s.site=vs[0]
        s.hostname=vs[1]
        s.startedat=vs[2]
        s.measurements.append([int(vs[3]),int(vs[4]),long(vs[5]),long(vs[6])]) #TOD,TOE,IN,OUT
    servers.append(s)

for serv in servers:
    serv.binit()
    serv.prnt()
	 
	 
