#!/usr/bin/python
import datetime

dan=sys.argv[1].split('-')
cday=datetime.datetime(dan[0],dan[1],dan[2])
print "working on:",cday

bs=datetime.timedelta(0,60)

class faxserver:
    def __init__(self):
        self.site=''
        self.hostname=''
    	self.startedat=0
        self.measurements=[]
        self.obins=[0]*1440
        self.ibins=[0]*1440
	def prnt(self):
		print 'site:',self.site,'\tserver:',self.hostname,'\tstarted at:',self.startedat, '\tmeasurements:',len(self.measurements)
        print 'first:',self.measurements[0]
        print 'last:',self.measurements[len(self.measurements)-1]
    def binit(self):
        fm=self.measuremets.pop(0)
        for sm in self.measurements:

            fdt=datetime.datetime.fromtimestamp(fm[0])
            fmin=fdt.hour*60+mdt.minute
            fsec=fdt.second
            
            sdt=datetime.datetime.fromtimestamp(sm[0])
            smin=sdt.hour*60+mdt.minute
            ssec=sdt.second
            
            dsec=smin*60+ssec-fsec*60-fsec # seconds between two measurements
            
            dout=float(sm[3]-fmin[3])/dsec #rates in bytes/second
            din =float(sm[2]-fmin[2])/dsec
            

servers=[]

f = open('part-r-00000')
lines=f.readlines()
for l in lines:
    s=faxserver()
    cl=l.replace('{(','').replace(')}','').strip()
    ms=cl.split('),(')
    print len(ms)
    for m in ms:
        vs=m.split(',')
        s.site=vs[0]
        s.hostname=vs[1]
        s.startedat=vs[2]
        s.measurements.append([vs[3],vs[4],vs[5],vs[6]]) #TOD,TOE,IN,OUT
    servers.append(s)

for serv in servers:
    serv.prnt()
    
	 
