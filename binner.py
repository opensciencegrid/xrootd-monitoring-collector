#!/usr/bin/python

class faxserver:
	site=''   
	hostname=''
	startedat=0
	measurements=[]
	def prnt(self):
		print 'site:',self.site,'\tserver:',self.hostname,'\tstarted at:',self.startedat, '\tmeasurements:',len(self.measurements)

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
        s.measurements.append([vs[3],vs[4],vs[5],vs[6]]) #TOD,TOE,IN,OUT
    servers.append(s)

for serv in servers:
    serv.prnt()
    
	 
