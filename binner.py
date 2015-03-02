#!/usr/bin/python
import sys,os
import requests, json

bs=180  #bin size in seconds

class fax:
    def __init__(self):
        self.sites={}
        self.sumout={}
        self.data={}
        self.data['siteNames']=[]
        self.data['bins']=[]
        self.data['values']=[]
        
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
            print bi, float(traf)/1024/1024/bs
        for s in self.sites:
            self.sites[s].prntBins()
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
        
        # creating data to upload
        self.data['bins']=sorted(self.sumout.keys())
        
        bibi={} # dictionary of bin positions to speed up lookup.
        for i,bi in enumerate(self.data['bins']): 
            bibi[bi]=i
            self.data['values'].append([])
            
        for s in self.sites.values():
            self.data['siteNames'].append(s.name)
            for i,bi in enumerate(self.data['bins']):
                if bi in s.sumout:
                    self.data['values'][i].append(s.sumout[bi]/1024/1024/bs)
                else:
                    self.data['values'][i].append(-1)
                
        
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
    def prntBins(self):
        print '-------- FAX '+self.name+' out ---------'
        for bi in sorted(self.sumout.keys()):
            traf=self.sumout[bi]
            print bi, float(traf)/1024/1024/bs
    def crunch(self):
        lastMeasurements=''
        for s in self.servers:
            s.binit()
            #s.prnt()
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
    def __init__(self, s, h, sat):
        self.site=s
        self.hostname=h
        self.startedat=sat
        self.measurements=[]
        self.bin={}  
    def binit(self):
        if len(self.measurements)<2: 
            print 'not enough measurements.'
            return;
        fm=self.getFirstMeasurement()
        lm=self.getLastMeasurement()
        fb=fm[0]/bs*bs #beginning of the first bin
        lb=lm[0]/bs*bs #beginning of the last bin
        
        for b in range(fb,lb+1,bs): #initialize bins
            self.bin[b]=0
        
        for b in range(fb,lb+1,bs):  #loop over bins
            bb=b     #bin begin
            be=b+bs  #bin end
            for m in range(len(self.measurements)-1):   #loop over measurements
                mb=self.measurements[m][0]   #measurement interval begin
                if mb>=be: break  # measurement interval after bin end - all other measurements are even later
                me=self.measurements[m+1][0] #measurement interval end
                if me<=bb: continue  #measurement interval before bin starts
                dsec=me-mb #measurement interval duration in seconds
                if dsec==0:
                    print "warning 0 interval between two measurements."
                    continue
                dout=float(self.measurements[m+1][3]-self.measurements[m][3])/dsec  #data rate in bytes/s during this measurement interval
                # if dout<2147483647:
                    # print "server dateout rolled over", self.measurements[m][3], '->' , self.measurements[m+1][3]
                    # dout=float(self.measurements[m+1][3])/dsec
                if mb<=bb and me>=be: #measurement interval completely covers bin
                    self.bin[bb]+=bs*dout
                    break
                if mb>=bb and me<=be: #measurement interval completely inside bin (no double counting as previous if breaks)
                    self.bin[bb]+=dsec*dout
                    continue
                if mb<bb and me<be:  #tail of the measurement interval inside bin
                    self.bin[bb]+=(me-bb)*dout
                    continue
                if mb>bb and me>be:  #head of the measurement interval inside bin
                    self.bin[bb]+=(be-mb)*dout
                    break
        
    def checkMe(self):
        fm=self.getFirstMeasurement()
        lm=self.getLastMeasurement()
        print 'Total bytes:', lm[3]-fm[3]
        stotal=0L
        for bi, Transfered in self.bin.iteritems():
            stotal+=Transfered
        print 'SummedUp bytes:',stotal
    def prnt(self):
        print 'site:', self.site,'\tserver:',self.hostname,'\tstarted at:',self.startedat,'\tmeasurements:',len(self.measurements)
        print 'first:', self.getFirstMeasurement()
        print 'last:', self.getLastMeasurement()
        # for bi in sorted(self.bin.keys()):
        #     traf=self.bin[bi]
        #     print bi, float(traf)/1024/1024/bs
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
    s=FAX.getSite(firstRec[1]).getServer(firstRec[0],int(firstRec[2]) )
    for m in ms:
        vs=m.split(',')
        s.addMeasurement([int(vs[3]),int(vs[4]),long(vs[5]),long(vs[6])]) #TOD,TOE,IN,OUT


FAX.crunch() 
FAX.prnt()
FAX.sumup()
#FAX.prntBins()
print FAX.data

GAEurl= 'http://1-dot-waniotest.appspot.com/summarymonitor'
data = json.dumps({'name':'test', 'description':'some test repo'}) 
headers = {'content-type': 'application/json'}
#data={ "siteNames":["MWT2","AGLT2", "WT2"], "bins":[10,20,30,40], "values":[[1,2,3],[3,2,1],[1,3,2],[2,1,3]] }
r = requests.post(GAEurl, data=json.dumps(FAX.data), headers=headers)
print r.text