from __future__ import division
import utm
import numpy as np
import matplotlib.pyplot as plt 
import pandas as pd
import sys
from datetime import datetime as dt
from datetime import timedelta
filename=sys.argv[1]
routes=pd.read_csv('../OC/routes.txt')
trips=pd.read_csv('../OC/trips.txt')
stoptime=pd.read_csv('../OC/stop_times.txt')
stops=pd.read_csv('../OC/stops.txt')
print("1. finish data loading :) ")
routes_set=['472','59','178','79','213','167','473']

data=[[] for i in range(len(routes_set))]
#print(routes_set[0])
for i in range (len(routes_set)):
  for index,row in trips.iterrows():
   if(str(row['route_id'])== routes_set[i]):
        data[i].append(row['trip_id'])   
  
dic=[]
for i in range(len(routes_set)):
   L = [i]*len(data[i])
   dic.append(dict(zip(data[i],L)))

super_dict = {}
for d in dic:
    for k, v in d.items():
        l=super_dict.setdefault(k,[])
        if v not in l:
            l.append(v)   # merge the 7 dictionary to one. 

triproute=[[] for i in range (len(routes_set))]
column=['arrival_time','trip_id', 'stop_id']
for index, row in stoptime.iterrows():
    for  i in range (len(routes_set)):
     if(super_dict.get(row['trip_id'])==[i]):
        tmp=stoptime.ix[index,['arrival_time','trip_id','stop_id']]
        tmp['route_id']=routes_set[i]
        tmp['x']='1';
        tmp['y']='1';
       # print(tmp)
        triproute[i].append(tmp)  
trippd=[] #drop duplicated 
for i in range (len(routes_set)):
    trippd.append(pd.DataFrame(triproute[i]))
for i in range (len(routes_set)):
    trippd[i].drop_duplicates(subset=['arrival_time','trip_id','stop_id'],keep=False)
'''
for i in range (len(routes_set)):
     str ="".join(('../data/raw_trip/trip',routes_set[i],'.txt'))
     trippd[i].to_csv(str, header=True,
                      index=False, sep=' ', encoding='utf-8', ) 
'''

#Create a extracted one.
print("2. Doing time formatting") # For tracking
#  First order according to arrival time 
# tranform time to integer 
from datetime import datetime as dt
from datetime import timedelta
min=np.min( trippd[0]['arrival_time'])
for i in range (len(routes_set)):
   if(min>np.min( trippd[i]['arrival_time'])):
      min=np.min( trippd[i]['arrival_time'])
mintime= dt.strptime(min, "%H:%M:%S")


tripcopy=trippd.copy()
triptime=[[]for i in range(len(routes_set))]
for i in range(len(routes_set)):
    for index,row in tripcopy[i].iterrows():
        tptime=dt.strptime(row['arrival_time'],"%H:%M:%S")
        td=(tptime-mintime) / timedelta (seconds=1)
        tmp=tripcopy[i].ix[index,['arrival_time','trip_id',
                                                'stop_id']];
        tmp['arrival_time']=td
        triptime[i].append(tmp)
'''
for i in range(len(routes)):
    str ="".join(('../data/1220/unordered/trip',routes_set[i],'.txt'))
    pd.DataFrame(triptime[i]).to_csv(str, header=True,index=False, sep=' ',
                                     encoding='utf-8', ) 
'''

print("3. Begin to order data !")
old=[[]for i in range(len(routes_set))];
ortrip=[[]for i in range(len(routes_set))]

for i in range(len(routes_set)):
    tmp=pd.DataFrame(triptime[i]).sort_values(by=['arrival_time'])
    str ="".join(('../data/1220/ordered/trip',routes_set[i],'.txt'))
# tmp.to_csv(str,header=True,sep=' ',index=False,encoding='utf-8')
    ortrip[i]=tmp# ortrip is a ordered trips 


#######process location 
print("4. Begin to process location !")
#manipulation for stops 
stop_id=[];stop_lat=[];stop_lon=[];location=[]
for index,row in stops.iterrows():
    stop_id.append(row['stop_id'])
    stop_lat.append(row['stop_lat'])
    stop_lon.append(row['stop_lon'])
location=[];x=[];y=[]
for i in range(len(stop_lon)):
    location.append(utm.from_latlon(stop_lat[i], stop_lon[i]))
    x.append(round(location[i][0],2))
    y.append(round(location[i][1],2))
loc=list(zip(x,y))
for i in range(len(location)):
    locdic=dict(zip(stop_id,loc))

tripout=[[]for i in range(len(routes_set))]
for i in range(len(routes_set)):
    for index,row in ortrip[i].iterrows():
        tmp=ortrip[i].ix[index,['arrival_time','trip_id','stop_id']]
        tmp['x']=locdic.get(row['stop_id'])[0];
        tmp['y']=locdic.get(row['stop_id'])[1];     
        tripout[i].append(tmp)
'''
for i in range(len(routes_set)):
    tmp=pd.DataFrame(tripout[i])
    str ="".join(('../data/1220/location/trip',routes_set[i],'.txt'))
    tmp.to_csv(str,header=True,sep=' ',index=False,encoding='utf-8')
'''
################ tripout is the lastest trip for each toures. Next, get location boundary


minx=int(np.min(pd.DataFrame(tripout[0])['x']))
miny=int(np.min(pd.DataFrame(tripout[0])['y']))
for i in range(len(routes_set)):
    tmpx=np.min(pd.DataFrame(tripout[i])['x'])
    tmpy=np.min(pd.DataFrame(tripout[i])['y'])
    if(minx>tmpx): 
        minx=tmpx
    if(miny>tmpy):
        miny=tmpy
print("5. Begin to merge tirps !")
########## Merge them 
merge=[]
for i in range (len(routes_set)):
    merge.append(pd.DataFrame(tripout[i]))
mergetrip=pd.concat(merge)
tripmerge=mergetrip.sort_values(by=['arrival_time'])
#str ="".join(('../data/1220/merge/trip','ordered','.txt'))
#tripmerge.to_csv(str,header=True,sep=' ',index=False,encoding='utf-8')

tbound=[] #get time boundary
try22=[]
for i in range (len(routes_set)):
    tbound.append(pd.DataFrame(triproute[i]))
for i in range (len(routes_set)):
    try22.append(tbound[i].drop_duplicates(subset=['arrival_time','trip_id','stop_id'],
                                           keep=False))
maxmin=np.min(try22[0]['arrival_time'])
minmax=np.max(try22[0]['arrival_time'])
for i in range(len(routes_set)):
    if(maxmin<np.min(try22[i]['arrival_time'])):
        maxmin=np.min(try22[i]['arrival_time'])
    if(minmax>np.max(try22[i]['arrival_time'])):
        minmax=np.max(try22[i]['arrival_time'])
min=np.min(try22[0]['arrival_time'])
for i in range (len(routes_set)):
   if(min>np.min( try22[i]['arrival_time'])):
      min=np.min( try22[i]['arrival_time'])
mintime= dt.strptime(min, "%H:%M:%S")
minbound=dt.strptime(maxmin,"%H:%M:%S")
maxbound=dt.strptime(minmax,"%H:%M:%S")
print("The time range we are going to explore")
print(minbound,maxbound)
mint=(minbound-mintime) / timedelta (seconds=1)
maxt=(maxbound-mintime) / timedelta (seconds=1)
print(mint,maxt)

print("6. Begin to cut off data !")
#######Cut off time from 6:00:00 to 17:21:00 

cutmergetrip=[]
for index,row in tripmerge.iterrows():
    if (row['arrival_time']>=mint and row['arrival_time']<=maxt):
        #tmp=tripmerge.iloc[index]
        cutmergetrip.append(row)
       # print(row['arrival_time'])

#str ="".join((filename,'mergetrip','.txt'))
    #pd.DataFrame(cutmergetrip).to_csv(str,header=True,sep=' ',
#                  index=False,encoding='utf-8')
##########Reset time and location 


cutmertp=pd.DataFrame(cutmergetrip)
resttrip=[]
for index,row in cutmertp.iterrows():
    tmp=row
    tmp['arrival_time']=row['arrival_time']-mint
    resttrip.append(tmp)
#Reset address 
print(minx,miny)
restp=pd.DataFrame(resttrip)
readtrip=[]
for index,row in restp.iterrows():
    tmp=row
    tmp['x']=round(row['x']-minx,2)
    tmp['y']=round(row['y']-miny,2)
    readtrip.append(tmp)
str ="".join((filename,'mergetrip','.txt'))
pd.DataFrame(readtrip).to_csv(str,header=True,sep=' ',
                              index=False,encoding='utf-8')
movement=[]
mov=pd.DataFrame(readtrip)
for index,row in mov.iterrows():
    tmp=mov.ix[index,['arrival_time','trip_id','x','y']]
    movement.append(tmp)
str ="".join((filename,'movement','.txt'))
pd.DataFrame(movement).to_csv(str,header=True,sep=' ',
                              index=False,encoding='utf-8')
###############Split readtrip into subtrip every 3 hours. 
print("7. Begin to split data ! It is near now : ) ")
cutmert=pd.DataFrame(readtrip)
N=int(sys.argv[2])
k=int((maxt-mint)//(N*3600))
split=[[]for i in range(k+1)]

for index,row in cutmert.iterrows():
    for i in range (k+1):
        if((row['arrival_time'])//(N*3600)==i):
           split[i].append(row)
       
for i in range(k+1):
    
    name ="".join((filename,'split_',N.__str__(),'_',i.__str__(),'.txt'))
    pd.DataFrame(split[i]).to_csv(name,header=True,sep=' ',
                                  index=False,encoding='utf-8')


##Statistics
k=int((maxt-mint)//(N*3600))
print("The number of nodes in each sliced file")
print('stop')
for i in range(k+1):
    print(pd.DataFrame(split[i])['stop_id'].nunique())
print('trip')
for i in range(k+1):
     print(pd.DataFrame(split[i])['trip_id'].nunique())
print('arrival_time')
for i in range(k+1):
    print(pd.DataFrame(split[i])['arrival_time'].nunique())
print('total')
for i in range(k+1):
    print(len(split[i]))
print("8. It it done. The files are in the folder")
print(filename)


























