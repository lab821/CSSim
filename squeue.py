#The sending queue class and Flow class  

import numpy as np
import pandas as pd

class Flow(object):
    def __init__(self, row):
        self.rtime = row['rtime']   #the relative time of first flow 
        self.src = row['src']       #src node
        self.dst = row['dst']       #dst node
        self.sp = row['sp']         #src port 
        self.dp = row['dp']         #dst port
        self.size = row['size']     #flow size    
        self.tag = row['tag']       #flow tag (for identification)
        self.protocol = 6           #protocol

    def flowinfo(self):
        '''
        print the infomation of this flow
        '''
        print ('rtime:%d, src:%d, dst:%d, source port:%d, destination port:%d, size:%d, tag: %d')%(self.rtime,      self.src, self.dst, self.sp, self.dp, self.size, self.tag)

class Squeue(object):
    def __init__(self, flow, starttime, index):
        self.flow = flow            #infomation of this flow
        self.residualsize = flow.size    #residual size of this flow
        self.priority = 1           #the sending priority
        self.status = 0             #sending status, 0 for uncompleted and 1 for completed
        self.starttime = starttime  #the start time 
        self.duration = -1          #the duration of transmission
        self.bw = 0                 #the current bandwidth
        self.index = index          #queue index


    def update(self, bandwidth, interval, temp):
        '''
        update the residual size and status of this flow
        if the residual size < 0 then this flow is completed and return True
        else return False, the sentsize in this interval is also returned as sentsize 
        '''
        sentsize = bandwidth * (interval / 1000)
        self.residualsize = self.residualsize - sentsize
        self.bw = bandwidth
        if self.residualsize <= 0:
            self.status = 1
            self.duration = temp - self.starttime
            return sentsize, True
        else:
            return sentsize, False

    ###############debug#########################
    def getinfo(self):
        '''
        To report the infomation of this flow
        There are two model of this function:
        if the flow is completed return the inde0
        '''
        res = {}
        #active flow
        if self.status == 0:
            res['src'] = self.flow.src
            res['dst'] = self.flow.dst
            res['protocol'] = self.flow.protocol
            res['sp'] = self.flow.sp
            res['dp'] = self.flow.dp
            res['priority'] = self.priority
            res['sentsize'] = self.flow.size - self.residualsize
            res['qindex'] = self.index
            return res
        #completed flow
        else:
            res['src'] = self.flow.src
            res['dst'] = self.flow.dst
            res['protocol'] = self.flow.protocol
            res['sp'] = self.flow.sp
            res['dp'] = self.flow.dp
            res['duration'] = self.duration
            res['size'] = self.flow.size
            return res           

class Coflow(object):
    def __init__(self, starttime, index):
        self.starttime = starttime  #the start time
        self.duration = -1          #the duration of this coflow
        self.index = index          #coflow index
        self.size = 0               #The sentsize of this flow group(coflow)
        self.flow_count = 0         #record the total count of flows belong to this coflow
        self.flow_indices = []      #The list of flow's index in this coflow
        self.active = False         #the flag to show if there are active flows in this coflow
        self.printed = False        #the flag to assign if this coflow infomation has been printed
        self.priority = 1           #the priority of this coflow

    #append a new flow in this coflow
    def append(self,flow_index):
        if self.active == False:
            self.active = True
            self.printed = False
        self.flow_count += 1
        self.flow_indices.append(flow_index)

    #remove a flow from this coflow
    def remove(self,flow_index):
        self.flow_indices.remove(flow_index)        
        if self.length() == 0:
            self.active = False

    #Get the number of flows in this coflow
    def length(self):
        return len(self.flow_indices)
    
    #update the coflow size and duraiton
    def update(self, sentsize, temp):
        self.size += sentsize
        self.duration = temp - self.starttime

    #Get the infomation of this coflow
    def getinfo(self):
        res = {}
        res['index'] = self.index
        res['starttime'] = self.starttime
        res['duration'] = self.duration
        res['count'] = self.length()
        res['sentsize'] = self.size
        return res