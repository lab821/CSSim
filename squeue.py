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
    def __init__(self, flow, starttime):
        self.flow = flow    #infomation of this flow
        self.residualsize = flow['size']    #residual size of this flow
        self.priority = 1   #the sending priority
        self.status = 0     #sending status, 0 for uncompleted and 1 for completed
        self.starttime = starttime  #the start time 
        self.duration = -1          #the duration of transmission


    def update(self, bandwidth, interval, temp):
        '''
        update the residual size and status of this flow
        if the residual size < 0 then this flow is completed and return 1
        else return 0
        '''
        sentsize = bandwidth * (interval / 1000)
        self.residualsize = self.residualsize - sentsize
        if self.residualsize <= 0:
            self.status = 1
            self.duration = temp - self.starttime
            return 1
        else:
            return 0

    ###############debug#########################
    def getinfo(self):
        '''
        To report the infomation of this flow
        There are two model of this function:
        if the flow is completed return the inde0
        '''
        res = {}
        if self.status :
            res['src'] = self.flow['src']
            res['dst'] = self.flow['dst']
            res['protocol'] = self.flow['protocol']
            res['sp'] = self.flow['sp']
            res['dp'] = self.flow['dp']
            res['priority'] = self.priority
            return res
        else:
            res['src'] = self.flow['src']
            res['dst'] = self.flow['dst']
            res['protocol'] = self.flow['protocol']
            res['sp'] = self.flow['sp']
            res['dp'] = self.flow['dp']
            res['duration'] = self.duration
            res['size'] = self.flow['size']
            return res           
