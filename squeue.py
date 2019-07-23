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

    def flowinfo(self):
        '''
        print the infomation of this flow
        '''
        print ('rtime:%d, src:%d, dst:%d, source port:%d, destination port:%d, size:%d, tag: %d')%(self.rtime,      self.src, self.dst, self.sp, self.dp, self.size, self.tag)


class Squeue(object):
    def __init__(self, flow):
        self.flow = flow    #infomation of this flow
        self.residualsize = flow['size']    #residual size of this flow
        self.priority = 1   #the sending priority
        self.status = 0     #sending status, 0 for uncompleted and 1 for completed

    def update(self, bandwidth, interval):
        '''
        update the residual size and status of this flow
        if the residual size < 0 then this flow is completed and return 1
        else return 0
        '''
        sentsize = bandwidth * (interval / 1000)
        self.residualsize = self.residualsize - sentsize
        if self.residualsize <= 0:
            self.status = 1
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
        pass
