#Storage data structures of flow, coflow and forwarding queue

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

# The flow state manager
class Flowstate(object):
    def __init__(self, flow, starttime, index):
        self.flow = flow                    #infomation of this flow
        self.residualsize = flow.size       #residual size of this flow
        self.sentsize = 0                   #sent size of this flow
        self.priority = 1                   #the sending priority
        self.status = 0                     #sending status, 0 for uncompleted and 1 for completed
        self.starttime = starttime          #the start time 
        self.duration = -1                  #the duration of transmission
        self.rate = 0                       #the current bandwidth
        self.index = index                  #queue index
        


    def update(self, rate, interval, temp):
        '''
        update the residual size and status of this flow
        if the residual size < 0 then this flow is completed and return 1
        else return 0
        '''
        sentsize = rate * (interval / 1000)
        self.residualsize = self.residualsize - sentsize
        self.sentsize = self.sentsize + sentsize
        self.rate = rate
        if self.residualsize <= 0:
            self.status = 1
            self.duration = temp - self.starttime
            return 1
        else:
            return 0

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

class MPQueue(object):
    def __init__(self, bw, threshold):
        self.len = 0            #length of this queue
        self.flow_list = []     #The list of flowstates in this queue
        self.threshold = threshold     #The threshold of this queue (upper bound)
        self.bw = bw             #bandwidth of this queue

    def length(self):
        #Get function of the length of this queue
        return self.len
    
    def push(self, flow):
        #push a new flow
        self.flow_list.append(flow)
        self.len += 1

    def flowlist(self):
        #Get the list of flowstates in this queue
        return self.flow_list

    def update(self, interval, temp, policy = 'FF'):
        '''
        update each flowstate in this queue
        Output:
            cpt_list : The completed flows in this queue in this cycle
            pop_list : The flows overflow the threshold in this cycle
        '''
        cpt_list = []
        pop_list = []
        if policy == 'FF':
            rate = int(self.bw / self.len) if self.len != 0 else self.bw
            for index in range(self.len-1, -1, -1):
                flow = self.flow_list[index]
                ret = flow.update(rate, interval, temp)
                if ret == 1:
                    # cpt flow
                    self.flow_list.pop(index)
                    cpt_list.append(flow)
                    self.len -= 1
                else:
                    #not completed
                    #check if this flow over threshold
                    if flow.sentsize > self.threshold:
                        #if over 
                        self.flow_list.pop(index)
                        pop_list.push(flow)
                        self.len -= 1
                    else:
                        pass
        return cpt_list, pop_list


            

