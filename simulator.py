#The simulator main module

import numpy as np
import pandas as pd
import time
import os
import interface
from squeue import Flow, Flowstate, MPQueue
from scheduler import DQNscheduler
from sys import maxsize

class simulator(object):
    def __init__(self, trace, mode, bandwidth = 1000000000, mq_config = None, algorithm = None):
        self.data = pd.read_csv(trace)  #Flow trace 
        self.data_backup = self.data.copy()    #Flow trace backup for cyclic mode
        self.p_interval = 100       #process interval, unit : ms
        self.t_interval = 1000      #training interval, unit : ms 
        self.mpqueues = []          #The multilevel priority queues
        self.cpt_list =[]           #completed flows in the latest cycle 
        self.bandwidth = bandwidth  #simulation bandwidth, unit:b/s
        self.counter = 1            #counter of the total flows in the simulator
        self.active_counter = 0     #counter of active flows
        self.mode = mode            #simulator mode(once or cyclic)
        self.cstime = 0             #start time of last cycle(for cyclic mode)
        self.latestcpti = 0         #record the latest completed flow index in a training period
        ##DEBUG: close httpinterface 
        #self.httpinterface = interface.HTTPinterface()    #http interface for information query
        #self.httpinterface.start()     #start the httpserver process
        
        self.threshold_list = []    #The threshold of each queue

        if algorithm == 'DQN':
            self.scheduler = DQNscheduler() #The DQN scheduler
        else:
            self.scheduler = None           #No scheduler

        mode_info = 'Cycle' if self.mode else 'Once'
        #al_info = algorithm
        info = 'Start simulation. Mode:%s. Algorithm:%s\n'%(mode_info, algorithm)
        self.Logprinter(info)

    def queue_initialization(self, bw_allocation=[1,0], threshold_list=None, mode='strict'):
        '''
        Initialize switch queue settings
        Input parameter:
            bw_allocation: The list of all the queues' bandwidth 
            threshold_list: The list of all the queues' threshold
        '''
        if threshold_list == None:
            threshold_list = [maxsize] * len(bw_allocation)
        
        #check configuration
        #length not match
        if len(bw_allocation) != len(threshold_list):
            ##DEBUG: print for debug##
            print('Configurations length not match')
            self.Logprinter('Configurations length not match')
            return False
        elif np.sum(bw_allocation) != 1:
            ##DEBUG: print for debug##
            print('Bandwidth allocation overflow')
            self.Logprinter('Bandwidth allocation overflow')
            return False

        ##TODO: make the configuration effective
        self.threshold_list = threshold_list
        for i in range(len(threshold_list)):
            bw = bw_allocation[i] * self.bandwidth
            threshold = threshold_list[i]
            queue = MPQueue(bw, threshold)
            self.mpqueues.append(queue)

        return True

            
    def prepare(self, temp):
        '''
            Adding new flow to sending queues according to the input trace data
            when the interval is larger than the rtime of a flow trace
            Determine whether trace simulation is complete，and perform different operations in different modes
            In once mode:
                Perform Loginfo and then return false to shutdown the simulator run function 
            In cyclic mode:
                Generating flows using backup trace data
            Input parameter：
                temp: Current timestamp
        '''
        if self.data.empty and self.active_counter==0:
            #Cyclic mode
            if self.mode:
                data = self.data_backup.copy()
                total_interval = temp - self.starttime
                data.loc[:,'rtime'] += total_interval
                self.data = data
                if self.cstime == 0:
                    duration = temp -self.starttime
                else:
                    duration = temp - self.cstime 
                self.cstime = temp
                line = '%50s\n'%(50*'-')
                info = line +'Cyclic mode INFO. Last cycle duration: %d\n'%duration + line
                self.Logprinter(info)
            #Once mode
            else:
                self.Loginfo(temp)
                ##DEBUG:print info
                print('Timer: %d ms. Simulation completed.'%(temp - self.starttime))
                return True
        for index, row in self.data.iterrows():
            interval = row['rtime']
            if temp - self.starttime >= interval:
                f = Flow(row)
                q_index = self.counter
                s = Flowstate(f, temp, q_index)
                self.counter += 1
                self.addflow(s)
                self.data = self.data.drop(index)
            else:
                break
        return False

    def addflow(self, flow):
        # add a new flow
        self.active_counter += 1
        #distribute the flow to the appropriate queue
        self.distribute(flow)
    
    def distribute(self, flow):
        #distribute the flow to the appropriate queue according to the sentsize and threshold
        
        sentsize = flow.sentsize
        for i in range(len(self.threshold_list)):
            if self.threshold_list[i] > sentsize:
                self.mpqueues[i].push(flow)
                return 
        self.mpqueues[-1].push(flow)

    def updatequeue(self, interval, temp):
        '''
        Update info in each sending queues and change the state of each flow
        Input parameter：
            interval: Time interval between two calls
            temp: Current timestamp
        '''        
        ##for WEB UI TODO:
        # interface_info = {}
        # interface_info['timer'] = temp - self.starttime
        
        #flow_info list
        #flows_info = []
        
        pop_list =[]    #overflow threshold flows in this cycle
        for queue in self.mpqueues:
            cpt, pop = queue.update(interval, temp)
            self.cpt_list.extend(cpt)
            self.active_counter -= len(cpt)     #update activate flows count
            pop_list.extend(pop)
        
        #distribute the overflow flows, can be optimazed
        for flow in pop_list:
            self.distribute(flow) 
        
        ##TODO:
        # interface_info['Active flows'] = flows_info
        # interface.data = interface_info

    #TODO: need update 
    def Getinfo(self):
        '''
        Get the current active queues and completion queues information
        Two tables are returned, representing the active queues and the completed queues, respectively
        ret unit : dataframe  
        '''
        actq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'priority', 'sentsize', 'qindex'])
        cptq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'duration', 'size'])
        
        #activate flow
        for queue in self.mpqueues:
            for flow in queue.flow_list:
                row = flow.getinfo()
                actq = actq.append(row, ignore_index=True)

        #completed flow
        for flow in self.cpt_list:
            row = flow.getinfo()
            cptq = cptq.append(row, ignore_index=True)
            
        return actq, cptq
    
    #TODO: update 
    def control(self, res):
        '''
        Rate control by modifying the priority of sending queue
        Implementation based on the results returned by the scheduler
        Input parameter：
            res: flow control information returned by scheduling module
        '''
        #update threshold_list of each queue
        self.threshold_list = res

    #TODO: update
    def Loginfo(self,temp, info = ''):
        '''
        Print info in console and save in log file
        Input parameter：
            temp: Current timestamp
        '''
        line = '%50s\n'%(50*'=')
        timerstr = 'Timer: %d ms.\n'%(temp - self.starttime)
        aqstr = 'Active flow info:\n'
        for queue in self.mpqueues:
            for flow in queue.flow_list:
                aqstr += 'Flow index:%d, Residual size:%d Mb, Current bandwidth:%d Mb\n'%(flow.index,                     flow.residualsize//1000000, flow.rate//1000000)
        if len(self.cpt_list) > 0 :
            cqstr = 'Completed flow info:\n'
            for flow in self.cpt_list:
                cqstr += 'Flow index:%d, Total size:%d Mb, Duration:%d ms\n'%(flow.index, flow.flow.size//              1000000, flow.duration)
            cqstr += line
        else:
            cqstr = ''
        ##NOTE: We delete the completed queues that have been print in log
        self.cpt_list = []
        debugstr = line+ timerstr + line+ aqstr+ line+ cqstr + info
        ##DEBUG:print log in console
        #print(debugstr)
        self.Logprinter(debugstr)
    
    def Logprinter(self,info):
        '''
        print log info into log file
        '''
        log_path = 'log/log'    ##HC##
        with open(log_path,'a') as f:
            f.write(info)   
    
    ###############TODO: make the control into non-blocking control#########################
    def run(self):
        '''
        The simulation main function
        every p_interval executes prepare data function and update info in each sending queues
        every t_interval executes training function
        '''
        nowTime = lambda t:int(round(t * 1000))
        self.starttime = nowTime(time.time())
        p_last = self.starttime
        t_last = self.starttime
        while True:
            temp = nowTime(time.time())
            interval = temp - p_last
            p_last = temp
            self.updatequeue(interval, temp)
            ret = self.prepare(temp)
            if ret:
                break
            if(temp - t_last > self.t_interval):
                ##schedule
                info = ''
                if self.scheduler != None:
                    actq, cptq = self.Getinfo()
                    res, info = self.scheduler.train(actq, cptq)
                    self.control(res)
                self.Loginfo(temp,info)
                t_last = temp
            else:
                pass
            time.sleep(self.p_interval/1000)


if __name__ == "__main__":
    # data path
    trace = 'data1.csv'
    # log path
    logpath = 'log/log'
    # switch bandwidth
    bandwidth = 100000000
    # Multilevel queue configurations
    bw_allocation = [0.5,0.2,0.15,0.1,0.05]
    threshold_list = [10000,20000,30000,40000,maxsize]
    mode = 'weight'
    # clear old log 
    if os.path.exists(logpath):
        os.remove(logpath)
    #simulator run 
    sim = simulator(trace, 0, bandwidth)
    #sim.queue_initialization(bw_allocation, threshold_list, mode)
    sim.queue_initialization()
    sim.run()

##TODO:
'''
    日志文件频繁打开的问题
    异常处理
    多级队列
    两种模式：真是时钟和虚拟时钟

    prepare 要改，放到多级队列中去
    update 要改，速度由当前所在队列决定
    严格多级队列，一个队列占据带宽，其余队列0，高队列为空时低队列发送
    加权多级队列，

'''    
##test