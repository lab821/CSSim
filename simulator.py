#The simulator main module

import numpy as np
import pandas as pd
import time
import os
from squeue import Flow, Squeue
from scheduler import scheduler

#TODO: adding the trace file 
class simulator(object):
    def __init__(self, trace, mode):
        self.data = pd.read_csv(trace)  #Flow trace 
        self.data_backup = self.data    #Flow trace backup for cyclic mode
        self.p_interval = 100       #process interval
        self.t_interval = 1000      #training interval 
        self.sendingqueues = []     #store the uncompleted flows
        self.completedqueues = []   #store the completed flows
        self.bandwidth = 100000000  #simulation bandwidth, unit:b/s
        self.hpc = 0                #counter of high priority flow 
        self.counter = 1            #counter of the total queue in the simulator
        self.mode = mode          #simulator mode(once or cyclic)

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
        if self.data.empty and len(self.sendingqueues)==0:
            #Cyclic mode
            if self.mode:
                data = self.data_backup.copy()
                total_interval = temp - self.starttime
                data.loc[:,'rtime'] += total_interval
                self.data = data
            #Once mode
            else:
                self.Loginfo(temp)
                print('Timer: %d ms. Simulation completed.'%(temp - self.starttime))
                return True
        for index, row in self.data.iterrows():
            interval = row['rtime']
            if temp - self.starttime >= interval:
                f = Flow(row)
                q_index = self.counter
                s = Squeue(f, temp, q_index)
                self.counter += 1
                self.hpc += 1
                self.sendingqueues.append(s)
                self.data = self.data.drop(index)
            else:
                break
        return False

    def updatequeue(self, interval, temp):
        '''
        Update info in each sending queues and change the state of each flow
        Input parameter：
            interval: Time interval between two calls
            temp: Current timestamp
        '''        
        for i in range(len(self.sendingqueues)-1, -1, -1):
            queue = self.sendingqueues[i]
            if queue.priority:
                bw = int(self.bandwidth / self.hpc)
                ret = queue.update(bw, interval, temp)
                if ret:
                    self.completedqueues.append(queue)
                    self.sendingqueues.pop(i)
                    self.hpc -= 1

    def Getinfo(self):
        '''
        Get the current active queues and completion queues information
        Two tables are returned, representing the active queues and the completed queues, respectively
        ret unit : dataframe  
        '''
        actq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'priority'])
        cptq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'duration', 'size'])
        for queue in self.sendingqueues:
            row = queue.Getinfo()
            actq = actq.append(row, ignore_index=True)
        for queue in self.completedqueues:
            row = queue.Getinfo()
            cptq = cptq.append(row, ignore_index=True)
        return actq, cptq
    
    def control(self, res):
        '''
        Rate control by modifying the priority of sending queue
        Implementation based on the results returned by the scheduler
        Input parameter：
            res: flow control information returned by scheduling module
        '''
        for i in res.keys():
            change = res[i] - self.sendingqueues[i].priority
            self.sendingqueues[i].priority = res[i]
            hpc = hpc + change  #change the high priority flow counter

    def Loginfo(self,temp):
        '''
        Print info in console and save in log file
        Input parameter：
            temp: Current timestamp
        '''
        line = '%50s\n'%(50*'=')
        timerstr = 'Timer: %d ms.\n'%(temp - self.starttime)
        aqstr = 'Active queue info:\n'
        for queue in self.sendingqueues:
            aqstr += 'Queue index:%d, Residual size:%d Mb, Current bandwidth:%d Mb\n'%(queue.index,                        queue.residualsize//1000000, queue.bw//1000000)
        cqstr = 'Completed queue info:\n'
        for queue in self.completedqueues:
            cqstr += 'Queue index:%d, Total size:%d Mb, Duration:%d ms\n'%(queue.index, queue.flow.size//1000000,          queue.duration)
        debugstr = line+ timerstr + line+ aqstr+ line+ cqstr+ line
        print(debugstr)
        with open('log/log','a') as f:
            f.write(debugstr)
    
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
                # actq, cptq = self.Getinfo()
                # res = scheduler(actq, cptq)
                # self.control(res)
                # t_last = temp
                self.Loginfo(temp)
                t_last = temp
            else:
                pass
            time.sleep(self.p_interval/1000)


if __name__ == "__main__":
    trace = 'data.csv'
    logpath = 'log/log'
    if os.path.exists(logpath):
        os.remove(logpath)
    sim = simulator(trace, 1)
    sim.run()