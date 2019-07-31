#The simulator main module

import numpy as np
import pandas as pd
import time
from squeue import Flow, Squeue
from scheduler import scheduler

#TODO: adding the trace file 
class simulator(object):
    def __init__(self, trace):
        self.data = pd.read_csv(trace)
        self.p_interval = 100       #process interval
        self.t_interval = 1000      #training interval 
        self.sendingqueues = []     #store the uncompleted flows
        self.completedqueues = []   #store the completed flows
        self.bandwidth = 100000000  #simulation bandwidth, unit:b/s
        self.hpc = 0                #counter of high priority flow 
        self.counter = 1            #counter of the total queue in the simulator

    def prepare(self, temptime):
        '''
        Adding new flow to sending queues according to the input trace data
        when the interval is larger than the rtime of a flow trace
        '''
        for index, row in self.data.iterrows():
            interval = row['rtime']
            if temptime - self.starttime >= interval:
                f = Flow(row)
                s = Squeue(f, temptime)
                self.hpc += 1
                self.sendingqueues.append(s)
                self.data = self.data.drop(index)
            else:
                break

    def updatequeue(self, interval, temp):
        '''
        Update info in each sending queues and change the state of each flow
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
        '''
        for i in res.keys():
            change = res[i] - self.sendingqueues[i].priority
            self.sendingqueues[i].priority = res[i]
            hpc = hpc + change  #change the high priority flow counter

    def debuginfo(self,temp):
        '''
        Only for debug, print info in console
        '''
        line = '%50s\n'%(50*'=')
        timerstr = 'Timer: %d.\n'%(temp - self.starttime)
        aqstr = 'Active queue info:\n'
        for queue in self.sendingqueues:
            aqstr += 'Residual size:%d Mb, Current bandwidth:%d Mb\n'%(queue.residualsize//1000000, queue.bw//             1000000)
        cqstr = 'Completed queue info:\n'
        for queue in self.completedqueues:
            cqstr += 'Total size:%d Mb, Duration:%d ms\n'%(queue.flow.size//1000000, queue.duration)
        debugstr = line+ timerstr + line+ aqstr+ line+ cqstr+ line
        print(debugstr)
        with open('log','a') as f:
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
            self.prepare(temp)
            if(temp - t_last > self.t_interval):
                # actq, cptq = self.Getinfo()
                # res = scheduler(actq, cptq)
                # self.control(res)
                # t_last = temp

                self.debuginfo(temp)
                if self.data.empty and len(self.sendingqueues)==0:
                    print('Timer: %d. Simulation completed.'%(temp - self.starttime))
                    break
                t_last = temp
            else:
                pass
            time.sleep(self.p_interval/1000)

#TODO: adding the trace file 
if __name__ == "__main__":
    trace = 'data.csv'
    sim = simulator(trace)
    sim.run()