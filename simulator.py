#The simulator main module

import numpy as np
import pandas as pd
import time
from squeue import Flow, Squeue
from scheduler import scheduler

#TODO: adding the trace file 
class simulator(object):
    def __init__(self):
        #self.data = pd.read_csv(trace)
        self.p_interval = 100   #process interval
        self.t_interval = 1000  #training interval 
        self.sendingqueues = []     #store the uncompleted flows
        self.completedqueues = []   #store the completed flows
        self.bandwidth = 1000000000 #simulation bandwidth, unit:b/s
        self.hpc = 0                #counter of high priority flow 

    def prepare(self, temptime):
        '''
        Adding new flow to sending queues according to the input trace data
        when the interval is larger than the rtime of a flow trace
        '''
        for index, row in self.data.iterrows():
            interval = row['rtime']
            if temptime - self.starttime > interval:
                f = Flow(row)
                s = Squeue(f, temptime)
                self.hpc += 1
                self.sendingqueues.append(s)
                self.data.drop(index)
            else:
                break

    def updatequeue(self, interval, temp):
        '''
        Update info in each sending queues and change the state of each flow
        '''        
        for i in range(len(sendingqueues)-1, -1, -1):
            queue = sendingqueues[i]
            if queue.priority:
                bw = int(self.bandwidth / self.hpc)
                ret = queue.update(bw, interval, temp)
                if ret:
                    self.completedqueues.append(queue)
                    self.sendingqueues.pop(i)

    def Getinfo(self):
        '''
        Get the current active queues and completion queues information
        Two tables are returned, representing the active queues and the completed queues, respectively
        ret unit : dataframe  
        '''
        actq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'priority'])
        cptq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'duration', 'size'])
        for queue in sendingqueues:
            row = queue.Getinfo()
            actq = actq.append(row, ignore_index=True)
        for queue in completedqueues:
            row = queue.Getinfo()
            cptq = cptq.append(row, ignore_index=True)
        return actq, cptq
    
    def control(self, res):
        '''
        Rate control by modifying the priority of sending queue
        Implementation based on the results returned by the scheduler
        '''
        for i in res.keys():
            change = res[i] - sendingqueues[i].priority
            sendingqueues[i].priority = res[i]
            hpc = hpc + change  #change the high priority flow counter


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
            if(temp - t_last > t_interval):
                actq, cptq = self.Getinfo()
                res = scheduler(actq, cptq)
                self.control(res)
                t_last = temp
            else:
                time.sleep(self.p_interval/1000)

#TODO: adding the trace file 
if __name__ == "__main__":
    sim = simulator()
    sim.run()