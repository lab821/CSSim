import numpy as np
import pandas as pd
import time

class simulator(object):
    def __init__(self):
        #self.data = pd.read_csv(trace)
        self.p_interval = 100   #process interval
        self.t_interval = 1000  #training interval 
        self.sendingqueues = []     #store the uncompleted flows
        self.completedqueues = []   #store the completed flows

    ###############debug#########################
    def prepare(self, temptime):
        '''
        Adding new flow to sending queues according to the input trace data
        when the interval is larger than the rtime of a flow trace
        '''
        for index, row in self.data.iterrows():
            interval = row['rtime']
            if temptime - self.starttime > interval:
                pass
    ###############debug#########################
    def updatequeue(self,interval):
        '''
        Update info in each sending queues and change the state of each flow
        '''
        for queue in self.s_queues:
            pass
    ###############debug#########################
    def train():
        pass

    ###############debug#########################
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
            updatequeue(interval)
            prepare(temp)
            if(temp - t_last > t_interval):
                train()
                t_last = temp
            else:
                time.sleep(self.p_interval/1000)

###############debug#########################
if __name__ == "__main__":
    sim = simulator()
    sim.run()