#The simulator main module

import numpy as np
import pandas as pd
import time
import os
from squeue import Flow, Squeue, Coflow
from scheduler import DQNscheduler
import interface

class simulator(object):
    def __init__(self, trace, mode, granularity, algorithm = None):
        self.data = pd.read_csv(trace)  #Flow trace 
        self.data_backup = self.data    #Flow trace backup for cyclic mode
        self.p_interval = 1000       #process interval
        self.t_interval = 1000      #training interval 
        self.sendingqueues = []     #store the uncompleted flows
        self.completedqueues = []   #store the completed flows
        self.bandwidth = 100000000  #simulation bandwidth, unit:b/s
        self.hpc = 0                #counter of high priority flow 
        self.counter = 1            #counter of the total queue in the simulator
        self.mode = mode            #simulator mode(0 for once or 1 for cyclic)
        self.granularity = granularity  #The granularity of scheduler
        self.cstime = 0             #start time of last cycle(for cyclic mode)
        self.latestcpti = 0         #record the latest completed flow index in a training period
        
        ##TESTING: CLOSE WEBUI
        #self.httpinterface = interface.HTTPinterface()    #http interface for information query
        #self.httpinterface.start()     #start the httpserver process
        if granularity == 'coflow':
            self.coflow_list = {}       #The coflow list 
            self.cpt_coflow_list= []    #completed coflow list
            self.coflow_duration_list = []  #For calculate CCT

        if algorithm == 'DQN':
            self.scheduler = DQNscheduler() #The DQN scheduler
            info = 'Start simulation. mode:'
        else:
            self.scheduler = None           #No scheduler

        mode_info = 'Cycle' if self.mode else 'Once'
        #al_info = algorithm
        info = 'Start simulation. Mode:%s. Algorithm:%s\n'%(mode_info, algorithm)
        self.Logprinter(info)

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
        #tag: if true, the simulator will stop else run the data again
        #done: if true, tell the scheduler this trace has been completed   
        tag, done = False, False
        if self.data.empty and len(self.sendingqueues)==0:
            done = True
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
                if self.granularity == 'coflow':
                    cycle_cct = np.mean(self.coflow_duration_list)//1000
                    info += 'CCT in last cycle is : %d s\n'%cycle_cct + line
                self.Logprinter(info)
            #Once mode
            else:
                tag = True
                self.Loginfo(temp)
                ##DEBUG:print info
                info = 'Timer: %d ms. Simulation completed.\n'%(temp - self.starttime)
                if self.granularity == 'coflow':
                    cycle_cct = np.mean(self.coflow_duration_list)//1000
                    info += 'CCT in last cycle is : %d s\n'%cycle_cct
                print(info)
                return tag, done
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
                if self.granularity == 'coflow':
                    self.dispatch_to_coflow(q_index, row['tag'], temp)
            else:
                break
        return tag, done

    def dispatch_to_coflow(self, flow_index, coflow_index, temp):
        '''
        dispatch a flow into corresping coflow
        Input parameter:
            flow_index: the index of the flow to be dispatched
            coflow_index: the coflow tag of this flow
            temp: current timestamp
        '''
        if self.granularity != 'coflow':
            return
        #if a new coflow, create a new coflow and store in the list
        if coflow_index not in self.coflow_list.keys():
            coflow = Coflow(temp, coflow_index)
            self.coflow_list[coflow_index] = coflow
        #append this flow in correspending coflow
        self.coflow_list[coflow_index].append(flow_index)
    

    def updatequeue(self, interval, temp):
        '''
        Update info in each sending queues and change the state of each flow
        Input parameter：
            interval: Time interval between two calls
            temp: Current timestamp
        '''        
        ##TESTING: CLOSE WEBUI
        # interface_info = {}
        # interface_info['timer'] = temp - self.starttime
        flows_info = []
        share_count = self.hpc
        for i in range(len(self.sendingqueues)-1, -1, -1):
            queue = self.sendingqueues[i]
            #process high priority flow
            if queue.priority:
                bw = int(self.bandwidth / share_count)
                sentsize, ret = queue.update(bw, interval, temp)
                if ret:
                    self.completedqueues.append(queue)
                    self.sendingqueues.pop(i)
                    self.hpc -= 1
                #update coflow info
                if self.granularity == 'coflow':
                    coflow_index = queue.flow.tag
                    self.coflow_list[coflow_index].update(sentsize, temp)
                    if ret:
                        #if this flow is finished, pop it from the coflow's flow_indices
                        #if this flow is the last one of this coflow, pop this coflow from coflow_list 
                        #and add it to the cpt_coflow_list
                        self.coflow_list[coflow_index].remove(queue.index)
                        if self.coflow_list[coflow_index].length() == 0:
                            coflow = self.coflow_list.pop(coflow_index)
                            self.cpt_coflow_list.append(coflow)
            else:
                queue.bw = 0
            ##TESTING: CLOSE WEBUI
            # row = {}
            # row['queue_index'] = queue.index
            # row['src'] = queue.flow.src
            # row['dst'] = queue.flow.dst
            # row['protocol'] = queue.flow.protocol
            # row['sp'] = queue.flow.sp
            # row['dp'] = queue.flow.dp
            # row['bw'] = queue.bw
            # flows_info.append(row)
        ##TESTING: CLOSE WEBUI
        # interface_info['Active flows'] = flows_info
        # interface.data = interface_info

    def Getinfo(self):
        '''
        Get the current active queues and completion queues information
        Two tables are returned, representing the active queues and the completed queues, respectively
        ret unit : dataframe  
        '''
        actq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'priority', 'sentsize', 'qindex'])
        cptq = pd.DataFrame(columns=['src', 'dst','protocol', 'sp', 'dp', 'duration', 'size'])
        coflowinfo = pd.DataFrame(columns=['index', 'starttime', 'duration', 'count', 'sentsize'])
        for queue in self.sendingqueues:
            row = queue.getinfo()
            actq = actq.append(row, ignore_index=True)
        ##NOTE:This change is followed by the change of removal of printed queues
        # for i in range(len(self.completedqueues)-1, -1, -1):
        #     queue = self.completedqueues[i]
        #     if queue.index == self.latestcpti:
        #         break
        #     row = queue.getinfo()
        #     cptq = cptq.append(row, ignore_index=True)
        # if len(self.completedqueues) > 0:
        #     self.latestcpti = self.completedqueues[-1].index
        ##NOTE:Ibid
        for queue in self.completedqueues:
            row = queue.getinfo()
            cptq = cptq.append(row, ignore_index=True)
        if self.granularity == 'coflow':
            for coflow in self.coflow_list.values():
                row = coflow.getinfo()
                coflowinfo.append(row, ignore_index=True)
        return actq, cptq, coflowinfo
    

    def control(self, res):
        '''
        Rate control by modifying the priority of sending queue
        Implementation based on the results returned by the scheduler
        Input parameter：
            res: flow control information returned by scheduling module
        '''
        if self.granularity == 'coflow':
            #in this case res should be a list of all the coflows' priority
            #for the high priority coflow make all of the flows in them in high priority to schedule
            #in contrary the flows in low priority coflows should  waite
            index_list = self.get_flow_index()
            for coflow_index in res.keys():
                flow_indices = self.coflow_list[coflow_index].flow_indices
                for index in flow_indices:
                    i = index_list.index(index)
                    change = res[coflow_index] - self.sendingqueues[i].prioirty
                    self.sendingqueues[i].priority = res[coflow_index]
                    self.hpc += change 
        else:
            for i in res.keys():
                change = res[i] - self.sendingqueues[i].priority
                self.sendingqueues[i].priority = res[i]
                self.hpc = self.hpc + change  #change the high priority flow counter
    
    def get_flow_index(self):
        '''
        Get the index of each flow in sendingqueues
        return parameter:
            index_list : The index list of flow in sendingqueues. 
        '''
        index_list = []
        for flow in self.sendingqueues:
            index = flow.index
            index_list.append(index)
        return index_list 
    
    def Loginfo(self,temp, info = ''):
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
        if len(self.completedqueues) > 0 :
            cqstr = 'Completed queue info:\n'
            for queue in self.completedqueues:
                cqstr += 'Queue index:%d, Total size:%d Mb, Duration: %d ms\n'%(queue.index, queue.flow.size//              1000000, queue.duration)
            cqstr += line
            ##NOTE: We delete the completed queues that have been print in log
            self.completedqueues = []
        else:
            cqstr = ''
        if self.granularity == 'coflow':
            coflowstr = 'Active coflow info:\n'
            for coflow in self.coflow_list.values():
                coflowstr +='Coflow index: %s, Sent size : %d Mb, Flow count: %d , Coflow-Duration: %d ms\n'%(coflow.index, coflow.size//1000, coflow.length(), coflow.duration)
            coflowstr += line
            if len(self.cpt_coflow_list) > 0 :
                coflowstr += 'Completed coflow info:\n'
                for coflow in self.cpt_coflow_list:
                    coflowstr +='Coflow index: %s, Total size : %d Mb, Completed-Duration: %d ms\n'%(coflow.index, coflow.size//1000, coflow.duration)
                    self.coflow_duration_list.append(coflow.duration)
                coflowstr += line
                ##NOTE: We delete the completed coflows that have been print in log
                self.cpt_coflow_list = []
        else:
            coflowstr = ''

        debugstr = line+ timerstr + line+ aqstr+ line+ cqstr + coflowstr +info
        ##DEBUG:print log in console
        #print(debugstr)
        self.Logprinter(debugstr)
    
    def Logprinter(self,info):
        '''
        print log info into log file
        '''
        log_path = 'log/log'
        with open(log_path,'a') as f:
            f.write(info)   
    
    ###############TODO: make the control into non-blocking control#########################
    def run(self, timer = True, timer_upper_limit = None):
        '''
        The simulation main function
        every p_interval executes prepare data function and update info in each sending queues
        every t_interval executes training function
        input:
            timer: the timer mode, virtual or real
            timer_upper_limit: the upper limit of the timer, None for no limit
        '''
        nowTime = lambda t:int(round(t * 1000))
        if timer:
            #real timer mode, the same below
            self.starttime = nowTime(time.time())
        else:
            #virtual timer mode, the same below
            self.starttime = 0
        p_last = self.starttime
        t_last = self.starttime

        while True:
            if timer:
                temp = nowTime(time.time())
            else:
                temp = self.p_interval/1000 + p_last
            interval = temp - p_last
            p_last = temp
            self.updatequeue(interval, temp)
            tag, done = self.prepare(temp)
            if tag:
                break
            if(temp - t_last >= self.t_interval):
                ##schedule
                info = ''
                if self.scheduler != None:
                    actq, cptq, coflowinfo = self.Getinfo()
                    if self.granularity == 'coflow':
                        res, info = self.scheduler.train(actq, cptq, coflowinfo, done)
                    else:
                        res, info = self.scheduler.train(actq, cptq, done)     
                    self.control(res)
                self.Loginfo(temp,info)
                t_last = temp
            else:
                pass
            if timer:
                time.sleep(self.p_interval/1000)
            if timer_upper_limit and timer > timer_upper_limit:
                return

if __name__ == "__main__":
    trace = 'data1.csv'
    logpath = 'log/log'
    if os.path.exists(logpath):
        os.remove(logpath)
    sim = simulator(trace, 0, 'coflow')
    sim.run(timer = False)