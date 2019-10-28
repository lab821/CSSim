#The simulator main module

import numpy as np
import pandas as pd
import time
import os
from squeue import Flow, Squeue, Coflow
from scheduler import DQNscheduler, DDQNCS
import interface
import sys

class simulator(object):
    def __init__(self, trace, mode, granularity, log_level = 5, algorithm = None, compensation = None):
        """
        init the simulator
        Parameters:
        -----------------
            trace : str
                the trace file of flows
            mode : integer
                cyclic mode or once mode, in cyclic mode, the trace will be repeated after finished, 0 for once and 1for cyclic
            granularity : str
                schedule granularity, 'coflow' or 'flow'
            log_level : str
                the level of log printer, 5 for all flows and coflows infomation, 4 for only all the coflow info, 3 for only completed coflow infomation, 2 for only scheduler log, 1 is reserved, 0 for no loginfo 5 as default
            algorithm : str
                the scheduler algorithm, None as default (per-flow fairness)
            compensation : str 
                the bandwidth compensation mechanism , None for default
        """
        self.data = pd.read_csv(trace)  #Flow trace 
        self.data_backup = self.data    #Flow trace backup for cyclic mode
        self.p_interval = 1000000       #process interval
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
        self.log_level = log_level  #log level 
        self.compensation = compensation    #bandwidth compensation mechanism, None for default
        ##TESTING: CLOSE WEBUI
        #self.httpinterface = interface.HTTPinterface()    #http interface for information query
        #self.httpinterface.start()     #start the httpserver process
        if granularity == 'coflow':
            self.coflow_list = {}       #The coflow list 

        if algorithm == 'DQN':
            self.scheduler = DQNscheduler() #The DQN scheduler
        elif algorithm == 'DDQNCS':
            self.scheduler = DDQNCS()
        else:
            self.scheduler = None           #No scheduler

        mode_info = 'Cycle' if self.mode else 'Once'
        #al_info = algorithm
        log_lel_list = ['No log', 'Reserved', 'Scheduler', 'Coflow-completed', 'Coflow', 'All']
        info = 'Start simulation. Mode:%s. Log-level:%s. Algorithm:%s.\n'%(mode_info, 
                log_lel_list[self.log_level], algorithm)
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

        #One cycle completed, record infomation in log
        if self.data.empty and len(self.sendingqueues)==0:
            done = True
            #Cyclic mode
            if self.mode:
                data = self.data_backup.copy()
                total_interval = temp - self.starttime
                data.loc[:,'rtime'] += total_interval
                self.data = data
                if self.cstime == 0:
                    duration = temp - self.starttime
                else:
                    duration = temp - self.cstime 
                self.cstime = temp
                line = '%50s\n'%(50*'-')
                info = line +'Cyclic mode INFO. Last cycle duration: %d\n'%duration + line
                if self.granularity == 'coflow':
                    coflow_duration = 0
                    for coflow in self.coflow_list.values():
                        coflow_duration += coflow.duration
                    cycle_cct = coflow_duration//(1000*len(self.coflow_list))
                    info += 'CCT in last cycle is : %d s\n'%cycle_cct + line
                    #reset coflow_list
                    self.coflow_list = {}
                self.Logprinter(info)
            #Once mode
            else:
                tag = True
                self.Loginfo(temp)
                ##DEBUG:print info
                info = 'Timer: %d ms. Simulation completed.\n'%(temp - self.starttime)
                if self.granularity == 'coflow':
                    coflow_duration = 0
                    for coflow in self.coflow_list.values():
                        coflow_duration += coflow.duration
                    cycle_cct = coflow_duration//(1000*len(self.coflow_list))
                    info += 'CCT in last cycle is : %d s\n'%cycle_cct
                print(info)
                return tag, done

        #prepare new flows
        for index, row in self.data.iterrows():
            interval = row['rtime']
            if temp - self.starttime >= interval:
                f = Flow(row)
                q_index = self.counter
                flow = Squeue(f, temp, q_index)
                self.counter += 1
                self.hpc += 1
                self.sendingqueues.append(flow)
                self.data = self.data.drop(index)
                if self.granularity == 'coflow':
                    self.dispatch_to_coflow(flow, row['tag'], temp)
            else:
                break
        return tag, done

    def dispatch_to_coflow(self, flow, coflow_index, temp):
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
        self.coflow_list[coflow_index].append(flow)
        #if this coflow is a low priority coflow this flow should be the same
        if self.coflow_list[coflow_index].priority == 0:
            #TODO: observe the result
            flow.priority = 0
            self.hpc -= 1

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
        if self.hpc == 0 and self.compensation != None:
            for flow in self.sendingqueues:
                flow.priority = 1
            self.hpc = len(self.sendingqueues)
        flows_info = []
        share_count = self.hpc
        for i in range(len(self.sendingqueues)-1, -1, -1):
            flow = self.sendingqueues[i]
            sentsize = 0
            ret = 0
            #process high priority flow
            if flow.priority:
                bw = int(self.bandwidth / share_count)
                sentsize, ret = flow.update(bw, interval, temp)
                if ret:
                    self.completedqueues.append(flow)
                    self.sendingqueues.pop(i)
                    self.hpc -= 1
                # #update coflow info
                # if self.granularity == 'coflow':
                #     coflow_index = flow.flow.tag
                #     self.coflow_list[coflow_index].update(sentsize, temp)
                #     if ret:
                #         #if this flow is finished, pop it from the coflow's flow_list
                #         self.coflow_list[coflow_index].remove(flow)
            else:
                flow.bw = 0
            #update coflow info
            if self.granularity == 'coflow':
                coflow_index = flow.flow.tag
                self.coflow_list[coflow_index].update(sentsize, temp)
                if ret:
                    #if this flow is finished, pop it from the coflow's flow_list
                    self.coflow_list[coflow_index].remove(flow)
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

    def Getinfo(self, temp):
        '''
        Get the current active queues and completion queues information
        Two tables are returned, representing the active queues and the completed queues, respectively
        ret unit : list
        '''
        actq = []
        cptq = []
        coflowinfo = []
        for queue in self.sendingqueues:
            row = queue.getinfo()
            actq.append(row)
        for queue in self.completedqueues:
            row = queue.getinfo()
            cptq.append(row)
        if self.granularity == 'coflow':
            
            for coflow in self.coflow_list.values():
                if coflow.active:
                    #this need to be improve, HC
                    coflow.duration = temp - coflow.starttime
                    row = coflow.getinfo()         
                    coflowinfo.append(row)
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
            for coflow_index in res.keys():
                coflow = self.coflow_list[coflow_index]
                #change the priority of this coflow
                coflow.priority = res[coflow_index]
                #change each flow's priority belongs to this coflow
                for flow in coflow.flow_list:
                    change = res[coflow_index] - flow.priority
                    flow.priority = res[coflow_index]
                    self.hpc += change 
        else:
            for i in res.keys():
                change = res[i] - self.sendingqueues[i].priority
                self.sendingqueues[i].priority = res[i]
                self.hpc += change  #change the high priority flow counter
    
    def Loginfo(self,temp, info = ''):
        '''
        Print info in console and save in log file
        Input parameter：
            temp: current timestamp
            info: additional infomation
        '''

        line = '%50s\n'%(50*'=')
        timerstr = line + 'Timer: %d ms.\n'%(temp - self.starttime)

        if self.log_level > 4:
        #flow level info
        #active flows info:
            aq_str = line + 'Active queue info:\n'
            for queue in self.sendingqueues:
                aq_str += 'Queue index:%d, Residual size:%d Mb, Current bandwidth:%d Mb\n'%(queue.index,                    queue.residualsize//1000000, queue.bw//1000000)

            #completed flows info
            if len(self.completedqueues) > 0 :
                cq_str = line + 'Completed queue info:\n'
                for queue in self.completedqueues:
                    cq_str += 'Queue index:%d, Total size:%d Mb, Duration: %d ms\n'%(queue.index, queue.flow.size//1000000, queue.duration)
            else:
                cq_str = ''
            flow_str = aq_str + cq_str
        else:
            flow_str = ''

        #coflow level info
        if self.granularity == 'coflow':
            act_coflow_str = line + 'Active coflow info:\n'
            cpt_coflow_str = line + 'Completed coflow info:\n'
            #TODO: YOU WEN TI
            for coflow in self.coflow_list.values():
                if coflow.active:
                    if self.log_level < 4:
                        continue
                    act_coflow_str +='Coflow index: %s, Sent size : %d Mb, Flow count: %d , Coflow-Duration: %d ms\n'%(coflow.index, coflow.size//1000000, coflow.length(), coflow.duration)
                elif coflow.printed == False and self.log_level > 2:
                    cpt_coflow_str +='Coflow index: %s, Total size : %d Mb, Total flow count: %d , Completed-Duration: %d ms\n'%(coflow.index, coflow.size//1000000, coflow.flow_count, coflow.duration)
                    #change printed status
                    self.coflow_list[coflow.index].printed = True
            if self.log_level > 3:
                coflow_str = act_coflow_str + cpt_coflow_str
            elif self.log_level > 2:
                coflow_str = cpt_coflow_str
            else:
                coflow_str = ''
        else:
            coflow_str = ''

        #total infomation
        log_str = timerstr + flow_str + coflow_str + line + info
        ##DEBUG:print log in console
        #print(debugstr)
        if self.log_level > 0:
            self.Logprinter(log_str)
    
    def Logprinter(self,info):
        '''
        print log info into log file
        '''
        log_path = 'log/log'
        with open(log_path,'a') as f:
            f.write(info)   
    
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
                    actq, cptq, coflowinfo = self.Getinfo(temp)
                    if self.granularity == 'coflow':
                        res, info = self.scheduler.train(actq, cptq, coflowinfo, done)
                    else:
                        res, info = self.scheduler.train(actq, cptq, done)  
                    self.control(res)
                #print loginfo in this cycle
                if self.log_level > 1:
                    self.Loginfo(temp,info)
                else:
                    self.Logprinter(info)
                #reset completed flow_list
                self.completedqueues = []
                t_last = temp
            else:
                pass
            if timer:
                time.sleep(self.p_interval/1000)
            if timer_upper_limit and temp > timer_upper_limit:
                break


if __name__ == "__main__":

    trace = 'data/data-example.csv'
    logpath = 'log/log'
    if os.path.exists(logpath):
        os.remove(logpath)
    sim = simulator(trace, 1, 'coflow', 1, 'DDQNCS')
    #sim = simulator(trace, 0, 'coflow', 3)
    sim.run(timer = False)

        