#The scheduler module
#TODO: Implementing a General Scheduling Algorithms Interface

from algorithms.dqn import DQN
import pandas as pd
import numpy as np

NUM_A = 3   #number of active flows
NUM_F = 3   #number of finished flows

def scheduler(actq, cptq):
    res = {}
    return res

class DQNscheduler():
    def __init__(self):
        self.agent = DQN()
        self.last_state = np.zeros(6*NUM_A+7*NUM_F, dtype = np.int)
        self.last_action = 0
        self.last_througout = 0
        self.last_reward = 0
        self.key = []

    def train(self, actq, cptq):
        '''
        Generating control strategy and training model based on current flow information    
        input:
            actq: the infomation of active flows
            cptq: the infomation fo completed flows     
        '''
        #state
        state = self.stateparser(actq, cptq)

        #get action
        action = self.agent.egreedy_action(state) # e-greedy action for train

        #reward
        current_throughout = self.throughout(cptq)
        if self.last_througout == 0:
            if current_throughout == 0:
                reward = 0
            else:
                reward = 1 
        else:
            reward = current_throughout/self.last_througout
            if reward > 1:
                # (0, 1) U (1, +)
                reward = reward / 10
            else:
                # (-1, 0)
                reward = reward - 1

        done = False

        if reward != 0:
            #train
            self.agent.perceive(self.last_state,self.last_action,self.last_reward,state,done)

        #record state action and throughout
        self.last_state = state
        self.last_action = action
        self.last_reward = reward
        self.last_througout = current_throughout

        #analyzing the meaning of actions
        ret = self.actionparser(action)
        infostr = self.getinfo(state,action,reward)

        return ret,infostr


    def throughout(self, cptq):
        '''
        Computing the bandwidth of the completed flows
        Input:
            cptq: the infomation of completed flows
        '''
        res = 0.0
        for index, row in cptq.iterrows():
            res += row['size'] / row['duration']
        return res

    def stateparser(self, actq, cptq):
        '''
        Converting the active and completed flows information to a 1*136 state space
        Intput:
            actq: the infomation of active flows
            cptq: the infomation fo completed flows
        '''
        temp = actq.sort_values(by='sentsize')
        active_num = NUM_A
        finished_num = NUM_F
        state = np.zeros(active_num*6+finished_num*7, dtype = np.int)
        i = 0 
        self.key = []
        self.qindex_list = []
        for index, row in temp.iterrows():
            if i > active_num:
                break
            else:
                state[6*i] = row['src']
                state[6*i + 1] = row['dst']
                state[6*i + 2] = row['protocol']
                state[6*i + 3] = row['sp']
                state[6*i + 4] = row['dp']
                state[6*i + 5] = row['priority']
                self.key.append(index)
                self.qindex_list.append(row['qindex'])
            i += 1
        i = active_num
        for index, row in cptq.iterrows():
                state[6*active_num+7*(i-active_num)] = row['src']
                state[6*active_num+7*(i-active_num)+1] = row['dst']
                state[6*active_num+7*(i-active_num)+2] = row['protocol']
                state[6*active_num+7*(i-active_num)+3] = row['sp']
                state[6*active_num+7*(i-active_num)+4] = row['dp']
                state[6*active_num+7*(i-active_num)+5] = row['duration']
                state[6*active_num+7*(i-active_num)+6] = row['size']
                i += 1
        return state
    
    def actionparser(self, action):
        '''
        Converting 11-bit integer to control information
        Input:
            action: 11-bit integer as action
        '''
        bstr = ('{:0%sb}'%(NUM_A)).format(action)
        res = {}
        for i in range(len(self.key)):
            res[self.key[i]] = int(bstr[-1-i])
        return res

    def getinfo(self, state, action, reward):
        '''
        Generating the log info of this time training
        Input:
            state: state space
            action: action space
            reward: current reward
        '''
        infostr = ''
        line = '%50s\n'%(50*'*')
        rewardstr = 'Evaluation Reward: %f\n'%reward
        policy = 'State and action:\n'
        bstr = ('{:0%sb}'%(NUM_A)).format(action)
        for i in range(NUM_A):
            if i >= len(self.key):
                break
            else:
                policy += 'Queue index:%d, Five tuple={%d,%d,%d,%d,%d}, priority: %d, action:%s\n'%                           (self.qindex_list[i], state[6*i],state[6*i+1],state[6*i+2],state[6*i+3],state[6*i+4],             state[6*i+5],bstr[-1-i])
        infostr = line + rewardstr + policy + line
        return infostr 

