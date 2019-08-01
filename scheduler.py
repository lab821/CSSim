#The scheduler module
#TODO: Implementing a General Scheduling Algorithms Interface

from algorithms.dqn import DQN

def scheduler(actq, cptq):
    res = {}
    return res

class DQNscheduler():
    def __init__(self):
        self.agent = DQN()
        self.last_state = 136*[0]
        self.last_action = 0

    def train(self,state):
        # initialize task
        state = state
        # Train
        
        action = self.agent.egreedy_action(state) # e-greedy action for train
        reward = calculcate
        done = False
    
        agent.perceive(self.last_state,self.last_action,reward,state,done)
        self.last_state = state
        self.last_action = action

        return action
        print('info')

