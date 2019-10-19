import matplotlib.pyplot as plt
import numpy as np

font_lable_e = {
    'family' : 'Times New Roman',
    'weight' : 'normal',
    'size'   : 14,
}
font_lable_c = {
    'family' : 'SimHei',
    'weight' : 'normal',
    'size'   : 14,
}

def log_analysis(logpath):
    cycle_duration = []
    rewards_list = []
    with open(logpath,'r') as f:
        line = f.readline()
        while(line):
            if 'Cyclic' in line:
                duration = int(line.split(' ')[-1])
                cycle_duration.append(duration)
            elif 'Reward' in line:
                reward = float(line.split(' ')[-1])
                rewards_list.append(reward)
            line = f.readline()
    return cycle_duration, rewards_list

def cycle_plot(cycle_duration):
    plt.figure()
    plt.xlabel('Cycle time',font_lable_e)
    plt.ylabel('Duration / ms',font_lable_e)
    plt.plot(cycle_duration)
    plt.savefig('cycle_duration_plot.png')

def reward_plot(rewards_list):
    rewards_arr = np.array(rewards_list)
    sum_arr = []
    for i in range(len(rewards_arr)):
        sum_arr.append(rewards_arr[0:i].sum())

    fig = plt.figure(figsize=(12,6))
    fig.add_subplot(1,2,1)
    plt.xlabel('Sampling period',font_lable_e)
    plt.ylabel('Reward',font_lable_e)
    plt.plot(rewards_arr)
    
    fig.add_subplot(1,2,2)
    plt.xlabel('Sampling period',font_lable_e)
    plt.ylabel('Total reward',font_lable_e)
    plt.plot(sum_arr)
    plt.savefig('reward_plot.png')

def fct_plot(logpath):
    duration_list = []
    with open(logpath,'r') as f:
        line = f.readline()
        while(line):
            if 'Duration' in line:
                duration = int(line.split(' ')[-2])
                duration_list.append(duration)
            line = f.readline()
    
    print(duration_list)
    
if __name__ == "__main__":

    logpath = 'log/log'

    # cycle_duration, rewards_list = log_analysis(logpath)   
    # cycle_plot(cycle_duration)
    # reward_plot(rewards_list) 
    fct_plot(logpath)