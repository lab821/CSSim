import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

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
    cpt_duration = []
    with open(logpath,'r') as f:
        line = f.readline()
        while(line):
            if 'Cyclic' in line:
                duration = int(line.split(' ')[-1])
                cycle_duration.append(duration)
            elif 'Reward' in line:
                reward = float(line.split(' ')[-1])
                rewards_list.append(reward)
            elif 'Duration' in line:
                index = line.split(':')[1].split(',')[0]
                duration = int(line.split(':')[-1].split(' ')[0])
                cpt_duration.append({'index':index, 'duration':duration})
            line = f.readline()
    return cycle_duration, rewards_list, cpt_duration

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

    fig = plt.figure(figsize=(18,6))
    fig.add_subplot(1,3,1)
    plt.xlabel('Sampling period',font_lable_e)
    plt.ylabel('Reward',font_lable_e)
    plt.plot(rewards_arr)
    
    fig.add_subplot(1,3,2)
    plt.xlabel('Sampling period',font_lable_e)
    plt.ylabel('Total reward',font_lable_e)
    plt.plot(sum_arr)
    
    #calculate CDF 
    rewards_arr.sort()
    plotDataset = [[],[]]
    count = len(rewards_arr)
    stat = [0,0,0,0,0]
    for i in range(count):
        plotDataset[0].append(rewards_arr[i])
        plotDataset[1].append((i+1)/count)
        if rewards_arr[i-1] == -1 and rewards_arr[i] != -1:
            stat[0] = i/count
        elif rewards_arr[i-1] < 0 and rewards_arr[i] == 0:
            stat[1] = i/count - stat[0]
        elif rewards_arr[i-1] == 0 and rewards_arr[i] > 0:
            stat[2] = i/count - stat[1] - stat[0]
        elif rewards_arr[i-1] < 1 and rewards_arr[i] == 1:
            stat[3] = i/count - stat[2] - stat[1] - stat[0]
            stat[4] = 1 - i/count

    print(stat)
    fig.add_subplot(1,3,3)
    plt.xlabel('Reward value',font_lable_e)
    plt.ylabel('CDF',font_lable_e)
    plt.plot(plotDataset[0],plotDataset[1])

    plt.savefig('reward_plot.png')

def avg_plot(cpt_duration):
    cpt_df = pd.DataFrame(cpt_duration, dtype = 'int').sort_values('index')
    cpt_df.to_csv('1.csv')
    avg_list = []
    for i in range(2,cpt_df.shape[0],3):
        avg = (cpt_df.iloc[i,0] + cpt_df.iloc[i-1,0] + cpt_df.iloc[i-2,0])/3
        avg_list.append(avg)

    plt.figure()
    plt.xlabel('Cycle time',font_lable_e)
    plt.ylabel('Avg flow duration / ms',font_lable_e)
    plt.plot(avg_list[500:])
    new_list = [x for x in avg_list[-500:] if x > 6300]
    print(len(new_list)/len(avg_list[-500:]))
    print(min(avg_list[500:]))
    plt.savefig('ft_avg_plot.png')

if __name__ == "__main__":
    logpath = 'log-2'
    cycle_duration, rewards_list, cpt_duration = log_analysis(logpath)   
    cycle_plot(cycle_duration)
    reward_plot(rewards_list) 
    avg_plot(cpt_duration)

    