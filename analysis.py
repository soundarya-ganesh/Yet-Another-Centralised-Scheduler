import time
from time import *
from datetime import *
import statistics
import matplotlib.pyplot as plt
import numpy as np

#print(lt[0].split(' '))
#print(lt[2].split(' ')[1].split('\t')[1])
#['2020-12-04', '01:37:01.372199:\tINCOMING', 'CONNECTION', 'ESTABLISHED', '=', '[host:127.0.0.1,', 'port:65387]\n']
#
#['2020-12-04', '01:37:01.409068:\tSTARTED', 'EXECUTING', 'TASK', '=', '[job_id:0,', 'task_id:0_M0]\n']
#['2020-12-04', '01:37:04.230565:\tFINISHED', 'EXECUTING', 'TASK', '=', '[job_id:0,', 'task_id:0_M2]\n']


def diff(dt1, dt2):
    timedelta = dt1 - dt2
    return timedelta.days * 24 * 3600 + timedelta.seconds


def time_tasks(file_name):
    file = open(file_name, 'r')
# print(file.readlines())
    lt = file.readlines()
    start = {}
    time = []  # List for Times
    for k in lt:
        k = k.split(' ')
        t = k[1].split('\t')[1]
        if t in ['Started', 'Finished']:
            if t == 'Started':
                start[k[5] + k[6]] = k[0] + " " + k[1].split('\t')[0][:-1]
            else:
                t1 = datetime.strptime(
                    k[0]+" " + k[1].split('\t')[0][:-1], '%Y-%m-%d %H:%M:%S.%f')
                t2 = datetime.strptime(
                    start[k[5] + k[6]], '%Y-%m-%d %H:%M:%S.%f')
                time.append(diff(t1, t2))
    print("TASK_MEAN:")
    print(statistics.mean(time))
    print("TASK_MEDIAN:")
    print(statistics.median(time))

    plt.hist(time)
    plt.title("Time taken for task completion " + str(file_name))
    plt.show()


def time_jobs(file_name):
    file = open(file_name, 'r')
    # print(file.readlines())
    lt = file.readlines()
    start = {}
    # List of Times.....................................................................................................
    time = []
    for k in lt:
        k = k.split(' ')
        # print(k[1])
        t = k[1].split('\t')[1]
        if t in ['Received', 'Finished']:
            if t == 'Received':
                start[k[5][1:-1]] = k[0] + " " + k[1].split('\t')[0][:-1]
            else:
                t1 = datetime.strptime(
                    k[0]+" " + k[1].split('\t')[0], '%Y-%m-%d %H:%M:%S.%f')
                if k[5][1:-2] in start.keys():
                    t2 = datetime.strptime(
                        start[k[5][1:-2]], '%Y-%m-%d %H:%M:%S.%f')
                    time.append(diff(t1, t2))

    print(file_name)
    print("JOB_MEAN:")
    print(statistics.mean(time))
    print("JOB_MEDIAN:")
    print(statistics.median(time))

    plt.hist(time)
    plt.title("Time taken: job completion " + str(file_name))
    plt.show()


paths1 = ['RR/master.txt', 'LL/master.txt', 'RANDOM/master.txt']
paths2 = ['RR/worker_1.txt', 'LL/worker_1.txt', 'RANDOM/worker_1.txt']

i = 0
for i in range(3):
    time_tasks(paths2[i])
    time_jobs(paths1[i])
    print("----------------------------------------------------------------------------------------------------")


'''
1.Open necessary log file required
2.Read all Line Sequentially and Store it list of strings
3.For Each String in List Split the String with Space
4.Through Index Analysis
    i}Tasks:
        If Status found is "Started" OR "Finished"  proceed
            a)If Started, Extract Time as Value and ID as Key and Store it to "start" Dictionary
            b)If Finished, Extract ID  and Time and Get start time using "start" Dictionary using ID Extracted in "Finished"
            c)Calculate the Time difference and append to list.
        Find the MEAN and MEDIAN of diff_list.
    ii)Jobs:
        If Status found is "RECEIVED" OR "Finished"  proceed
            a)If RECEIVED, Extract Time as Value and ID as Key and Store it to "start" Dictionary
            b)If Finished, Extract ID  and Time and Get start time using "start" Dictionary using ID Extracted in "Finished"
            c)Calculate the Time difference and append to list.
        Find the MEAN and MEDIAN of diff_list.
        
                
'''
