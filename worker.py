# -*- coding: utf-8 -*-
"""
Created on Wed Nov 30 22:18:02 2020

@author: Soundarya Ganesh
"""
import sys
import random
import threading
import json
from socket import *
import time
import numpy as np
from time import *
from datetime import *
import os

w_id = sys.argv[2]

class Task:
    def __init__(self, job_id, task_id, remaining_time):
        self.job_id = job_id
        self.task_id = task_id
        self.remaining_time = remaining_time

    def reduce(self):
        self.remaining_time -= 1

    def time_check(self):
        remaining_time = self.remaining_time
        if (remaining_time == 0):
            return 1
        else:
            return 0

def func_connect_est(path2file, flag, host, port):  # 0-incoming 1-outgoing
    with open(path2file, "a+") as fp:
        if(flag == 0):
            print_lock.acquire()
            fp.write(str(datetime.now(
            )) + "\tIN: Connection Established :host is {0}, port used is {1}]\n".format(host, port))
            print_lock.release()

        elif(flag == 1):
            print_lock.acquire()
            fp.write(str(datetime.now(
            )) + "\tOUT: Connection Established :host is {0}, port used is {1}]\n".format(host, port))
            print_lock.release()

def func_log_worker_start(path2file, i, port, slot):
    with open(path2file, "a+") as f:
        print_lock.acquire()
        f.write(str(datetime.now()) + ":\tWorker has started = [worker_id:{0}, port:{1}, slots:{2}]\n".format(i,
            port,slot))
        print_lock.release()

def func_log_update_exec(path2file,flag, job_id, task_id):
    with open(path2file, "a+") as f:
        if(flag == 0):
            print_lock.acquire()

            f.write(str(datetime.now()) + ":\tUpdate sent to master = [job_id:{0}, task_id:{1}] completed\n".format(job_id, task_id))
            print_lock.release()            
        
        if(flag==1):
            print_lock.acquire()
            f.write(str(datetime.now()) + ":\tFinished executing task = [job_id:{0}, task_id:{1}]\n".format(job_id, task_id))
            print_lock.release()
        if(flag==2):
            print_lock.acquire()

            f.write(str(datetime.now()) + ":\tStarted executing task = [job_id:{0}, task_id:{1}]\n".format(job_id,task_id))
            print_lock.release()
        
        
          

def func_log_receive(path2file, job_id, task_id, duration): #check
    with open(path2file, "a+") as f:
                        print_lock.acquire()
                        f.write(str(datetime.now()) + ":\tReceived task = [job_id:{0}, task_id:{1}, duration:{2}]\n".format(
                            job_id,
                            task_id,
                            duration))
                        print_lock.release()



def start_execute_task(w_id, task_data):
    global job_id, task_id, remaining_time
    task = Task(task_data["job_id"],
                task_data["task_id"], task_data["duration"])
    job_id = task_data["job_id"]
    task_id = task_data["task_id"]
    remaining_time = task_data["duration"]
    n=len(execn_pool)
    path2file= "proj_log/worker_" + str(w_id) + ".txt"
    func_log_update_exec(path2file,2, task_data["job_id"], task_data["task_id"])
    for i in range(n):
        if (isinstance(execn_pool[w_id-1][i], int) and (execn_pool[w_id-1][i] == 0 and num_free_slots[w_id-1] > 0)):
            execn_pool[w_id-1][i] = task
            num_free_slots[w_id-1] -= 1
            break


def func_receive_task_start(w_id):
    skt = socket(AF_INET, SOCK_STREAM)
    with skt:
        skt.bind(("localhost", ports[w_id-1]))
        skt.listen(1024)
        while (1):
            connectn, addr = skt.accept()
            path2file= "proj_log/worker_"+str(w_id)+".txt"
            func_connect_est(path2file, 0, addr[0], addr[1])
            
            with connectn:
                task_start_data = connectn.recv(1024).decode()
                if task_start_data:
                    task = json.loads(task_start_data)
                    path2file= "proj_log/worker_"+str(w_id)+".txt"
                    func_log_receive(path2file, task["job_id"], task["task_id"], task["duration"])
                    workerLock.acquire()
                    start_execute_task(w_id, task)
                    workerLock.release()


def worker_task_execute(w_id):
    while (1):
        if(slots[w_id-1] == 0):
            continue
        for i in range(slots[w_id-1]):
            if (isinstance(execn_pool[w_id-1][i], int) and execn_pool[w_id-1][i] == 0):
                continue
            elif (execn_pool[w_id-1][i].time_check()):
                task = execn_pool[w_id-1][i]
                job_id = task.job_id
                task_id = task.task_id
                store_t_data = {
                    "worker_id": w_id,
                    "job_id": job_id,
                    "task_id": task_id
                }
                path2file= "proj_log/worker_"+str(w_id)+".txt"
                func_log_update_exec(path2file,1, job_id, task_id)

                workerLock.acquire()
                #remove_task(w_id, i)
                execn_pool[w_id-1][i] = 0
                num_free_slots[w_id-1] += 1
                workerLock.release()

                t_data = json.dumps(store_t_data)
                skt = socket(AF_INET, SOCK_STREAM)
                with skt:
                    skt.connect(('localhost', 5001))
                    path2file="proj_log/worker_"+str(w_id)+".txt"
                    func_connect_est(path2file, 1, "localhost", "5001")
                    skt.send(t_data.encode())
                path2file="proj_log/worker_"+str(w_id)+".txt"
                func_log_update_exec(path2file,0, job_id, task_id)
            else:
                workerLock.acquire()
                execn_pool[w_id-1][i].reduce()
                workerLock.release()
        sleep(1)


workerLock = threading.Lock()
print_lock = threading.Lock()  

slots = list()
ports = list()

num_free_slots = list()
execn_pool = list()



with open("config.json") as f:
    config = json.load(f)
for worker in config['workers']:
    ports.append(worker['port'])
    slots.append(worker['slots'])
    num_free_slots.append(worker['slots'])
    execn_pool.append([0 for i in range(worker['slots'])])

count = len(ports)
for i in range(count):
    path2file="proj_log/worker_" + str(w_id)+ ".txt"
    func_log_worker_start(path2file,i+1,ports[i],slots[i])


if __name__ == "__main__":
    try:
        os.mkdir('proj_log')
    except:
        pass  
    f = open("proj_log/worker_"+w_id+".txt", "w")
    f.close()
    if (len(sys.argv) != 3):
        sys.exit()
    worker_port = int(sys.argv[1])
    worker_id = int(sys.argv[2])
    ports[worker_id - 1] = worker_port
    thread_receive_task_start = threading.Thread(
        target=func_receive_task_start, args=(worker_id,))
    thread_task_execute = threading.Thread(
        target=worker_task_execute, args=(worker_id,))
    thread_receive_task_start.start()
    thread_task_execute.start()
    thread_receive_task_start.join()
    thread_task_execute.join()
