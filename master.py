
import os
import re
import threading
import json
import sys
import random
from datetime import *
from socket import *
from _thread import *

NO_OF_WORKERS = 3
FREE_SLOT = 0


# 0-received 1-finish execution of job
def func_log_job_request(path2file, flag, job_id, list_mapID, list_reducerID):
    with open(path2file, "a+") as fp:
        if(flag == 0):
            print_lock.acquire()
            fp.write(str(datetime.now()) + "\tReceived job request = [job_id:{0}, list_mapID:{1}, list_reducerID:{2}]\n".format(
                job_id, list_mapID, list_reducerID))
            print_lock.release()

        elif(flag == 1):
            print_lock.acquire()
            fp.write(str(datetime.now()) +
                     "\tFinished job execution = [job_id:{0}]\n".format(job_id))
            print_lock.release()


def func_task_to_worker(path2file, worker_id, task):
    with open(path2file, "a+") as fp:
        print_lock.acquire()
        fp.write(str(datetime.now()) + "\tTask sent to worker= [worker_id:{0}, task_id:{1}, job_id:{1}, duration:{1}]\n".format(
            worker_id, task['task_id'], task['job_id'], task['duration']))
        print_lock.release()


def func_upd_from_worker(path2file, worker_id, task):
    with open(path2file, "a+") as fp:
        print_lock.acquire()
        fp.write(str(datetime.now()) + "\tUpdate received from worker = [worker_id:{0}, task_id:{1}] Task Completed\n"
                 .format(worker_id, task))
        print_lock.release()


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


def RANDOM_scheduling():
    while (1):
        worker_id = random.randrange(1, NO_OF_WORKERS+1)
        if num_free_slots[worker_id]['slots'] > FREE_SLOT:
            return worker_id


def RR_scheduling():
    while (1):
        for worker_id in range(1, NO_OF_WORKERS+1):
            if num_free_slots[worker_id]['slots'] > FREE_SLOT:
                return worker_id


def LL_scheduling():
    res_worker_id = 1
    i = 2
    while(i <= NO_OF_WORKERS):
        # for worker_id in range(2, NO_OF_WORKERS+1):
        curr_worker_slots = num_free_slots[i]['slots']
        greater_slots = num_free_slots[res_worker_id]['slots']
        if curr_worker_slots > greater_slots:
            res_worker_id = i
        i += 1
    return res_worker_id


def task_scheduler(scheduling_algo):
    global task_wait_buffer
    while(1):
        list_exist = len(task_wait_buffer)
        if(list_exist):
            for task in task_wait_buffer:
                if scheduling_algo == "RR":
                    worker_id = RR_scheduling()
                elif scheduling_algo == "RANDOM":
                    worker_id = RANDOM_scheduling()
                elif scheduling_algo == "LL":
                    worker_id = LL_scheduling()
                else:
                    print("Invalid Argrument- Enter RANDOM, RR, LL")
                    sys.exit()
                port_number = num_free_slots[worker_id]['port']
                task = task_wait_buffer[0]
                task_wait_buffer.remove(task)
                task_message = json.dumps(task)

                with socket(AF_INET, SOCK_STREAM) as skt:
                    skt.connect(("localhost", port_number))
                    func_connect_est("proj_log/master.txt", 1,
                                     "localhost", port_number)
                    skt.send(task_message.encode())
                func_task_to_worker("proj_log/master.txt", worker_id, task)

                print("Sent task", task['task_id'], "to worker",
                      worker_id, "on port_number", port_number)

                lock_worker.acquire()
                num_free_slots[worker_id]['slots'] -= 1
                lock_worker.release()


def listen_requests():
    global counting_MapT
    global task_wait_buffer
    port = 5000
    skt = socket(AF_INET, SOCK_STREAM)
    global list_reduced_task
    global job_task_count

    with skt:
        skt.bind(("", port))
        skt.listen(1000)
        while(1):
            connectn, addr = skt.accept()
            func_connect_est("proj_log/master.txt", 0, addr[0], addr[1])

            with connectn:
                request_struct = connectn.recv(100000)
                list_mapID = list()
                list_reducerID = list()
                request_struct = json.loads(request_struct)

                for ele in request_struct['map_tasks']:
                    list_mapID.append(ele['task_id'])

                for ele in request_struct['reduce_tasks']:
                    list_reducerID.append(ele['task_id'])

                func_log_job_request(
                    "proj_log/master.txt", 0, request_struct['job_id'], list_mapID, list_reducerID)

                no_map_task = len(request_struct['map_tasks'])
                no_reduce_task = len(request_struct['reduce_tasks'])

                counting_MapT[request_struct['job_id']] = dict()

                counting_MapT[request_struct['job_id']] = no_map_task
                job_task_count[request_struct['job_id']
                               ] = no_map_task + no_reduce_task

                list_reduced_task[request_struct['job_id']] = list()
                tasks = request_struct['reduce_tasks']
                task = dict()
                for ele in tasks:
                    task[ele['task_id']] = dict()
                    task[ele['task_id']]['task_id'] = ele['task_id']
                    task[ele['task_id']]['duration'] = ele['duration']
                    task[ele['task_id']]['job_id'] = request_struct['job_id']
                    list_reduced_task[request_struct['job_id']].append(
                        task[ele['task_id']])

                tasks = request_struct['map_tasks']
                task = dict()
                for ele in tasks:
                    task[ele['task_id']] = dict()
                    task[ele['task_id']]['task_id'] = ele['task_id']
                    task[ele['task_id']]['duration'] = ele['duration']
                    task[ele['task_id']]['job_id'] = request_struct['job_id']
                    task_wait_buffer.append(task[ele['task_id']])


def getUpdatesWorkers():
    global counting_MapT
    global num_free_slots
    global job_task_count

    port = 5001
    skt = socket(AF_INET, SOCK_STREAM)
    with skt:
        skt.bind(("", port))
        skt.listen(1000)
        while(1):
            connectn, addr = skt.accept()
            func_connect_est("proj_log/master.txt", 0, addr[0], addr[1])
            with connectn:
                update_json = connectn.recv(1000)
                update = json.loads(update_json)
                worker_id = update['worker_id']
                job_id = update['job_id']
                task_id = update['task_id']
                func_upd_from_worker("proj_log/master.txt", worker_id, task_id)

                print("Task", task_id, "completed execution in", worker_id)
                job_task_count[job_id] -= 1
                job_comp_list1 = []
                job_comp_list2 = []
                if job_task_count[job_id] == 0:
                    func_log_job_request(
                        "proj_log/master.txt", 1, job_id, job_comp_list1, job_comp_list2)

                lock_worker.acquire()
                num_free_slots[worker_id]['slots'] += 1
                lock_worker.release()
                map_job_id = counting_MapT[job_id]
                if(re.search(r'M', task_id)):
                    map_job_id -= 1
                    if map_job_id == 0:
                        for reduce_task in list_reduced_task[job_id]:
                            task_wait_buffer.append(reduce_task)


num_free_slots = dict()
counting_MapT = dict()
job_task_count = dict()
list_reduced_task = dict()
task_wait_buffer = list()

lock_worker = threading.Lock()
print_lock = threading.Lock()

if __name__ == '__main__':
    try:
        os.mkdir('proj_log')
    except:
        pass
    fp = open("proj_log/master.txt", "w")
    fp.close()
    path_config = sys.argv[1]
    with open(path_config) as fp:
        config = json.load(fp)

    for worker in config['workers']:
        num_free_slots[worker['worker_id']] = {
            'slots': worker['slots'], 'port': worker['port']}
    print("Master listening...............")

    thread_requests = threading.Thread(target=listen_requests)
    thread_update = threading.Thread(target=getUpdatesWorkers)
    thread_scheduler = threading.Thread(
        target=task_scheduler, args=(sys.argv[2],))

    thread_requests.start()
    thread_update.start()
    thread_scheduler.start()

    thread_requests.join()
    thread_update.join()
    thread_scheduler.join()
