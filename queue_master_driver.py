#!/usr/bin/python
# queue_master_driver.py
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import threading
from time import sleep
from Queue import Queue
import re
import subprocess
import sys
import os

home_dir = os.environ['HOME']
queue_dir = home_dir + '/Codes/queue_hlrs'

def node_alloc():
    queue_script = 'queue'
    command = 'salloc -N1 -t 21-0 -K -A ithkoehn ' + queue_script + ' &'
    salloc = subprocess.Popen(command,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    salloc_stderr = salloc.stderr.readline()
    re_res = re.match('salloc: Granted job allocation (\d+)', salloc_stderr)
    job_id = re_res.group(1)
    command = 'squeue | grep ' + job_id
    squeue = subprocess.Popen(command,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    saloc_stdout = squeue.stdout.readline().split()
    node = saloc_stdout[-1]

    init_script = 'source ~/.bash_profile; init_q'
    alloc = subprocess.Popen(["ssh", node, init_script],
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    print "stdout: ", alloc.stdout.readlines()
    print "stderr: ", alloc.stderr.readlines()
    
    return (job_id, node)


def node_dealloc( jobID, nodeID):
    term_script = 'source ~/.bash_profile; term_q'
    dealloc = subprocess.call(["ssh", "%s" % nodeID, term_script],
                              shell=True)

    command = 'scancel ' + jobID
    dealloc = subprocess.call(command,
                              shell=True)
    
    


# Job sender - send a job to a node
#
class job_send( threading.Thread):
    
    def __init__( self, node, job, jobID):
        threading.Thread.__init__( self)
        self.node = node
        self.job = job
        self.jobID = jobID

    def run( self):
        command = 'source ~/.bash_profile; cd ' + self.job[1] + '; ' + self.job[0]
        ssh = subprocess.Popen(["ssh", "%s" % self.node, command],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
        print "stdout: ", ssh.stdout.readlines()
        print "stderr: ", ssh.stderr.readlines()

# A node.
#
class node_connection( threading.Thread):

    def __init__( self):
        threading.Thread.__init__( self)
        self.max_jobs = 20
        self.queue = Queue()
        self.jobs = []
        self.jobs_lock = threading.RLock()

        new_node = node_alloc()
        self.alloc_id = new_node[0]
        self.node_id = new_node[1]
        
        self.life = threading.Event()
        self.life.set()
        
    def run( self):

        while self.life.is_set() or not( self.jobs):
            new_job = self.queue.get()

            if ( isinstance( new_job, str)):
                if ( info == 'kill'):
                    self.life.clear()

            else:

                # new_job = [jobID, job_type, job_info]
                j = job_send( self.node_id, new_job[2], new_job[0])
                j.start()
                self.jobs_lock.acquire()
                self.jobs.append(j)
                self.jobs_lock.release()

        sleep(1)
        m.exit()
        node.close()


    def kill( self):
        print 'queue_master_driver: killing node connection'
        self.queue.put( 'kill')


# Control jobs subimission
# 
#
class submission_control( threading.Thread):
    def __init__( self):
        threading.Thread.__init__( self)
        self.node_list = []
        self.jobs_queue = Queue()
        self.queue_lock = threading.RLock()

    def run( self):
        while True:
            
            # Check finished jobs
            for node_c in self.node_list:
                node_c.jobs_lock.acquire()
                for running_job in node_c.jobs:
                    if (not (running_job.is_alive())):
                        node_c.jobs.remove(running_job)
                node_c.jobs_lock.release()

            # Send a job, if there is space
            self.queue_lock.acquire()
            if (not( self.jobs_queue.empty())):
                best_free = 0
                best_node = None
                for node_c in self.node_list:
                    free = node_c.max_jobs - len(node_c.jobs)
                    if (free > best_free):
                        best_node = node_c
                        best_free = free
                if (best_node != None):
                    best_node.queue.put( self.jobs_queue.get())
            self.queue_lock.release()


            sleep(1)

# Get job from client and send it for submission
# message from client must be:
#   (job_type, job)
# where:
#   job_type is
#       0 - submit a job
#       1 - kill a job
#       2 - add a node or change max_jobs
#       3 - remove a node
#       4 - show nodes
#       5 - show jobs
#              
#   job is a list with the job informations
#
def handle_client( sub_ctrl, jobId):
    print "queue_master_driver: ready"
    server_master = Listener(( "localhost", 16011), authkey = 'qwerty')
    while True:
        client_master = server_master.accept()
        (job_type, job) = client_master.recv()

        # Send or kill a job
        if (job_type == 0 or job_type == 1):
            client_master.send( 'Job received.')
            sub_ctrl.queue_lock.acquire()
            sub_ctrl.jobs_queue.put( (jobId, job_type, job))
            sub_ctrl.queue_lock.release()
            jobId += 1

        # Add a node or change max_jobs
#        elif (job_type == 2):
            # node_found = False
            # for node in sub_ctrl.node_list:
            #     if node.add == job[0]:
            #         node.max_jobs = job[1]
            #         client_master.send( 'max_jobs changed to ' + str( job[1]) + ' for node ' + job[0] + '.')
            #         node_found = True
            #         break
            # if (not( node_found)):
            #     node = node_connection( job[0], job[1])
            #     node.start()
            #     sub_ctrl.node_list.append( node)
            #     client_master.send( 'Node ' + job[0] + ' added, with max_jobs set to ' + str( job[1]) + '.')

        # Kill a job
        elif (job_type == 3):
            client_master.send( 'Kill job: not yet implemented.')

        # Show the nodes
        elif (job_type == 4):
            cur_nodes = ''
            for node in sub_ctrl.node_list:
                cur_nodes += node.node_id + ' ' + str( len( node.jobs)) + '/' + str( node.max_jobs) + '\n'
            client_master.send( cur_nodes)

        # Show the jobs
        elif (job_type == 5):

            nd_jobs = ''
            cur_jobs = ''

            sub_ctrl.queue_lock.acquire()
            n_q = sub_ctrl.jobs_queue.qsize()
            for i in range( 0, n_q):
                j = sub_ctrl.jobs_queue.get()
                nd_jobs += str( j[0]) + ':' + j[2][0] + ' (wd: ' + j[2][1] + ')\n';
                sub_ctrl.jobs_queue.put( j)
            sub_ctrl.queue_lock.release()

            cur_jobs += 'Queue' + ':\n' + nd_jobs



            for node in sub_ctrl.node_list:
                nd_jobs = ''
                for j in node.jobs:
                    nd_jobs += str( j.jobID) + ':' + j.job[0] + ' (wd: ' + j.job[1] + ')\n';
                if (nd_jobs):
                    cur_jobs += node.node_id + ':\n' + nd_jobs

            client_master.send( cur_jobs)
            
        else:
            pass




sub_ctrl = submission_control()
sub_ctrl.start()

node = node_connection()
node.start()
sub_ctrl.node_list.append( node)
print 'Node ' + node.node_id + ' added, with alloc_id to ' + node.alloc_id


#node_dealloc('26402')


handle_client( sub_ctrl, 1)
print "queue_master_driver: done!"
