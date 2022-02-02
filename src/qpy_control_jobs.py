""" qpy - Threads to check and control the jobs

"""
import os
import sys
import threading
from datetime import datetime
from time import sleep
from queue import Queue

import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_communication as qpycomm
import qpy_nodes_management as qpynodes


class CheckRun(threading.Thread):
    """Check if the jobs are still running.
    
    Attributes:
    jobs (Job_Collection)      All jobs being handled by qpy-master
    multiuser_alive (Event)    It is set if the multiuser is alive
    config (Configurations)    qpy configurations
    finish (Event)             Set this Event to terminate this Thread
    
    Behaviour:
    This thread makes regular checks on the running jobs
    to see if they have been finished.
    If so, the job is changed from running to done
    and a message is sent to qpy-multiuser.
    
    This is done at each config.sleep_time_check_run seconds.
    """

    __slots__ = (
        'jobs',
        'multiuser_alive',
        'config',
        'finish')
    
    def __init__(self, jobs, multiuser_alive, config):
        """Initiate the class.

        Arguments:
        jobs (Job_Collection)       All jobs being handled by qpy-master
        multiuser_alive (Event)     It is set if the multiuser is alive
        config (Configurations)     qpy configurations
        """
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive
        self.config = config
        threading.Thread.__init__(self)
        self.finish = threading.Event()

    def run(self):
        """Check if the jobs are running, see class documentation."""
        while not self.finish.is_set():
            i = 0
            jobs_modification = False
            nodes_down, all_running_jobs = self.jobs.fetch_running_jobs()
            with self.jobs.lock:
                jobs_to_check = list(self.jobs.running)
            self.config.logger.debug('nodes_down:\n%s', nodes_down)
            self.config.logger.debug('all_running_jobs:\n%s', all_running_jobs)
            self.config.logger.debug('jobs_to_check:\n%s', jobs_to_check)
            for job in jobs_to_check:
                if (job.node not in nodes_down
                    and job.ID not in all_running_jobs
                        and job.status == qpyconst.JOB_ST_RUNNING):
                    multiuser_down = job.end_running(qpyconst.JOB_ST_DONE,
                                                     len(self.jobs.queue),
                                                     self.config)
                    if multiuser_down:
                        self.multiuser_alive.clear()
                    else:
                        self.multiuser_alive.set()
                    jobs_modification = True
                    self.jobs.mv(job, self.jobs.running, self.jobs.done)
                    self.skip_job_sub = 0
                    self.config.logger.info('Job %s changed to done.', job.ID)
            if jobs_modification:
                self.jobs.write_all_jobs()
                jobs_modification = False
            sleep(self.config.sleep_time_check_run)


class JobsKiller(threading.Thread):
    """Kill the jobs when required.
    
    Attributes:
    jobs (Job_Collection)      All jobs being handled by qpy-master
    multiuser_alive (Event)    It is set if the multiuser is alive
    config (Configurations)    qpy configurations
    to_kill (Queue)            It receives the jobs to kill
    
    Behaviour:
    This thread waits for jobs to be killed, passed
    by the Queue to_kill. These have to be a JOB instance or the
    string "kill". In this later case, the thread is terminated.

    TODO:
    When we qpy kill all, not all of them are killed, and
    we have to kill several times
    """

    __slots__ = (
        'jobs',
        'multiuser_alive',
        'config',
        'to_kill')
    
    def __init__(self, jobs, multiuser_alive, config):
        """Initiate the class.

        Arguments:
        jobs (Job_Collection)      All jobs being handled by qpy-master
        multiuser_alive (Event)    It is set if the multiuser is alive
        config (Configurations)    qpy configurations
        """
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive
        self.config = config
        threading.Thread.__init__(self)
        self.to_kill = Queue()

    def run(self):
        """Kill jobs, see class documentation."""
        while True:
            job = self.to_kill.get()
            if isinstance(job, str):
                if job == 'kill':
                    break
            command = ('python3 '
                       + qpysys.source_dir + '/qpy_job_killer.py '
                       + str(job.ID))
            self.config.logger.info('Killing job %s on %s', job.ID, job.node)
            try:
                if job.status != qpyconst.JOB_ST_RUNNING:
                    raise Exception('Job status is not running.')
                (std_out, std_err) = qpycomm.node_exec(
                    job.node.address,
                    command,
                    pKey_file=self.config.ssh_p_key_file)
                self.config.logger.debug('stdout of killing job:\n%r\n'
                                         'stderr of killing job:\n%r',
                                         std_out, std_err)
            except Exception as e:
                self.config.logger.warninig('Exception when killing job:\n%s', e)
            else:
                multiuser_down = job.end_running(qpyconst.JOB_ST_KILLED,
                                                 len(self.jobs.queue),
                                                 self.config)
                if multiuser_down:
                    self.multiuser_alive.clear()
                else:
                    self.multiuser_alive.set()
                self.jobs.mv(job, self.jobs.running, self.jobs.killed)
                self.jobs.write_all_jobs()
                self.config.logger.info('Job %s changed to killed.', job.ID)


class Submission(threading.Thread):
    """Control the job submission.
    
    Attributes:
    jobs (Job_Collection)          All jobs being handled by qpy-master
    muHandler (MULTIUSER_HANDLER)  The multiuser_handler
    config (Configurations)        qpy configurations
    finish (Event)                 Set this Event to terminate this Thread
    skip_jobs_submission (int)     Skip the submission by this amount of cicles
    submit_jobs (bool)             Jobs are submitted only if True
    
    Behaviour:
    This thread looks the jobs.queue and try to submit tje jobs,
    asking for a node to qpy-multiuser and making the submission if
    a node is given.

    Each cycle is done at each config.sleep_time_sub_ctrl seconds.
    """

    __slots__ = (
        'jobs'
        'muHandler'
        'config'
        'finish'
        'skip_job_sub'
        'submit_jobs')

    def __init__(self, jobs, muHandler, config):
        """Initiate the class.
        
        Arguments:
        jobs (Job_Collection)            All jobs being handled by qpy-master
        muHandler (MULTIUSER_HANDLER)    The multiuser_handler
        config (Configurations)          qpy configurations
        """
        self.jobs = jobs
        self.muHandler = muHandler
        self.config = config
        threading.Thread.__init__(self)
        self.finish = threading.Event()
        self.skip_job_sub = 0
        self.submit_jobs = True

    def run(self):
        """Submit the jobs, see class documentation."""
        jobs_modification = False
        if not self.muHandler.multiuser_alive.is_set():
            self.skip_job_sub = 30
        i_next_job = -1
        while not self.finish.is_set():
            if ((not self.muHandler.multiuser_alive.is_set())
                    and self.skip_job_sub == 0):
                self.muHandler.add_to_multiuser()
                if not self.muHandler.multiuser_alive.is_set():
                    self.skip_job_sub = 30
            if self.submit_jobs and self.skip_job_sub == 0:
                if len(self.jobs.Q) != 0:
                    i_next_job = (i_next_job
                                  if abs(i_next_job) <= len(self.jobs.Q) else
                                  -1)
                    next_jobID = self.jobs.Q[i_next_job].ID
                    next_Ncores = self.jobs.Q[i_next_job].n_cores
                    next_mem = self.jobs.Q[i_next_job].mem
                    next_node_attr = self.jobs.Q[i_next_job].node_attr
                    try:
                        msg_back = qpycomm.message_transfer(
                            (qpyconst.MULTIUSER_REQ_CORE,
                             (qpysys.user,
                              next_jobID,
                              next_Ncores,
                              next_mem,
                              len(self.jobs.queue),
                              next_node_attr)),
                            qpycomm.multiuser_address,
                            qpycomm.multiuser_port,
                            qpycomm.multiuser_key)
                    except:
                        self.muHandler.multiuser_alive.clear()
                        self.config.messages.add(
                            'SUB_CTRL: Exception in message transfer: '
                            + str(sys.exc_info()[0]) + '; '
                            + str(sys.exc_info()[1]))
                        self.config.logger.error(
                            'Exception in SUB_CTRL message transfer',
                            exc_info=True)
                        self.skip_job_sub = 30
                    else:
                        status, allocated_node = msg_back
                        self.muHandler.multiuser_alive.set()
                        self.config.logger.info('Message from multiuser:\n%s',
                                                msg_back)
                        if status == 0:
                            job = self.jobs.Q[i_next_job]
                            job.node = self.jobs.add_node(allocated_node)
                            self.config.logger.debug('Submitting job in %r',
                                                     job.node)
                            try:
                                job.run(self.config)
                            except:
                                self.config.logger.error(
                                    "Exception in when submitting job",
                                    exc_info=True)
                                multiuser_down = job.end_running(qpyconst.JOB_ST_QUEUE,
                                                                 len(self.jobs.queue),
                                                                 self.config)
                                job.node = None
                                if multiuser_down:
                                    self.muHandler.multiuser_alive.clear()
                                else:
                                    self.muHandler.multiuser_alive.set()
                            else:
                                job = self.jobs.Q_pop(i_next_job)
                                self.jobs.mv(job,
                                             self.jobs.queue,
                                             self.jobs.running)
                                jobs_modification = True
                        elif status == 1 and abs(i_next_job) < len(self.jobs.Q):
                            i_next_job -= 1
                        else:
                            i_next_job = -1
                            self.skip_job_sub = 30
                if jobs_modification:
                    self.jobs.write_all_jobs()
                    jobs_modification = False
            else:
                if self.skip_job_sub > 0:
                    self.skip_job_sub -= 1
                self.config.messages.add("SUB_CTRL: Skipping job submission.")
            sleep(self.config.sleep_time_sub_ctrl)
