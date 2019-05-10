"""Main threads to check and control the jobs

"""
import os
import sys
import threading
from datetime import datetime
from time import sleep
from Queue import Queue

import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_communication as qpycomm


if (qpycomm.multiuser_port == None):
    sys.exit("Information for multiuser connection could not be obtained."
             + " Contact your administrator.")

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
            with self.jobs.lock:
                jobs_to_check = list(self.jobs.running)
            for job in jobs_to_check:
                try:
                    is_running = job.is_running(self.config)
                except:
                    self.config.logger.error('Exception in CHECK_RUN.is_running',
                                             exc_info=True)
                    self.config.messages.add('CHECK_RUN: Exception in is_running: '
                                             + repr(sys.exc_info()[0]))
                    is_running = True
                if (not is_running and job.status == qpyconst.JOB_ST_RUNNING):
                    job.status = qpyconst.JOB_ST_DONE
                    job.end_time = datetime.today()
                    job.run_duration()
                    jobs_modification = True
                    self.jobs.mv(job, self.jobs.running, self.jobs.done)
                    self.skip_job_sub = 0
                    if (job.cp_script_to_replace != None):
                        if (os.path.isfile(job.cp_script_to_replace[1])):
                            os.remove(job.cp_script_to_replace[1])

                    try:
                        msg_back = qpycomm.message_transfer((qpyconst.MULTIUSER_REMOVE_JOB,
                                                             (qpysys.user,
                                                              job.ID,
                                                              len(self.jobs.queue))),
                                                            qpycomm.multiuser_address,
                                                            qpycomm.multiuser_port,
                                                            qpycomm.multiuser_key)
                    except:
                        self.multiuser_alive.clear()
                        self.config.logger.error('Exception in CHECK_RUN message transfer',
                                                 exc_info=True)
                        self.config.messages.add('CHECK_RUN: Exception in message transfer: '
                                                 + repr(sys.exc_info()[0]))
                    else:
                        self.multiuser_alive.set()
                        self.config.messages.add('CHECK_RUN: Multiuser message: ' + str(msg_back))
                else:
                    i += 1
            if (jobs_modification):
                self.jobs.write_all_jobs()
                jobs_modification = False
            sleep (self.config.sleep_time_check_run)


class JobsKiller( threading.Thread):
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

    def __init__( self, jobs, multiuser_alive, config):
        """Initiate the class.

        Arguments:
        jobs (Job_Collection)      All jobs being handled by qpy-master
        multiuser_alive (Event)    It is set if the multiuser is alive
        config (Configurations)    qpy configurations
        """
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive
        self.config = config

        threading.Thread.__init__( self)
        self.to_kill = Queue()

    def run(self):
        """Kill jobs, see class documentation."""
        while True:
            job = self.to_kill.get()

            if (isinstance(job, str)):
                if (job == 'kill'):
                    break

            command = 'python ' + qpysys.source_dir + '/qpy_job_killer.py ' + str(job.ID)
            try:
                if (job.status != qpyconst.JOB_ST_RUNNING):
                    raise
                (std_out, std_err) = qpycomm.node_exec(job.node,
                                                       command,
                                                       pKey_file=self.config.ssh_p_key_file)
            except:
                pass
            else:
                job.status = qpyconst.JOB_ST_KILLED
                job.end_time = datetime.today()
                job.run_duration()
                self.jobs.mv(job, self.jobs.running, self.jobs.killed)
                self.jobs.write_all_jobs()

                self.config.messages.add('Killing: ' + str(job.ID)
                                         + ' on node ' + job.node + '. stdout = '
                                         + repr(std_out)  + '. stderr = '
                                         + repr(std_err))

                try:
                    msg_back = qpycomm.message_transfer((qpyconst.MULTIUSER_REMOVE_JOB,
                                                         (qpysys.user,
                                                          job.ID,
                                                          len(self.jobs.queue))),
                                                        qpycomm.multiuser_address,
                                                        qpycomm.multiuser_port,
                                                        qpycomm.multiuser_key)
                except:
                    self.multiuser_alive.clear()
                    self.config.logger.error('Exception in JOBS_KILLER message transfer',
                                             exc_info=True)
                    self.config.messages.add('JOBS_KILLER: Exception in message transfer: '
                                             + repr(sys.exc_info()[0]))
                else:
                    self.multiuser_alive.set()
                    self.config.messages.add('JOBS_KILLER: Message from multiuser: '
                                             + str(msg_back))


class Submission(threading.Thread):
    """Control the job submission.
    
    Attributes:
    jobs (Job_Collection)            All jobs being handled by qpy-master
    muHandler (MULTIUSER_HANDLER)    The multiuser_handler
    config (Configurations)          qpy configurations
    finish (Event)                   Set this Event to terminate this Thread
    skip_jobs_submission (int)       Skip the submission by this amount of cicles
    submit_jobs (bool)               Jobs are submitted only if True
    
    Behaviour:
    This thread looks the jobs.queue and try to submit tje jobs, 
    asking for a node to qpy-multiuser and making the submission if
    a node is given.

    Each cycle is done at each config.sleep_time_sub_ctrl seconds.

    """

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
        n_time_multiuser = 0
        self.jobs.initialize_old_jobs(self)
        jobs_modification = False
        if not(self.muHandler.multiuser_alive.is_set()):
            self.skip_job_sub = 30
            
        while not self.finish.is_set():

            if ((not self.muHandler.multiuser_alive.is_set())
                and self.skip_job_sub == 0):
                self.muHandler.add_to_multiuser()
                if not(self.muHandler.multiuser_alive.is_set()):
                    self.skip_job_sub = 30

            if (self.submit_jobs and self.skip_job_sub == 0):

                if (len(self.jobs.Q) != 0):
                    next_jobID = self.jobs.Q[-1].ID
                    next_Ncores = self.jobs.Q[-1].n_cores
                    next_mem = self.jobs.Q[-1].mem
                    next_node_attr = self.jobs.Q[-1].node_attr
                    avail_node = None

                    try:
                        msg_back = qpycomm.message_transfer((qpyconst.MULTIUSER_REQ_CORE,
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
                        self.config.messages.add('SUB_CTRL: Exception in message transfer: '
                                                 + str(sys.exc_info()[0]) + '; '
                                                 + str(sys.exc_info()[1]))
                        self.config.logger.error('Exception in SUB_CTRL message transfer',
                                                 exc_info=True)
                        self.skip_job_sub = 30
                    else:
                        self.muHandler.multiuser_alive.set()
                        self.config.messages.add('SUB_CTRL: Message from multiuser: '
                                                 + str(msg_back))
                        if (msg_back[0] == 0):
                            avail_node = msg_back[1]
                        else:
                            self.skip_job_sub = 30
                        if (avail_node != None):
                            self.config.messages.add("SUB_CTRL: submitting job in "
                                                     + avail_node)
                            job = self.jobs.Q_pop()
                            job.node = avail_node
                            try:
                                job.run(self.config)
                            except:
                                # If it's not running, we have to tell qpy-multiuser back somehow...
                                self.config.messages.add("SUB_CTRL: exception when submitting job: "
                                                         + repr(sys.exc_info()[1]))
                                self.logger.error("Exception in SUB_CTRL when submitting job",
                                                  exc_info=True)
                                job.node = None
                                self.job.append(job, self.jobs.queue)
                                self.job.append(job, self.jobs.Q)
                            else:
                                self.jobs.mv(job, self.jobs.queue, self.jobs.running)
                                jobs_modification = True
                if (jobs_modification):
                    self.jobs.write_all_jobs()
                    jobs_modification = False
            else:
                if (self.skip_job_sub > 0):
                    self.skip_job_sub -= 1
                self.config.messages.add("SUB_CTRL: Skipping job submission.")
            sleep(self.config.sleep_time_sub_ctrl)
