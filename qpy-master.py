"""qpy-master - set the main driver for qpy

History:
    29 May 2015 - Pradipta and Yuri
"""
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import threading
from time import sleep, time
from Queue import Queue
import re
import subprocess
import sys
import os
import datetime
from shutil import copyfile
from optparse import OptionError
import traceback

import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_useful_cosmetics as qpyutil
import qpy_communication as qpycomm
from qpy_configurations import Configurations
from qpy_job import JobId, Job, Job_Collection, ParseError, HelpException


if (not(os.path.isdir(qpysys.qpy_dir))):
    os.makedirs(qpysys.qpy_dir)
os.chmod(qpysys.qpy_dir, 0700)

if (not(os.path.isdir(qpysys.scripts_dir))):
    os.makedirs(qpysys.scripts_dir)

if (not(os.path.isdir(qpysys.notes_dir))):
    os.makedirs(qpysys.notes_dir)

if (os.path.isfile(qpysys.master_conn_file+'_port')):
    sys.exit('A connection file was found. Is there a qpy-master instance running?')

if (os.path.isfile(qpysys.master_conn_file+'_conn_key')):
    sys.exit('A connection file was found. Is there a qpy-master instance running?')

(multiuser_address,
 multiuser_port,
 multiuser_key) = qpycomm.read_conn_files(qpysys.multiuser_conn_file)
if (multiuser_port == None or multiuser_key == None):
    sys.exit("Information for multiuser connection could not be obtained. Contact your administrator.")


def analise_multiuser_status(info, check_n, check_u):
    """Transform the multiuser status message to a more readable version."""
    lines = info.split('\n')
    do_users = False
    do_nodes = False
    users_info = []
    nodes_info = []
    total_cores = '0'
    used_cores = '0'
    for l in lines:
        if (l == 'Users:'):
            do_users = True
            do_nodes = False
        elif (l == 'Nodes:'):
            do_users = False
            do_nodes = True
        elif (l[0:6] == 'Cores:'):
            re_res = re.match( 'Cores:\s+(\d+)\+(\d+)/(\d+)', l)
            if (re_res):
                used_cores = str( int( re_res.group(1)) + int( re_res.group(2)))
                total_cores = re_res.group(3)
        elif (do_users):
            re_res = re.match( '\s*([a-z,A-Z]+):\s+(\d+)\+(\d+)/(\d+)\+(\d+)', l)
            if (re_res):
                a = ( re_res.group(1), re_res.group(2), re_res.group(3), re_res.group(4), re_res.group(5))
            else:
                a = ( 'error', '-1', '-1', '-1', '-1')
            users_info.append( a)
        elif (do_nodes):
            re_res = re.match( '\s*([a-z,A-Z,0-9]+):\s+(\d+)/(\d+)-(\d+)', l)
            if (re_res):
                a = ( re_res.group(1), re_res.group(2), re_res.group(3), re_res.group(4))
            else:
                a = ( 'error', '-1', '-1', '-1', '-1')
            nodes_info.append( a)

    msgU = ''
    if (check_u):
        max_len = 0
        format_spec = '{0:17s} {1:15s} {2:4s}\n'
        for u in users_info:
            msgU += format_spec.format( u[0], u[1], u[2])
        if (msgU):
            msgU = 'user' + ' '*10 + 'using cores' + ' '*5 + 'queue size\n' + '-'*50 + '\n' + msgU + '='*50 + '\n'

    msgN = ''
    if (check_n):
        max_len = 0
        format_spec = '{0:17s} {1:15s} {2:4s}\n'
        for n in nodes_info:
            msgN += format_spec.format( n[0], str(int(n[1])+int(n[3])), n[2])
        if (msgN):
            msgN = 'node' + ' '*10 + 'used cores' + ' '*5 + 'total cores\n' + '-'*50 + '\n' + msgN + '='*50 + '\n'

    msg = ''
    if (check_u):
        msg += msgU
    if (check_n):
        msg += msgN
    if (check_u or check_n):
        msg += 'There are ' + used_cores + ' out of a total of ' + total_cores + ' cores being used.\n'

    return msg


class CHECK_RUN(threading.Thread):
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
            with jobs.lock:
                jobs_to_check = list(self.jobs.running)
            for job in jobs_to_check:
                try:
                    is_running = job.is_running(self.config)
                except:
                    self.config.logger.error('Exception in CHECK_RUN.is_running', exc_info=True)
                    self.config.messages.add('CHECK_RUN: Exception in is_running: ' + repr(sys.exc_info()[0]))
                    is_running = True
                if (not is_running and job.status == qpyconst.JOB_ST_RUNNING):
                    job.status = qpyconst.JOB_ST_DONE
                    job.end_time = datetime.datetime.today()
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
                                                            multiuser_address,
                                                            multiuser_port,
                                                            multiuser_key)
                    except:
                        self.multiuser_alive.clear()
                        self.config.logger.error('Exception in CHECK_RUN message transfer', exc_info=True)
                        self.config.messages.add('CHECK_RUN: Exception in message transfer: ' + repr(sys.exc_info()[0]))
                    else:
                        self.multiuser_alive.set()
                        self.config.messages.add('CHECK_RUN: Multiuser message: ' + str(msg_back))
                else:
                    i += 1

            if (jobs_modification):
                self.jobs.write_all_jobs()
                jobs_modification = False

            sleep (config.sleep_time_check_run)


class JOBS_KILLER( threading.Thread):
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

            command = qpysys.source_dir + '/qpy --jobkill ' + str(job.ID)
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
                job.end_time = datetime.datetime.today()
                job.run_duration()
                self.jobs.mv(job, self.jobs.running, self.jobs.killed)
                self.jobs.write_all_jobs()

                self.config.messages.add( 'Killing: ' + str(job.ID) + ' on node ' + job.node + '. stdout = ' + repr(std_out)  + '. stderr = ' + repr(std_err))

                try:
                    msg_back = qpycomm.message_transfer((qpyconst.MULTIUSER_REMOVE_JOB,
                                                         (qpysys.user,
                                                          job.ID,
                                                          len(self.jobs.queue))),
                                                        multiuser_address,
                                                        multiuser_port,
                                                        multiuser_key)
                except:
                    self.multiuser_alive.clear()
                    self.config.logger.error('Exception in JOBS_KILLER message transfer', exc_info=True)
                    self.config.messages.add('JOBS_KILLER: Exception in message transfer: ' + repr(sys.exc_info()[0]))
                else:
                    self.multiuser_alive.set()
                    self.config.messages.add('JOBS_KILLER: Message from multiuser: ' + str(msg_back))



class SUB_CTRL(threading.Thread):
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

            if (not(self.muHandler.multiuser_alive.is_set()) and self.skip_job_sub == 0):
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
                                                            multiuser_address,
                                                            multiuser_port,
                                                            multiuser_key)
                    except:
                        self.muHandler.multiuser_alive.clear()
                        self.config.messages.add('SUB_CTRL: Exception in message transfer: ' + str(sys.exc_info()[0]) + '; ' + str(sys.exc_info()[1]))
                        self.config.logger.error('Exception in SUB_CTRL message transfer', exc_info=True)
                        self.skip_job_sub = 30
                    else:
                        self.muHandler.multiuser_alive.set()
                        self.config.messages.add('SUB_CTRL: Message from multiuser: ' + str(msg_back))
                        if (msg_back[0] == 0):
                            avail_node = msg_back[1]
                        else:
                            self.skip_job_sub = 30
                        if (avail_node != None):
                            self.config.messages.add("SUB_CTRL: submitting job in " + avail_node)
                            job = self.jobs.Q_pop()
                            job.node = avail_node
                            try:
                                job.run(self.config)
                            except:
                                # If it's not running, we have to tell qpy-multiuser back somehow...
                                self.config.messages.add("SUB_CTRL: exception when submitting job: " + repr(sys.exc_info()[1]))
                                self.logger.error("Exception in SUB_CTRL when submitting job", exc_info=True)
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

            sleep(config.sleep_time_sub_ctrl)



class MULTIUSER_HANDLER( threading.Thread):
    """Handle the messages sent from qpy-multiuser.
    
    Attributes:
    jobs (Job_Collection)        All jobs being handled by qpy-master
    multiuser_alive (Event)      It is set if the multiuser is alive
    config (Configurations)      qpy configurations
    Listener_master (Listener)   To receive messages from qpy-multiuser
    port                         The port of the above connection
                                   (we share it with multiuser)
    conn_key                     The key of the above connection
                                   (we share it with multiuser)
    
    Behaviour:
    This Thread waits messages from the qpy-multiuser.
    When one comes, it analyses it, does whatever it is needed
    and send a message back.

    The message must be a tuple (job_type, arguments)
    where job_type is:

    qpyconst.FROM_MULTI_CUR_JOBS
    qpyconst.FROM_MULTI_FINISH

    the arguments are option dependent.
    
    """

    def __init__(self, jobs, multiuser_alive, config):
        """Initiates the class
        
        Behaviour:
        Opens a new connection and the corresponding Listener.
        It also tells qpy-multiuser about the present user,
        sharing the port and the key of the above connection.
        
        Arguments:
        jobs (Job_Collection)        All jobs being handled by qpy-master
        multiuser_alive (Event)      It is set if the multiuser is alive
        config (Configurations)      qpy configurations
        """
        threading.Thread.__init__(self)
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive
        self.config = config
        self.address = qpycomm.read_address_file(qpysys.master_conn_file)

        (self.Listener_master,
         self.port,
         self.conn_key) = qpycomm.establish_Listener_connection(self.address,
                                                                qpyconst.PORT_MIN_MASTER,
                                                                qpyconst.PORT_MAX_MASTER)
        self.add_to_multiuser()

    def run( self):
        """Wait messages from qpy-multiuser, see class documentation."""
        while True:
            client_master = self.Listener_master.accept()
            (msg_type, arguments) = client_master.recv()
            self.config.messages.add( 'MULTIUSER_HANDLER: Received: ' + str(msg_type) + ' -> ' + str(arguments))
                
            # Get current list of jobs
            # arguments = ()
            if (msg_type == qpyconst.FROM_MULTI_CUR_JOBS):
                multiuser_cur_jobs = self.jobs.multiuser_cur_jobs()
                client_master.send(multiuser_cur_jobs)
                self.multiuser_alive.set()

            # Finishi this thread
            # arguments = ()
            elif (msg_type == qpyconst.FROM_MULTI_FINISH):
                client_master.send( 'Finishing MULTIUSER_HANDLER.')
                self.Listener_master.close()
                break

            else:
                client_master.send( 'Unknown option: ' + str( job_type) + '\n')


    def add_to_multiuser(self):
        """Contact qpy-multiuser to tell connection details and jobs."""
        multiuser_cur_jobs = self.jobs.multiuser_cur_jobs()
        try:
            msg_back = qpycomm.message_transfer((qpyconst.MULTIUSER_USER,
                                                 (qpysys.user,
                                                  self.address,
                                                  self.port,
                                                  self.conn_key,
                                                  multiuser_cur_jobs)),
                                                multiuser_address,
                                                multiuser_port,
                                                multiuser_key)
        except:
            self.multiuser_alive.clear()
            self.config.messages.add('MULTIUSER_HANDLER: Exception in message transfer: ' + repr(sys.exc_info()[0]) + ' ' + repr(sys.exc_info()[1]))
            self.config.logger.error('Exception in MULTIUSER_HANDLER message transfer', exc_info=True)
        else:
            if (msg_back[0] ==  2):
                self.multiuser_alive.clear()
            else:
                self.multiuser_alive.set()

            self.config.messages.add('MULTIUSER_HANDLER: Message from multiuser: ' + str(msg_back))


def handle_qpy(jobs, sub_ctrl, jobs_killer, config):
    """Handle the user messages sent from qpy.

    Arguments:
    sub_ctrl (SUB_CTRL)         The submission control
    jobs_killer (JOBS_KILLER)   The jobs killer
    config (Configurations)     qpy configurations

    Behaviour:
    It opens a new connection, share the port and key with qpy
    by te corresponding files and waits for messages from qpy.
    When a message is received, it analyzes it, does whatever
    is needed and returns a message back.

    The message from qpy must be a tuple (job_type, arguments)
    where job_type is
       qpyconst.JOBTYPE_SUB     - submit a job                           (sub)
       qpyconst.JOBTYPE_CHECK   - check the jobs                         (check)
       qpyconst.JOBTYPE_KILL    - kill a job                             (kill)
       qpyconst.JOBTYPE_FINISH  - kill the master                        (finish)
       qpyconst.JOBTYPE_CONFIG  - show config                            (config)
       qpyconst.JOBTYPE_CLEAN   - clean finished jobs                    (clean)
    
    """
    address = qpycomm.read_address_file(qpysys.master_conn_file)
    (Listener_master,
     port,
     conn_key) = qpycomm.establish_Listener_connection(address,
                                                       qpyconst.PORT_MIN_MASTER,
                                                       qpyconst.PORT_MAX_MASTER)
    qpycomm.write_conn_files(qpysys.master_conn_file,
                             address, port, conn_key)
    job_id = JobId(qpysys.jobID_file)

    while True:
        client_master = Listener_master.accept()
        (job_type, arguments) = client_master.recv()
        config.messages.add( "handle_qpy: Received: " + str(job_type) + " -> " + str(arguments))
            
        # Send a job
        # arguments = the job info (see JOB.info)
        if (job_type == qpyconst.JOBTYPE_SUB):
            new_job = Job(int(job_id), arguments, config)
            try:
                new_job.parse_options()
                if config.default_attr and not new_job.node_attr:
                    new_job.node_attr = config.default_attr
                if config.or_attr:
                    if new_job.node_attr:
                        new_job.node_attr = ['('] + config.or_attr + [')', 'or', '(']  + new_job.node_attr + [')']
                    else:
                        new_job.node_attr = config.or_attr
                if config.and_attr:
                    if new_job.node_attr:
                        new_job.node_attr = ['('] + config.and_attr + [')', 'and', '(']  + new_job.node_attr + [')']
                    else:
                        new_job.node_attr = config.and_attr
                if (new_job.use_script_copy):
                    first_arg = new_job.info[0].split()[0]
                    script_name = new_job._expand_script_name(first_arg)
                    if (script_name != None):
                        copied_script_name = qpysys.scripts_dir + 'job_script.' + str( new_job.ID)
                        copyfile( script_name, copied_script_name)
                        new_job.cp_script_to_replace = ( first_arg, copied_script_name)
            except HelpException,ex :
                client_master.send( ex.message)
            except ParseError, ex:
                client_master.send( 'Job  rejected.\n'+ex.message+'\n')
            else:
                jobs.append(new_job, jobs.all)
                jobs.append(new_job, jobs.queue)
                jobs.Q_appendleft(new_job)
                client_master.send('Job ' + str(job_id) + ' received.\n')
                job_id += 1
                jobs.write_all_jobs()
        
        # Check jobs
        # arguments: a dictionary, indicating patterns (see JOB.asked)
        elif (job_type == qpyconst.JOBTYPE_CHECK):
            if (config.sub_paused):
                msg_pause = 'Job submission is paused.\n'
            else:
                msg_pause = ''

            client_master.send(jobs.check(arguments, config) + msg_pause)

        # Kill a job
        # arguments = a list of jobIDs and status (all, queue, running)
        elif (job_type == qpyconst.JOBTYPE_KILL):

            kill_q = (( 'all' in arguments) or ('queue' in arguments))
            kill_r = (( 'all' in arguments) or ('running' in arguments))

            for st in ['all', 'queue', 'running']:
                while (st in arguments):
                    arguments.remove( st)

            n_kill_q = 0
            to_remove = []
            for job in jobs.queue:
                if (job.ID in arguments or kill_q):
                    to_remove.append(job)
            for job in to_remove:
                job.status = qpyconst.JOB_ST_UNDONE
                jobs.remove(job, jobs.Q)
                jobs.remove(job, jobs.queue)
                jobs.append(job, jobs.undone)
                n_kill_q += 1

            n_kill_r = 0
            to_remove = []
            with jobs.lock:
                for job in jobs.running:
                    if (job.ID in arguments or kill_r):
                        to_remove.append(job)

            for job in to_remove:
                jobs_killer.to_kill.put(job)
                n_kill_r += 1

            if (n_kill_q + n_kill_r):
                sub_ctrl.skip_job_sub = 0

            msg = ''
            if (n_kill_q):
                plural = qpyutil.get_plural( ('job', 'jobs'), n_kill_q)
                msg += plural[1] + ' ' + plural[0] + ' removed from the queue.\n'
            if (n_kill_r):
                plural = qpyutil.get_plural( ('job', 'jobs'), n_kill_r)
                msg += plural[1] + ' ' + plural[0] + ' will be killed.\n'

            if (not( msg)):
                msg = 'Nothing to do: required jobs not found.\n'
            else:
                jobs.write_all_jobs()

            client_master.send(msg)


        # Finish the execution of all threads
        # argumets: no arguments
        if (job_type == qpyconst.JOBTYPE_FINISH):
            client_master.send( 'Stopping qpy-master driver.\n')
            Listener_master.close()
            break


        # Show status
        # No arguments (yet)
        elif (job_type == qpyconst.JOBTYPE_STATUS):
            try:
                msg_back = qpycomm.message_transfer((qpyconst.MULTIUSER_STATUS, ()),
                                                    multiuser_address,
                                                    multiuser_port,
                                                    multiuser_key)
            except:
                msg = 'qpy-multiuser seems not to be running. Contact the qpy-team.\n'
                multiuser_alive.clear()
            else:
                msg = msg_back[1]
                multiuser_alive.set()

            client_master.send( msg)


        # Control queue
        # arguments: a list: [<type>, <arguments>].
        elif (job_type == qpyconst.JOBTYPE_CTRLQUEUE):
            ctrl_type = arguments[0]
            if (ctrl_type == 'pause' or ctrl_type == 'continue'):
                if (ctrl_type == 'pause'):
                    config.sub_paused = True
                    msg = 'Job submission paused.\n'
                else:
                    config.sub_paused = False
                    msg = 'Job submission continued.\n'
                config.write_on_file()
                sub_ctrl.submit_jobs = not(config.sub_paused)

            elif (ctrl_type == 'jump'):
                if (not(config.sub_paused)):
                    msg = 'Pause the queue before trying to control it.\n'
                else:
                    msg = jobs.jump_Q(arguments[1], arguments[2])
            else:
                msg = 'Unknown ctrlQueue type: ' + ctrl_type + '.\n'

            client_master.send( msg)

        # Show current configuration
        # arguments: optionally, a pair to change the configuration: (<key>, <value>)
        elif (job_type == qpyconst.JOBTYPE_CONFIG):
            if (arguments):
                (status, msg) = config.set_key(arguments[0], arguments[1])
                msg = msg + '\n'
                config.write_on_file()
            else:
                msg = str(config)

            client_master.send(msg)

        # Clean finished jobs
        # arguments = a list of jobIDs and status (all, done, killed, undone)
        elif (job_type == qpyconst.JOBTYPE_CLEAN):
            n_jobs = 0
            for i in arguments:
                arg_is_id = isinstance( i, int)
                arg_is_dir = isinstance( i, str) and os.path.isdir(i)
                ij = 0
                with jobs.lock:
                    while (ij < len( jobs.all)):
                        job = jobs.all[ij]
                        remove = False
                        if (job.status > 1):
                            remove = arg_is_id and i == job.ID
                            remove = remove or (arg_is_dir and i == job.info[1])
                            remove = remove or not( arg_is_id) and i == 'all'
                            remove = remove or not( arg_is_id) and i == qpyconst.JOB_STATUS[job.status]
                            if (remove):
                                if (job.status == qpyconst.JOB_ST_DONE):
                                    jobs.remove(job, jobs.done)
                                elif (job.status == qpyconst.JOB_ST_KILLED):
                                    jobs.remove(job, jobs.killed)
                                elif (job.status == qpyconst.JOB_ST_UNDONE):
                                    jobs.remove(job, jobs.undone)
                                jobs.remove(job, jobs.all)
                                n_jobs += 1
                                if (os.path.isfile( qpysys.notes_dir + 'notes.' + str(job.ID))):
                                    os.remove(      qpysys.notes_dir + 'notes.' + str(job.ID))
                        if (not( remove)):
                            ij += 1

            if (n_jobs):
                jobs.write_all_jobs()
                plural = qpyutil.get_plural(('job', 'jobs'), n_jobs)
                msg = plural[1] + ' finished ' + plural[0] + ' removed.\n'
            else:
                msg = 'Nothing to do: required jobs not found.\n'

            client_master.send( msg)

        # Add and read notes
        # arguments = (jobID[, note])
        elif (job_type == qpyconst.JOBTYPE_NOTE):
            if (len( arguments) == 0):
                all_notes = os.listdir( qpysys.notes_dir)
                msg = ''
                for n in all_notes:
                    if (n[0:6] == 'notes.'):
                        msg += n[6:] + ' '
                if (msg):
                    msg = 'You have notes for the following jobs:\n' + msg + '\n'
                else:
                    msg = 'You have no notes.\n'
            else:
                notes_file = qpysys.notes_dir + 'notes.' + str(arguments[0])
                if (os.path.isfile( notes_file)):
                    f = open( notes_file, 'r')
                    notes = f.read()
                    f.close()
                else:
                    notes = ''
                if (len( arguments) == 1):
                    if (not( notes)):
                        msg = 'No notes for ' + arguments[0] + '\n'
                    else:
                        msg = notes + '\n'
                else:
                    notes += '----- Note added at ' + str(datetime.datetime.today()) + ':\n' + arguments[1] + '\n\n'
                    f = open(notes_file, 'w')
                    f.write(notes)
                    f.close()
                    msg = 'Note stored.\n'

            client_master.send( msg)

        else:
            client_master.send( 'Unknown option: ' + str( job_type) + '\n')


#------------------------------
config = Configurations(qpysys.config_file)
jobs = Job_Collection(config)

multiuser_alive = threading.Event()
multiuser_alive.clear()

check_run = CHECK_RUN(jobs, multiuser_alive, config)
check_run.start()

jobs_killer = JOBS_KILLER(jobs, multiuser_alive, config)
jobs_killer.start()

multiuser_handler = MULTIUSER_HANDLER(jobs, multiuser_alive, config)
multiuser_handler.start()

sub_ctrl = SUB_CTRL(jobs, multiuser_handler, config)
sub_ctrl.start()

try:
    handle_qpy(jobs, sub_ctrl, jobs_killer, config)
except:
    config.logger.error('Exception at handle_qpy', exc_info=True)

# Finishing qpy-master
config.logger.info("Finishing qpy-master")
sub_ctrl.finish.set()
check_run.finish.set()
jobs_killer.to_kill.put('kill')
qpycomm.message_transfer((qpyconst.FROM_MULTI_FINISH, ()),
                         multiuser_handler.address,
                         multiuser_handler.port,
                         multiuser_handler.conn_key)
os.remove(qpysys.master_conn_file + '_port')
os.remove(qpysys.master_conn_file + '_conn_key')
