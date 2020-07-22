""" qpy - Threads and funtions for interaction with qpy-master

"""
import os
import sys
import traceback
import threading
from shutil import copyfile
import datetime

import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_useful_cosmetics as qpyutil
import qpy_communication as qpycomm
from qpy_parser import JobOptParser
from qpy_job import JobId, Job
from qpy_exceptions import qpyParseError, qpyKeyError, qpyValueError


class MultiuserHandler(threading.Thread):
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
    __slots__ = (
        'jobs',
        'multiuser_alive',
        'config',
        'address')

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
         self.conn_key) = qpycomm.establish_Listener_connection(
             self.address,
             qpyconst.PORT_MIN_MASTER,
             qpyconst.PORT_MAX_MASTER
         )
        self.add_to_multiuser()

    def run(self):
        """Wait messages from the client, see class documentation."""
        while True:
            client_master = self.Listener_master.accept()
            (msg_type, arguments) = client_master.recv()
            self.config.messages.add('MultiuserHandler: Received: '
                                     + str(msg_type) + ' -> ' + str(arguments))
            if msg_type == qpyconst.FROM_MULTI_CUR_JOBS:
                multiuser_cur_jobs = self.jobs.multiuser_cur_jobs()
                client_master.send(multiuser_cur_jobs)
                self.multiuser_alive.set()
            elif msg_type == qpyconst.FROM_MULTI_FINISH:
                client_master.send('Finishing MultiuserHandler.')
                self.Listener_master.close()
                break
            else:
                client_master.send('Unknown option: ' + str(msg_type) + '\n')

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
                                                qpycomm.multiuser_address,
                                                qpycomm.multiuser_port,
                                                qpycomm.multiuser_key)
        except:
            self.multiuser_alive.clear()
            self.config.messages.add(
                'MULTIUSER_HANDLER: Exception in message transfer: '
                + repr(sys.exc_info()[0]) + ' '
                + repr(sys.exc_info()[1]))
            self.config.logger.error(
                'Exception in MULTIUSER_HANDLER message transfer',
                exc_info=True)
        else:
            if msg_back[0] == 2:
                self.multiuser_alive.clear()
            else:
                self.multiuser_alive.set()
            self.config.logger.info(
                'MULTIUSER_HANDLER: Message from multiuser: '
                + str(msg_back))


def handle_qpy(jobs,
               sub_ctrl,
               jobs_killer,
               config,
               multiuser_alive):
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
       qpyconst.JOBTYPE_SUB     - submit a job          (sub)
       qpyconst.JOBTYPE_CHECK   - check the jobs        (check)
       qpyconst.JOBTYPE_KILL    - kill a job            (kill)
       qpyconst.JOBTYPE_FINISH  - kill the master       (finish)
       qpyconst.JOBTYPE_CONFIG  - show config           (config)
       qpyconst.JOBTYPE_CLEAN   - clean finished jobs   (clean)
    """
    address = qpycomm.read_address_file(qpysys.master_conn_file)
    (Listener_master,
     port,
     conn_key) = qpycomm.establish_Listener_connection(
         address,
         qpyconst.PORT_MIN_MASTER,
         qpyconst.PORT_MAX_MASTER
     )
    qpycomm.write_conn_files(qpysys.master_conn_file,
                             address, port, conn_key)
    job_id = JobId(qpysys.jobID_file)
    job_options_parser = JobOptParser.set_parser()
    while True:
        client_master = Listener_master.accept()
        (job_type, arguments) = client_master.recv()
        config.messages.add("handle_qpy: Received: "
                            + str(job_type) + " -> " + str(arguments))
        # Send a job
        # arguments = the job info (see JOB.info)
        if job_type == qpyconst.JOBTYPE_SUB:
            new_job = Job(int(job_id), arguments, config, job_options_parser)
            try:
                new_job.parse_options()
            except qpyParseError as e:
                client_master.send('qpy: Job rejected due to its options:\n'
                                   + e.message + '\n')
            except:
                client_master.send(
                    'qpy: Job rejected:\n'
                    + 'Unexpected exception after parsing options:\n'
                    + traceback.format_exc() + '\n'
                    + 'Please, contact the qpy team.\n')
            else:
                if config.default_attr and not new_job.node_attr:
                    new_job.node_attr = config.default_attr
                if config.or_attr:
                    if new_job.node_attr:
                        new_job.node_attr = (['('] + config.or_attr +
                                             [')', 'or', '(']
                                             + new_job.node_attr + [')'])
                    else:
                        new_job.node_attr = config.or_attr
                if config.and_attr:
                    if new_job.node_attr:
                        new_job.node_attr = (['('] + config.and_attr +
                                             [')', 'and', '('] +
                                             new_job.node_attr + [')'])
                    else:
                        new_job.node_attr = config.and_attr
                if (new_job.use_script_copy):
                    first_arg = new_job.info[0].split()[0]
                    script_name = new_job._expand_script_name(first_arg)
                    if (script_name is not None):
                        copied_script_name = (
                            qpysys.scripts_dir + 'job_script.'
                            + str(new_job.ID))
                        copyfile(script_name, copied_script_name)
                        new_job.cp_script_to_replace = (first_arg,
                                                        copied_script_name)
                jobs.append(new_job, jobs.all)
                jobs.append(new_job, jobs.queue)
                jobs.Q_appendleft(new_job)
                client_master.send('Job ' + str(job_id) + ' received.\n')
                job_id += 1
                jobs.write_all_jobs()
        
        # Check jobs
        # arguments: a dictionary, indicating patterns (see JOB.asked)
        elif job_type == qpyconst.JOBTYPE_CHECK:
            if (config.sub_paused):
                msg_pause = 'Job submission is paused.\n'
            else:
                msg_pause = ''
            client_master.send(jobs.check(arguments, config) + msg_pause)

        # Kill a job
        # arguments = a list of jobIDs and status (all, queue, running)
        elif job_type == qpyconst.JOBTYPE_KILL:
            orig_sub_jobs = sub_ctrl.submit_jobs
            sub_ctrl.submit_jobs = False
            kill_q = ('all' in arguments) or ('queue' in arguments)
            kill_r = ('all' in arguments) or ('running' in arguments)
            for st in ['all', 'queue', 'running']:
                while (st in arguments):
                    arguments.remove(st)
            n_kill_q = 0
            to_remove = []
            for job in jobs.queue:
                if job.ID in arguments or kill_q:
                    to_remove.append(job)
            for job in to_remove:
                job.status = qpyconst.JOB_ST_UNDONE
                jobs.remove(job, jobs.Q)
                jobs.remove(job, jobs.queue)
                jobs.append(job, jobs.undone)
                n_kill_q += 1
            sub_ctrl.submit_jobs = orig_sub_jobs
            n_kill_r = 0
            to_remove = []
            with jobs.lock:
                for job in jobs.running:
                    if job.ID in arguments or kill_r:
                        to_remove.append(job)
            for job in to_remove:
                jobs_killer.to_kill.put(job)
                n_kill_r += 1
            if n_kill_q + n_kill_r:
                sub_ctrl.skip_job_sub = 0
            msg = ''
            if n_kill_q:
                plural = qpyutil.get_plural(('job', 'jobs'), n_kill_q)
                msg += (plural[1] + ' '
                        + plural[0] + ' removed from the queue.\n')
            if n_kill_r:
                plural = qpyutil.get_plural(('job', 'jobs'), n_kill_r)
                msg += plural[1] + ' ' + plural[0] + ' will be killed.\n'
            if not msg:
                msg = 'qpy: Nothing to do: required jobs not found.\n'
            else:
                jobs.write_all_jobs()
            client_master.send(msg)

        # Finish the execution
        # argumets: no arguments
        if job_type == qpyconst.JOBTYPE_FINISH:
            client_master.send('Stopping qpy-master driver.\n')
            Listener_master.close()
            break

        # Show status
        # No arguments (yet)
        elif job_type == qpyconst.JOBTYPE_STATUS:
            try:
                msg_back = qpycomm.message_transfer(
                    (qpyconst.MULTIUSER_STATUS, ()),
                    qpycomm.multiuser_address,
                    qpycomm.multiuser_port,
                    qpycomm.multiuser_key
                )
            except:
                msg = ('qpy: qpy-multiuser seems not to be running.'
                       + ' Contact the qpy-team.\n')
                multiuser_alive.clear()
            else:
                msg = msg_back[1]
                multiuser_alive.set()
            client_master.send(msg)

        # Control queue
        # arguments: a list: [<type>, <arguments>].
        elif job_type == qpyconst.JOBTYPE_CTRLQUEUE:
            ctrl_type = arguments[0]
            if ctrl_type == 'pause' or ctrl_type == 'continue':
                if ctrl_type == 'pause':
                    config.sub_paused = True
                    msg = 'Job submission paused.\n'
                else:
                    config.sub_paused = False
                    msg = 'Job submission continued.\n'
                config.write_on_file()
                sub_ctrl.submit_jobs = not config.sub_paused

            elif ctrl_type == 'jump':
                if not config.sub_paused:
                    msg = 'Pause the queue before trying to control it.\n'
                else:
                    msg = jobs.jump_Q(arguments[1], arguments[2])
            else:
                msg = 'qpy: Unknown ctrlQueue type: ' + ctrl_type + '.\n'
            client_master.send(msg)

        # Show current configuration
        # arguments:
        # optionally, a pair to change the configuration: (<key>, <value>)
        elif job_type == qpyconst.JOBTYPE_CONFIG:
            if arguments:
                try:
                    msg = config.set_key(arguments[0], arguments[1])
                except (qpyKeyError, qpyValueError) as e:
                    msg = 'qpy: ' + str(e)
                msg = msg + '\n'
                config.write_on_file()
            else:
                msg = str(config)
            client_master.send(msg)

        # Clean finished jobs
        # arguments = a list of jobIDs and status (all, done, killed, undone)
        elif job_type == qpyconst.JOBTYPE_CLEAN:
            n_jobs = 0
            for i in arguments:
                arg_is_id = isinstance(i, int)
                arg_is_dir = isinstance(i, str) and os.path.isdir(i)
                ij = 0
                with jobs.lock:
                    while ij < len(jobs.all):
                        job = jobs.all[ij]
                        remove = False
                        if job.status > 1:
                            remove = arg_is_id and i == job.ID
                            remove = remove or (arg_is_dir
                                                and i == job.info[1])
                            remove = remove or (not arg_is_id) and i == 'all'
                            remove = (remove or (not arg_is_id)
                                      and i == qpyconst.JOB_STATUS[job.status])
                            if remove:
                                if job.status == qpyconst.JOB_ST_DONE:
                                    jobs.remove(job, jobs.done)
                                elif job.status == qpyconst.JOB_ST_KILLED:
                                    jobs.remove(job, jobs.killed)
                                elif job.status == qpyconst.JOB_ST_UNDONE:
                                    jobs.remove(job, jobs.undone)
                                jobs.remove(job, jobs.all)
                                n_jobs += 1
                                if os.path.isfile(qpysys.notes_dir + 'notes.'
                                                  + str(job.ID)):
                                    os.remove(qpysys.notes_dir + 'notes.'
                                              + str(job.ID))
                        if not remove:
                            ij += 1
            if n_jobs:
                jobs.write_all_jobs()
                plural = qpyutil.get_plural(('job', 'jobs'), n_jobs)
                msg = plural[1] + ' finished ' + plural[0] + ' removed.\n'
            else:
                msg = 'qpy: Nothing to do: required jobs not found.\n'
            client_master.send(msg)

        # Add and read notes
        # arguments = (jobID[, note])
        elif job_type == qpyconst.JOBTYPE_NOTE:
            if len(arguments) == 0:
                all_notes = os.listdir(qpysys.notes_dir)
                msg = ''
                for n in all_notes:
                    if n[0:6] == 'notes.':
                        msg += n[6:] + ' '
                if msg:
                    msg = ('You have notes for the following jobs:\n'
                           + msg + '\n')
                else:
                    msg = 'You have no notes.\n'
            else:
                notes_file = qpysys.notes_dir + 'notes.' + str(arguments[0])
                if (os.path.isfile(notes_file)):
                    f = open(notes_file, 'r')
                    notes = f.read()
                    f.close()
                else:
                    notes = ''
                if len(arguments) == 1:
                    if not notes:
                        msg = 'No notes for ' + arguments[0] + '\n'
                    else:
                        msg = notes + '\n'
                else:
                    notes += ('----- Note added at '
                              + str(datetime.datetime.today()) + ':\n'
                              + arguments[1] + '\n\n')
                    f = open(notes_file, 'w')
                    f.write(notes)
                    f.close()
                    msg = 'Note stored.\n'
            client_master.send(msg)

        else:
            client_master.send('qpy: Unknown option: ' + str(job_type) + '\n')
