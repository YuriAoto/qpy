""" qpy - A job (a code being or to be executed in a node)

"""
from collections import deque
from datetime import datetime
import threading
import glob
import re
import os
import sys

import termcolor.termcolor as termcolour

import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_communication as qpycomm
import qpy_nodes_management as qpynodes
from qpy_parser import ParseError


class JobId(object):
    """The job ID.
    
    Attributes:
    file (str)       The file where the job ID is stored
    current (int)    The ID for the next job
    
    Behaviour:
    This class initiate a job Id for the jobs and increment
    them. It automatically writes the next ID to be used on a file
    in case of crash or restart.
    """
    __slots__ = (
        'file',
        'current')

    def __init__(self, job_id_file):
        """Initialise the class.
        
        Arguments:
        job_id_file (str)   The file name
        
        Behaviour:
        It reads self.current from the file job_id_file or
        from 1, if an exception is raised when reading the
        file.
        """
        self.file = job_id_file
        self.current = 1
        try:
            self.from_file()
        except:
            self.current = 1
            self.to_file()

    def from_file(self):
        """Read the current ID from the file."""
        with open(self.file, 'r') as f:
            self.current = int(f.read())

    def to_file(self):
        """Writes the current ID in the file."""
        f = open(self.file, 'w')
        f.write(str(self.current))
        f.close()
        
    def __iadd__(self, other):
        """Increments the ID and writes it in the file."""
        self.current += 1
        self.to_file()
        return self

    def __int__(self):
        """The current ID."""
        return self.current

    def __str__(self):
        """The current ID as a string."""
        return str(self.current)


class MultiuserJob(object):
    """Class to represent a running job
    
    Attributes:
    user (str)      The user that runs this job
    ID (int)        The job ID
    n_cores (int)   Number of cores
    mem (float)     Required memory
    node (str)      The node where this job runs
    
    Behaviour:
    This is a simple class that represents
    the job as seen by qpy-multiuser.

    See also:
    Job
    """
    __slots__ = (
        'user',
        'ID',
        'n_cores',
        'mem',
        'node')
    
    def __init__(self, user, jobID, mem, n_cores, node):
        """Inilialise the class
        
        Arguments:
        user (str)     The user that runs this job
        ID (int)       The job ID
        n_cores        Number of cores
        mem            Required memory
        node           The node where this job runs
        """
        self.user = user
        self.ID = jobID
        self.n_cores = n_cores
        self.mem = mem
        self.node = node

    def __eq__(self, other):
        """Check if self equals other."""
        return (self.user == other.user
                and self.ID == other.ID
                and self.n_cores == other.n_cores
                and self.mem == other.mem
                and self.node == other.node)

    def __str__(self):
        """String representation."""
        return (str(self.ID) + ": node = " + self.node
                + ", n_cores = " + str(self.n_cores)
                + ", mem = " + str(self.mem))


class Job(object):
    """A job submitted by the user.
    
    Attributes:
    ID (int)                A number to identify the job
    info (list)             A list of strings with the command (in [0]) and
                            the calling directory (in [1])
    n_cores (int)           Number of cores used by the job
    mem (float)             Memory requested for this job
    node_attr (list)        The node attributes for this job
    node (UsersNode)        The node where this job is running
                            It is None for jobs in the queue or undone
    status (int)            The status of the job. These can be:
                            qpyconst.JOB_ST_QUEUE
                            qpyconst.JOB_ST_UNDONE
                            qpyconst.JOB_ST_RUNNING
                            qpyconst.JOB_ST_DONE
                            qpyconst.JOB_ST_KILLED
    use_script_copy (bool)  If True, copy the script of the job, such that
                            the user can alter the script and the original
                            script will be used by qpy
    cp_script_to_replace    If not None, is a list with two strings, with
                            the script names to be replaced in the command
                            (used in conjunction with use_script_copy)
    re_run (bool)           If True, it means that this job started to run in
                            a previous instance of qpy-master
    opt_parser (JobParser)  A parser for the job options. It might be None,
                            if is a job that has been restarted, and thus
                            there is no need for a parse
    queue_time (datetime)   When the job was put in queue (that is, submited
                            by the user)
    start_time (datetime)   When the job started to run
    end_time (datetime)     When the job finished (as detected by this program.
                            It should not be used to check execution time
                            accurately)
    run_duration (datetime) Duration of the job execution
    
    Behaviour:
    This class has all the information about individual jobs.
    """
    __slots__ = ("ID",
                 "info",
                 "opt_parser",
                 "n_cores",
                 "mem",
                 "node_attr",
                 "node",
                 "status",
                 "use_script_copy",
                 "cp_script_to_replace",
                 "re_run",
                 "queue_time",
                 "start_time",
                 "end_time",
                 "run_duration")

    def __init__(self, jobID, job_info, config, options_parser=None):
        """Initiate the class.
        
        Arguments:
        jobID (int)                 The job identifier
        job_info (list)             A list of strings with the command
                                    (in [0]) and the calling directory (in [1])
        config (Configurations)     The qpy configurations
        """
        self.ID = jobID
        self.info = job_info
        self.opt_parser = options_parser
        self.n_cores = 1
        self.mem = 5.0
        self.node_attr = []
        self.node = None
        self.status = qpyconst.JOB_ST_QUEUE
        self.use_script_copy = config.use_script_copy
        self.cp_script_to_replace = None
        self.re_run = False
        self.queue_time = datetime.today()
        self.start_time = None
        self.end_time = None
        self.run_duration = None

    def run_duration_(self):
        """Return the running time or the queue time.
        
        Behaviour:
        If the job is running, return the current running time;
        If the job is in the queue, return for how long it is in queue;
        If the job has been finished, return self.run_duration and
        sets it if is still None.
        """
        if (self.status == qpyconst.JOB_ST_QUEUE):
            try:
                return datetime.today() - self.queue_time
            except:
                return None
        if (self.status == qpyconst.JOB_ST_RUNNING):
            try:
                return datetime.today() - self.start_time
            except:
                return None
        if (self.run_duration is not None):
            return self.run_duration
        try:
            self.run_duration = self.end_time - self.start_time
        except:
            self.run_duration = None
        return self.run_duration

    def __str__(self):
        """Return a formatted string with main information about the job."""
        job_str = (str(self.ID) + ' '
                   + str(self.status) + ' '
                   + str(self.n_cores) + ' '
                   + str(self.mem) + ' '
                   + str(self.use_script_copy) + ' ')
        if (self.cp_script_to_replace is not None):
            job_str += (self.cp_script_to_replace[0] + ' '
                        + self.cp_script_to_replace[1])
        job_str += '\n'
        if (self.node is None):
            job_str += 'None'
        else:
            job_str += repr(self.node)
        job_str += ('---' + str(self.queue_time)
                    + '---' + str(self.start_time)
                    + '---' + str(self.end_time) + '\n')
        job_str += self.info[0] + '\n'
        job_str += self.info[1]
        for i in self.node_attr:
            job_str += ' ' + i
        job_str += '\n'
        return job_str

    def fmt(self, pattern):
        """Format the job in a string according to the pattern.
        
        Arguments:
        pattern (str)     The pattern to be used
        
        Behaviour:
        Return the string pattern, after replacing some key strings
        as follows:
        %j    job ID
        %s    status
        %c    command
        %d    directory
        %a    requested node attributes
        %N    number of cores
        %m    requested memory
        %n    node
        %A    node or requested node attributes
        %Q    time the job was put in queue
        %S    time the job started to run
        %E    time job finished
        %R    running time
        %K    notes
        
        Return:
        A string with the job formatted as described above
        
        TODO:
        Call run_duration only if needed. Currently does all the time.
        """
        job_str = pattern
        try:
            str_node = str(self.node)
        except:
            str_node = 'None'

        if '%K' in job_str:
            if os.path.isfile(qpysys.notes_dir + 'notes.' + str(self.ID)):
                f = open(qpysys.notes_dir + 'notes.' + str(self.ID), 'r')
                notes = f.read()
                f.close()
                job_str = job_str.replace('%K', '\n' + notes)
            else:
                job_str = job_str.replace('%K', '')

        for pattern, info in (('%j', str(self.ID)),
                              ('%s', qpyconst.JOB_STATUS[self.status]),
                              ('%c', self.info[0]),
                              ('%d', self.info[1]),
                              ('%a', ' '.join(self.node_attr)),
                              ('%N', str(self.n_cores)),
                              ('%m', str(self.mem)),
                              ('%n', str_node),
                              ('%A', str_node if (self.node is not None)
                               else ('[' + ' '.join(self.node_attr) + ']')),
                              ('%Q', str(self.queue_time)),
                              ('%S', str(self.start_time)),
                              ('%E', str(self.end_time)),
                              ('%R', str(self.run_duration_()))
                              ):
            job_str = job_str.replace(pattern, info)
        return job_str

    def run(self, config):
        """Run the job."""
        def out_or_err_name(job, postfix):
            assert(postfix in ['.out', '.err'])
            return ('{dir}/job_{id}{postfix}'.format(dir=job.info[1],
                                                     id=str(job.ID),
                                                     postfix=postfix))
        command = []
        command.append('exec > {0}'.format(out_or_err_name(self, '.out')))
        command.append('exec 2> {0}'.format(out_or_err_name(self, '.err')))
        command.append('export QPY_JOB_ID={0}'.format(self.ID))
        command.append('export QPY_NODE={0}'.format(self.node))
        command.append('export QPY_N_CORES={0}'.format(self.n_cores))
        command.append('export QPY_MEM={0}'.format(self.mem))
        command.append('ulimit -Sv {0}'.format(
            int(max(self.mem * 1.5, 10) * 1048576)))
        for sf in config.source_these_files:
            command.append('source {0}'.format(sf))
        command.append('cd {0}'.format(self.info[1]))
        try:
            command.append(
                self.info[0].replace(self.cp_script_to_replace[0],
                                     'sh '
                                     + self.cp_script_to_replace[1],
                                     1))
        except:
            command.append(self.info[0])
        command = '; '.join(command)
        config.logger.info("Sending command: %s", command)
        try:
            qpycomm.node_exec(self.node.address,
                              command,
                              get_outerr=False,
                              pKey_file=config.ssh_p_key_file,
                              localhost_popen_shell=(
                                  self.node.address == 'localhost'))
        except:
            config.logger.error("Exception in run", exc_info=True)
            raise Exception("Exception in run: " + str(sys.exc_info()[1]))
 
        self.start_time = datetime.today()
        self.status = qpyconst.JOB_ST_RUNNING

    def is_running(self, config):
        """Check if the job is running.
        
        Arguments:
        config (Configurations)    qpy configurations
        
        Return:
        True if job is running, False otherwise.
        
        Raise:
        Exceptions from the SSH connection if the
        connection to the node is not successful.
        """
        command = 'ps -fu ' + qpysys.sys_user
        (std_out, std_err) = qpycomm.node_exec(self.node.address,
                                               command,
                                               pKey_file=config.ssh_p_key_file)
        re_res = re.search('export QPY_JOB_ID=' + str(self.ID) + ';', std_out)
        if (re_res):
            return True
        return False

    def _expand_script_name(self, file_name):
        """Expand a filename to its absolute name, UNIX only.
        
        TODO:
        Make it compatible to other systems.
        """
        if file_name[0] == '/':
            # assuming its the absolute Path
            script_name = file_name
        elif file_name[0] == '~':
            # relative to home directory
            script_name = os.getenv("HOME") + file_name[1:]
        else:
            # in or relative to working directory
            script_name = self.info[1] + '/' + file_name
        script_list = glob.glob(script_name)
        if len(script_list) == 1:
            return script_list[0]
        elif len(script_list) == 0:
            return
        else:
            raise ParseError('Nonexitstent or ambigous script name')

    def parse_options(self):
        """Parse the input and the submission script for options."""
        options = {'n_cores': self.n_cores,
                   'mem': self.mem,
                   'node_attr': self.node_attr,
                   'use_script_copy': self.use_script_copy}
        self.info[0] = self.opt_parser.parse_cmd_line(self.info[0], options)
        first_arg = self.info[0].split()[0]
#        script_name = self._expand_script_name(first_arg)
#        if script_name is not None:
#            self.opt_parser.parse_file(script_name, options)
        self.n_cores = options['n_cores']
        self.mem = options['mem']
        self.node_attr = options['node_attr']
        self.use_script_copy = options['use_script_copy']

    def asked(self, pattern):
        """Return True if the job obbeys the pattern
        
        Arguments::
        pattern (dict)    a dictionary indicating the pattern
        
        Empty pattern means that everthing obbeys it.
        """
        req = not ("status" in pattern
                   or "job_id" in pattern
                   or "dir" in pattern)
        for k in pattern:
            if k == 'status':
                req = req or qpyconst.JOB_STATUS[self.status] in pattern[k]
            elif k == "job_id":
                req = req or self.ID in pattern[k]
            elif k == 'dir':
                req = req or self.info[1] in pattern[k]
        return req


class JobCollection(object):
    """Store information about the jobs.
    
    Attributes:
    config (Configurations)     The qpy configurations
    all (list)                  All jobs
    queue (list)                Jobs in queue
    running (list)              Running jobs
    done (list)                 Jobs that are done
    killed (list)               Killed jobs
    undone (list)               Undone jobs
    Q (deque)                   The queue
    lock (RLock)                To use when dealing with the above lists
    """

    __slots__ = (
        'config',
        'all',
        'queue',
        'running',
        'done',
        'killed',
        'undone',
        'Q',
        'lock')
    
    def __init__(self, config):
        """Initiate the class.
        
        Arguments:
        config (Configurations)      qpy configurations
        """
        self.config = config
        self.all = []
        self.queue = []
        self.running = []
        self.done = []
        self.killed = []
        self.undone = []
        self.Q = deque()
        self.lock = threading.RLock()
        self.initialize_old_jobs()

    def Q_pop(self, i=-1):
        """Pop job of Q, and return it."""
        with self.lock:
            if i == -1:
                job = self.Q.pop()
            else:
                job = self.Q[i]
                del self.Q[i]
        return job

    def Q_appendleft(self, job):
        """Append 'job' to the left of Q."""
        with self.lock:
            self.Q.appendleft(job)

    def append(self, job, to_list):
        """Append 'job' to the Q."""
        with self.lock:
            to_list.append(job)

    def remove(self, job, from_list):
        """Remove 'job' from the list 'from_list'."""
        with self.lock:
            from_list.remove(job)

    def mv(self, job, from_list, to_list):
        """Move the 'job' from 'from_list' to 'to_list'."""
        with self.lock:
            from_list.remove(job)
            to_list.append(job)

    def check(self, pattern, config):
        """Return string with information on required jobs.
        
        Arguments:
        pattern (dict)            The pattern that job must obbey.
        config (Configurations)   qpy configurations
        
        Return:
        A string with the jobs in this collection that obbey
        the pattern
        
        See also:
        JOB.asked
        """
        asked_jobs = []
        with self.lock:
            for job in self.all:
                if (job.asked(pattern)):
                    asked_jobs.append(job)
        req_jobs = ''
        for job in asked_jobs:
            j_str = job.fmt(config.job_fmt_pattern)
            if (config.use_colour):
                j_str = termcolour.colored(j_str,
                                           config.colour_scheme[job.status])
            req_jobs += j_str
        return req_jobs

    def write_all_jobs(self):
        """Write jobs in file (global) all_jobs_file."""
        with self.lock:
            f = open(qpysys.all_jobs_file, 'w')
            for job in self.all:
                f.write(str(job))
            f.close()

    def multiuser_cur_jobs(self):
        """Return a list of the running jobs as MultiuserJob."""
        cur_jobs = []
        with self.lock:
            for job in self.running:
                cur_jobs.append(MultiuserJob(qpysys.user,
                                             job.ID,
                                             job.mem,
                                             job.n_cores,
                                             job.node.name))
        return cur_jobs

    def initialize_old_jobs(self):
        """Initialize jobs from file (global) all_jobs_file."""
        with self.lock:
            if os.path.isfile(qpysys.all_jobs_file):
                with open(qpysys.all_jobs_file, 'r') as f:
                    i = 0
                    for line in f:
                        i += 1
                        if i % 4 == 1:
                            line_spl = line.split()
                            new_id = line_spl[0]
                            new_status = line_spl[1]
                            new_n_cores = line_spl[2]
                            new_mem = line_spl[3]
                            # old way.
                            # Remove as soon as everybody has new version working
                            if len(line_spl) == 4:
                                new_use_script_copy = False
                                new_cp_script_to_replace = None
                            else:
                                new_use_script_copy = (
                                    True
                                    if (line_spl[4] == 'true') else
                                    False)
                                if len(line_spl) == 5:
                                    new_cp_script_to_replace = None
                                else:
                                    new_cp_script_to_replace = (line_spl[5],
                                                                line_spl[6])
                        elif i % 4 == 2:
                            new_node_and_times = line.strip().split('---')
                            new_node = new_node_and_times[0]
                            if len(new_node_and_times) == 1:
                                new_times = ['None', 'None', 'None']
                            else:
                                new_times = new_node_and_times[1:]
                        elif i % 4 == 3:
                            new_command = line.strip()
                        else:
                            lspl = line.split()
                            new_wd = lspl[0]
                            new_node_attr = [] if len(lspl) == 1 else lspl[1:]
                            new_job = Job(int(new_id),
                                          [new_command, new_wd],
                                          self.config)
                            new_job.n_cores = int(new_n_cores)
                            new_job.mem = float(new_mem)
                            if new_times[0] == 'None':
                                new_job.queue_time = None
                            else:
                                new_job.queue_time = datetime.strptime(
                                    new_times[0],
                                    "%Y-%m-%d %H:%M:%S.%f")
                            if new_times[1] == 'None':
                                new_job.start_time = None
                            else:
                                new_job.start_time = datetime.strptime(
                                    new_times[1],
                                    "%Y-%m-%d %H:%M:%S.%f")
                            if new_times[2] == 'None':
                                new_job.end_time = None
                            else:
                                new_job.end_time = datetime.strptime(
                                    new_times[2],
                                    "%Y-%m-%d %H:%M:%S.%f")
                                new_job.run_duration_()
                            if new_node == 'None':
                                new_job.node = None
                            else:
                                new_job.node = qpynodes.UsersNode.from_string(
                                    new_node)
                            new_job.status = int(new_status)
                            new_job.node_attr = new_node_attr
                            self.all.append(new_job)
                            if new_job.status == qpyconst.JOB_ST_QUEUE:
                                self.queue.append(new_job)
                                self.Q.appendleft(new_job)
                            elif new_job.status == qpyconst.JOB_ST_RUNNING:
                                self.running.append(new_job)
                                new_job.re_run = True
                            elif new_job.status == qpyconst.JOB_ST_DONE:
                                self.done.append(new_job)
                            elif new_job.status == qpyconst.JOB_ST_KILLED:
                                self.killed.append(new_job)
                            elif new_job.status == qpyconst.JOB_ST_UNDONE:
                                self.undone.append(new_job)

    def jump_Q(self, job_list, pos):
        """Reorganize the queue.
        
        Arguments:
        job_list    the jobs to be moved
        pos         their final position
        
        Behaviour:
        Put the jobs in job_list to be submited just before pos
        if pos =  0: put them in the beginning of queue
                 -1: put them in the end of queue
        
        Return:
        A string with an informative message.
        """
        with self.lock:
            if pos in job_list:
                return 'Position is part of list: queue not reordered.\n'
            found_pos = False
            if pos > 0:
                for j in self.Q:
                    if j.ID == pos:
                        found_pos = True
                        break
                if not(found_pos):
                    return 'Position not found: queue not reordered.\n'
            jobs_to_jump = []
            for j in self.Q:
                if j.ID in job_list:
                    jobs_to_jump.append(j)
            for j in jobs_to_jump:
                self.Q.remove(j)
            if not(jobs_to_jump):
                return ('List does not have jobs in queue:'
                        + ' queue not reordered.\n')
            jobs_in_front = []
            lenQ = len(self.Q)
            for i in range(0, lenQ):
                if self.Q[0].ID == pos:
                    break
                jobs_in_front.append(self.Q.popleft())
            first_remain_Q = self.Q[0] if len(self.Q) > 0 else None
            # last_remain_Q = self.Q[-1] if len(self.Q) > 0 else None
            first_in_front = (jobs_in_front[0]
                              if len(jobs_in_front) > 0 else
                              None)
            last_in_front = (jobs_in_front[-1]
                             if len(jobs_in_front) > 0 else
                             None)
            if pos == -1:
                for j in jobs_in_front[::-1]:
                    self.Q.appendleft(j)
            for j in jobs_to_jump[::-1]:
                self.Q.appendleft(j)
            if pos != -1:
                for j in jobs_in_front[::-1]:
                    self.Q.appendleft(j)
            # reorder self.queue and self.all if needed
            if last_in_front is None and first_remain_Q is None:
                return ('Queue is completely contained in list:'
                        + ' queue not reordered.\n')
            if last_in_front is not None or first_remain_Q is not None:
                for j in jobs_to_jump:
                    self.all.remove(j)
                    self.queue.remove(j)
                if pos == 0:
                    iq = 0
                    ia = self.all.index(last_in_front)
                elif pos == -1:
                    iq = len(self.queue)
                    ia = self.all.index(first_in_front) + 1
                else:
                    iq = self.queue.index(first_remain_Q) + 1
                    ia = self.all.index(first_remain_Q) + 1
                self.queue[iq:iq] = jobs_to_jump[::-1]
                self.all[ia:ia] = jobs_to_jump[::-1]
            return 'Queue reordered.\n'
