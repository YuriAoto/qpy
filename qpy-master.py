# qpy-master - set the main driver for qpy
#
# 29 May 2015 - Pradipta and Yuri
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
import threading
from time import sleep, time
from collections import deque
from Queue import Queue
import re
import subprocess
import sys
import os
import glob
import datetime
from shutil import copyfile
import termcolor.termcolor as termcolour
from optparse import OptionParser,OptionError
from qpyCommon import *
import logging
import logging.handlers
import traceback

class MyError(Exception):
    def __init__(self,msg):
        self.message=msg

class ParseError(MyError):
    pass

class HelpException(MyError):
    pass

# Important files and paths
home_dir = os.environ['HOME']
user = os.environ['USER']
if (TEST_RUN):
    qpy_dir = os.path.expanduser( '~/.qpy-test/')
else:
    qpy_dir = os.path.expanduser( '~/.qpy/')
scripts_dir = qpy_dir + '/scripts/'
notes_dir = qpy_dir + '/notes/'
jobID_file = qpy_dir + '/next_jobID'
all_jobs_file = qpy_dir + '/all_jobs'
config_file = qpy_dir + '/config'
multiuser_conn_file = qpy_dir + 'multiuser_connection'
master_conn_file = qpy_dir + 'master_connection'
master_log_file = qpy_dir + 'master.log'


if (not(os.path.isdir(qpy_dir))):
    os.makedirs(qpy_dir)
os.chmod(qpy_dir, 0700)

if (not(os.path.isdir(scripts_dir))):
    os.makedirs(scripts_dir)

if (not(os.path.isdir(notes_dir))):
    os.makedirs(notes_dir)

if (os.path.isfile(master_conn_file+'_port')):
    sys.exit('A connection file was found. Is there a qpy-master instance running?')

if (os.path.isfile(master_conn_file+'_conn_key')):
    sys.exit('A connection file was found. Is there a qpy-master instance running?')

multiuser_address, multiuser_port, multiuser_key = read_conn_files(multiuser_conn_file)
if (multiuser_port == None or multiuser_key == None):
    sys.exit("Information for multiuser connection could not be obtained. Contact your administrator.")

logger = configure_root_logger(master_log_file, logging.WARNING)

class JobParser(OptionParser):
    """An Option Parser that does not exit the program but just raises a ParseError

    NOTE: optparse is depreciated and the overwritten functions are somewhat mentioned in the documentation.
    """
    def exit(self,prog='',message=''):
        raise ParseError(message)
    def error(self,message):
        raise ParseError(message)
    def print_usage(self):
        pass
    def print_version(self):
        pass
    def print_help(self):
        raise HelpException(self.format_help())

class JOB(object):

    """The job, submitted by the user that will run on a node

    Contains:
    ID
    info
    n_cores
    mem

    This class has all the information about individual jobs.

    """

    __slots__=("ID","info","n_cores","mem","node","status","use_script_copy","cp_script_to_replace","re_run","queue_time","start_time","end_time","runDuration","parser")
    def __init__(self, jobID, job_info, config):
        """Initiate the class

        Arguments:
        jobID
        job_info
        config
        """
        self.ID = jobID
        self.info = job_info
        self.n_cores = 1
        self.mem = 5.0
        self.node = None
        self.status = JOB_ST_QUEUE
        self.use_script_copy = config.use_script_copy
        self.cp_script_to_replace = None
        self.re_run = False
        self.set_parser()
        self.queue_time = datetime.datetime.today()
        self.start_time = None
        self.end_time = None
        self.runDuration = None

    def run_duration(self):
        """The running time or the queue time

        If the job is running, returns the current running time.
        If the job is in the queue, returns for how long it is in queue
        If the job has been finished, returns self.runDuration or sets it
        if is still None
        """
        if (self.status == JOB_ST_QUEUE):
            try:
                return datetime.datetime.today() - self.queue_time
            except:
                return None
        if (self.status == JOB_ST_RUNNING):
            try:
                return datetime.datetime.today() - self.start_time
            except:
                return None
        if (self.runDuration != None):
            return self.runDuration
        try:
            self.runDuration = self.end_time - self.start_time
        except:
            self.runDuration = None
        return self.runDuration

    def __str__(self):
        """Returns a formatted string with important information."""
        job_str = str(self.ID) + ' ' + str(self.status) + ' ' + str(self.n_cores) + ' ' + str(self.mem) + ' ' + str(self.use_script_copy) + ' '
        if (self.cp_script_to_replace != None):
            job_str += self.cp_script_to_replace[0] + ' ' + self.cp_script_to_replace[1]
        job_str += '\n'
        if (self.node == None):
            job_str += 'None'
        else:
            job_str += self.node
        job_str += '---' + str(self.queue_time) + '---' + str(self.start_time) + '---' + str(self.end_time) + '\n'
        job_str += self.info[0] + '\n'
        job_str += self.info[1] + '\n'
        return job_str

    def fmt(self, pattern):
        """Format the job in a string according to the pattern"""
        job_str = pattern
        try:
            str_node = str(self.node)
        except:
            str_node = 'None'

        if ('%K' in job_str):
            if (os.path.isfile(notes_dir + 'notes.' + str(self.ID))):
                f = open(      notes_dir + 'notes.' + str(self.ID), 'r')
                notes = f.read()
                f.close()
                job_str = job_str.replace('%K', '\n' + notes)
            else:
                job_str = job_str.replace('%K', '')

        for pattern, info in (('%j', str(self.ID)),
                              ('%s', JOB_STATUS[self.status]),
                              ('%c', self.info[0]),
                              ('%d', self.info[1]),
                              ('%n', str_node),
                              ('%Q', str(self.queue_time)),
                              ('%S', str(self.start_time)),
                              ('%E', str(self.end_time)),
                              ('%R', str(self.run_duration())),
                              ('%N', str(self.n_cores))
                              ):
            job_str = job_str.replace(pattern, info)
        return job_str

    def set_parser(self):
        """creates a parser for the flags that can be set for a job"""
        parser=JobParser()
        parser.add_option("-n","--cores", dest="cores", help="set the number of cores", default="1")
        parser.add_option("-m","--mem","--memory",dest="memory",help="set the memory in GB", default="5")
        parser.add_option("-c","--copyScript",dest="cpScript",help="script should be copied",action='store_false')
        parser.add_option("-o","--originalScript",dest="orScript",help="use original script",action='store_false')
        parser.disable_interspersed_args()
        self.parser=parser

    def run(self, config):
        """Run the job."""
        def out_or_err_name(job, postfix):
            assert(postfix in ['.out', '.err'])
            return '{dir}/job_{id}{postfix}'.format(dir=job.info[1], id=str(job.ID), postfix=postfix )
        command =  'export QPY_JOB_ID=' + str( self.ID) + '; '
        command += 'export QPY_NODE=' + str( self.node) + '; '
        command += 'export QPY_N_CORES=' + str( self.n_cores) + '; '
        command += 'export QPY_MEM=' + str( self.mem) + '; '
# kills the job with large mem:       command += 'ulimit -Sv ' + str( self.mem*1048576) + '; '
        for sf in config.source_these_files:
           command += 'source ' + sf + '; '
        command += 'cd ' + self.info[1] + '; ' 
        try:
            command += self.info[0].replace( self.cp_script_to_replace[0], 'sh ' + self.cp_script_to_replace[1], 1)
        except:
            command += self.info[0]
        command += ' > ' + out_or_err_name( self, '.out') + ' 2> ' + out_or_err_name( self, '.err')

        try:
            node_exec(self.node, command, get_outerr = False, pKey_file = config.ssh_p_key_file)
        except:
            logger.error("Exception in run", exc_info=True)
            raise Exception( "Exception in run: " + str(sys.exc_info()[1]))

        self.start_time = datetime.datetime.today()
        self.status = JOB_ST_RUNNING


    def is_running(self, config):
        """Returns True if the job is running
        
        Raises exceptions from the SSH connection if the 
        connection to the node is not successful
        """
        command = 'ps -fu ' + user
        (std_out, std_err) = node_exec(self.node, command, pKey_file = config.ssh_p_key_file)
        re_res = re.search('export QPY_JOB_ID=' + str( self.ID) + ';', std_out)
        if (re_res):
            return True
        return False


    def _scanline(self,line,option_list):
        """ Parses the lines for qpy directives and options"""
        if (re.match( '#QPY', line)):
            option_found=False
            for attr,regexp in option_list:
                re_res = regexp.search(line )
                if ( re_res is not None ):
                    try:
                        if (attr == 'n_cores'):
                            self.n_cores = int(re_res.group(1))
                        if (attr == 'mem'):
                            self.mem = float(re_res.group(1))
                        if (attr == 'cpScript'):
                            self.use_script_copy = True if (re_res.group(1) == 'true') else False
                        option_found=True
                    except ValueError:
                        raise ParseError("Invalid Value for {atr} found.".format(atr=attr))
            if ( not option_found ):
                raise ParseError("QPY directive found, but no options supplied."\
                             "Please remove the #QPY directive if you don't want to supply a valid option.")


    def _parse_file_for_options(self,file_name):
        """ parses a submission script file for options set in the script
        
        @param file_name
        """
        option_list=[("n_cores"  , re.compile('n_cores[= ]?(\d*)' ) ),
                     ("mem"      , re.compile('mem[= ]?(\d*)'     ) ),
                     ("cpScript" , re.compile('cpScript[= ]?(\w*)') )
                     ]
        try:
            with open( file_name, 'r') as f:
                for line in f:
                    self._scanline(line,option_list)
        except:
            pass # Is an executable or file_name = None

    def _parse_command_for_options(self, command):
        try:
            options,command = self.parser.parse_args( command.split())
            self.n_cores = int(options.cores)
            self.mem = float(options.memory)
            if (options.cpScript == None and options.orScript == None):
                pass
            elif (options.cpScript != None and options.orScript != None):
                raise ParseError("Please, do not supply both cpScript or orScript")
            elif (options.cpScript != None):
                self.use_script_copy = True
            else:
                self.use_script_copy = False

        except (AttributeError, TypeError), ex:
            raise ParseError("Something went wrong. please contact the qpy-team\n"+ex.message)
        except ValueError:
            raise ParseError("Please supply only full numbers for memory or number of cores, true or false for cpScript")
        return ' '.join(command)

    
    def _expand_script_name(self, file_name):
        """Expands a filename to its absolute name, UNIX only"""
        if ( file_name[0] == '/'):
            #assuming its the absolute Path
            script_name = file_name
        elif( file_name[0] == '~'):
            #relative to home directory
            script_name = os.getenv("HOME")+file_name[1:]
        else:
            #in or relative to working directory
            script_name = self.info[1] + '/' + file_name
        script_list = glob.glob( script_name)
        if ( len(script_list) == 1):
            return script_list[0]
        elif (len(script_list)==0):
            return
        else:
            raise ParseError('Nonexitstent or ambigous script name')


    def parse_options(self):
        """ Parses the input and the submission script for options"""

        self.info[0] = self._parse_command_for_options(self.info[0])
        first_arg = self.info[0].split()[0]
        script_name = self._expand_script_name(first_arg)
        self._parse_file_for_options(script_name)


    def asked(self, pattern):
        """
        Return a boolean, indicating whether this job obbeys the dictionary pattern or not
        empty pattern means that everthing is required
        """
        req = not ("status" in pattern
                   or "job_id" in pattern
                   or "dir" in pattern)
        for k in pattern:
            if (k == 'status'):
                req = req or JOB_STATUS[self.status] in pattern[k]
            elif ( k == "job_id"):
                req = req or self.ID in pattern[k]
            elif k == 'dir':
                req = req or self.info[1] in pattern[k]
        return req


class Job_Collection():

    """Stores information about the jobs
    
    Contains:
    all       a list with all jobs
    queue     a list with jobs in queue
    running   a list with the running jobs
    done      a list with the done jobs
    killed    a list with the killed jobs
    undone    a list with the undone jobs
    Q         a deque for the queue
    lock      a Thread.RLock to use when dealing with the above lists
    """

    def __init__(self, config):
        """Initiate the class

        Arguments:
        config   the Configurations
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

    def Q_pop(self):
        with self.lock:
            job = self.Q.pop()
        return job

    def Q_appendleft(self, job):
        with self.lock:
            self.Q.appendleft(job)

    def append(self, job, to_list):
        with self.lock:
            to_list.append(job)

    def remove(self, job, from_list):
        with self.lock:
            from_list.remove(job)

    def mv(self, job, from_list, to_list):
        """Move the job from a list to another"""
        with self.lock:
            from_list.remove(job)
            to_list.append(job)

    def check(self, pattern, config):
        """Returns string with information on required jobs, defined by pattern"""
        asked_jobs = []
        with self.lock:
            for job in self.all:
                if (job.asked(pattern)):
                    asked_jobs.append( job)

        req_jobs = ''
        for job in asked_jobs:
            j_str = job.fmt(config.job_fmt_pattern)
            if (config.use_colour):
                j_str = termcolour.colored(j_str, config.colour_scheme[job.status])
            req_jobs += j_str

        return req_jobs

    def write_all_jobs(self):
        """Write jobs in file."""
        with self.lock:
            f = open(all_jobs_file, 'w')
            for job in self.all:
                f.write(str(job))
            f.close()

    def multiuser_cur_jobs(self):
        """Returns a list of the running jobs as MULTIUSER_JOB."""
        cur_jobs = []
        with self.lock:
            for job in self.running:
                cur_jobs.append(MULTIUSER_JOB(user, job.ID, job.mem, job.n_cores, job.node))
        return cur_jobs

    def initialize_old_jobs(self, sub_ctrl):
        """Initialize jobs from file."""
        with self.lock:
            if (os.path.isfile(all_jobs_file)):
                with open(all_jobs_file, 'r') as f:
                    i = 0
                    for line in f:
                        i += 1
                        if (i%4 == 1):
                            line_spl = line.split()
                            new_id = line_spl[0]
                            new_status = line_spl[1]
                            new_n_cores = line_spl[2]
                            new_mem = line_spl[3]
                            if (len( line_spl) == 4): # old way. Remove as soon as everybody has new version working
                                new_use_script_copy = False
                                new_cp_script_to_replace = None
                            else:
                                new_use_script_copy = True if ( line_spl[4] == 'true') else False
                                if (len( line_spl) == 5):
                                    new_cp_script_to_replace = None
                                else:
                                    new_cp_script_to_replace = (line_spl[5], line_spl[6])
                        elif (i%4 == 2):
                            new_node_and_times = line.strip().split('---')
                            new_node = new_node_and_times[0]
                            if (len( new_node_and_times) == 1):
                                new_times = ['None','None','None']
                            else:
                                new_times = new_node_and_times[1:]
                        elif (i%4 == 3):
                            new_command = line.strip()
                        else:
                            new_wd = line.strip()
                            new_job = JOB( int(new_id), [new_command, new_wd], self.config)
                            new_job.n_cores = int( new_n_cores)
                            new_job.mem = float( new_mem)
                            if (new_times[0] == 'None'):
                                new_job.queue_time = None
                            else:
                                new_job.queue_time = datetime.datetime.strptime(new_times[0], "%Y-%m-%d %H:%M:%S.%f")
                            if (new_times[1] == 'None'):
                                new_job.start_time = None
                            else:
                                new_job.start_time = datetime.datetime.strptime(new_times[1], "%Y-%m-%d %H:%M:%S.%f")
                            if (new_times[2] == 'None'):
                                new_job.end_time = None
                            else:
                                new_job.end_time = datetime.datetime.strptime(new_times[2], "%Y-%m-%d %H:%M:%S.%f")
                                new_job.run_duration()
                            if (new_node == 'None'):
                                new_job.node = None
                            else:
                                new_job.node = new_node
                            new_job.status = int( new_status)
                            self.all.append( new_job)
                            if (new_job.status == JOB_ST_QUEUE):
                                self.queue.append( new_job)
                                self.Q.appendleft( new_job)
                            elif (new_job.status == JOB_ST_RUNNING):
                                self.running.append( new_job)
                                new_job.re_run = True
                            elif (new_job.status == JOB_ST_DONE):
                                self.done.append( new_job)
                            elif (new_job.status == JOB_ST_KILLED):
                                self.killed.append( new_job)
                            elif (new_job.status == JOB_ST_UNDONE):
                                self.undone.append( new_job)


    def jump_Q(self, job_list, pos):
        """Reorganize the queue

        Arguments:
        job_list  the jobs to be moved
        pos       their final position

        Puts the jobs in job_list to be submited just before pos
        if pos =  0: put them in the beginning of queue
                 -1: put them in the end of queue

        Returns a string with a informative message.
        """
        with self.lock:
            if (pos in job_list):
                return 'Position is part of list: queue not reordered.\n'
            found_pos = False
            if(pos > 0):
                for j in self.Q:
                    if (j.ID == pos):
                        found_pos = True
                        break
                if (not( found_pos)):
                    return 'Position not found: queue not reordered.\n'
            jobs_to_jump = []
            for j in self.Q:
                if(j.ID in job_list):
                    jobs_to_jump.append( j)
            for j in jobs_to_jump:
                self.Q.remove( j)
            if (not( jobs_to_jump)):
                return 'List does not have jobs in queue: queue not reordered.\n'
            jobs_in_front = []
            lenQ = len( self.Q)
            for i in range(0, lenQ):
                if (self.Q[0].ID == pos):
                    break
                jobs_in_front.append( self.Q.popleft())
            first_remain_Q = self.Q[0] if len( self.Q) > 0 else None
            last_remain_Q = self.Q[-1] if len( self.Q) > 0 else None
            first_in_front = jobs_in_front[0] if len( jobs_in_front) > 0 else None
            last_in_front = jobs_in_front[-1] if len( jobs_in_front) > 0 else None
            if (pos == -1):
                for j in jobs_in_front[::-1]:
                    self.Q.appendleft( j)
            for j in jobs_to_jump[::-1]:
                self.Q.appendleft( j)
            if (pos != -1):
                for j in jobs_in_front[::-1]:
                    self.Q.appendleft( j)
            # reorder self.queue and self.all if needed
            if (last_in_front == None and first_remain_Q == None):
                return 'Queue is completely contained in list: queue not reordered.\n'
            if (last_in_front != None or first_remain_Q != None):
                for j in jobs_to_jump:
                    self.all.remove( j)
                    self.queue.remove( j)
                if (pos == 0):
                    iq = 0
                    ia = self.all.index( last_in_front)
                elif (pos == -1):
                    iq = len(self.queue)
                    ia = self.all.index( first_in_front)+1
                else:
                    iq = self.queue.index( first_remain_Q)+1
                    ia = self.all.index( first_remain_Q)+1
                self.queue[iq:iq] = jobs_to_jump[::-1]
                self.all[ia:ia] = jobs_to_jump[::-1]
            return 'Queue reordered.\n'


def analise_multiuser_status(info, check_n, check_u):
    """Transform the multiuser status message in a more readable way."""
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

    """Checks if the jobs are still running

    Contains:
    jobs              a Job_Collection
    multiuser_alive   an Event that is set if the multiuser is alive
    config            the Configurations
    finish            an Event that should be set to terminate this Thread


    This thread makes regular checks on the running jobs
    to see if they have been finished.
    If so, the job is changed from running to done
    and a message is sent to qpy-multiuser.

    This is done at each config.sleep_time_check_run seconds

    """

    def __init__(self, jobs, multiuser_alive, config):
        """Initiate the class.

        Arguments:
        jobs
        multiuser_alive
        config
        """
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive
        self.config = config

        threading.Thread.__init__(self)
        self.finish = threading.Event()

    def run(self):
        """Checks if the jobs are running regularlly."""
        while not self.finish.is_set():
            i = 0
            jobs_modification = False
            with jobs.lock:
                jobs_to_check = list(self.jobs.running)
            for job in jobs_to_check:
                try:
                    is_running = job.is_running(self.config)
                except:
                    logger.error('Exception in CHECK_RUN.is_running', exc_info=True)
                    self.config.messages.add('CHECK_RUN: Exception in is_running: ' + repr(sys.exc_info()[0]))
                    is_running = True
                if (not is_running and job.status == JOB_ST_RUNNING):
                    job.status = JOB_ST_DONE
                    job.end_time = datetime.datetime.today()
                    job.run_duration()
                    jobs_modification = True
                    self.jobs.mv(job, self.jobs.running, self.jobs.done)
                    self.skip_job_sub = 0
                    if (job.cp_script_to_replace != None):
                        if (os.path.isfile(job.cp_script_to_replace[1])):
                            os.remove(job.cp_script_to_replace[1])

                    try:
                        msg_back = message_transfer((MULTIUSER_REMOVE_JOB,
                                                     (user, job.ID, len(self.jobs.queue))),
                                                    multiuser_address, multiuser_port, multiuser_key)
                    except:
                        self.multiuser_alive.clear()
                        logger.error('Exception in CHECK_RUN message transfer', exc_info=True)
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

    """Kill the jobs when required

    Contains:
    jobs              a Job_Collection
    multiuser_alive   an Event that is set if the multiuser is alive
    config            the Configurations
    to_kill           a Queue, that receives the jobs to kill

    This thread waits for jobs to be killed, passed
    by the Queue to_kill. These have to be a JOB instance or the
    string "kill". In this later case, the thread is terminated.

    """

    def __init__( self, jobs, multiuser_alive, config):
        """Initiate the class.

        Arguments:
        jobs
        multiuser_alive
        config
        """
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive
        self.config = config

        threading.Thread.__init__( self)
        self.to_kill = Queue()

    def run(self):
        """Kills jobs sent to to_kill."""
        while True:
            job = self.to_kill.get()

            if (isinstance(job, str)):
                if (job == 'kill'):
                    break


            command = 'source ~/.bash_profile; qpy --jobkill ' + str(job.ID)
            try:
                if (job.status != JOB_ST_RUNNING):
                    raise
                (std_out, std_err) = node_exec(job.node, command, pKey_file = self.config.ssh_p_key_file)
            except:
                pass
            else:
                job.status = JOB_ST_KILLED
                job.end_time = datetime.datetime.today()
                job.run_duration()
                self.jobs.mv(job, self.jobs.running, self.jobs.killed)
                self.jobs.write_all_jobs()

                self.config.messages.add( 'Killing: ' + str(job.ID) + ' on node ' + job.node + '. stdout = ' + repr(std_out)  + '. stderr = ' + repr(std_err))

                try:
                    msg_back = message_transfer((MULTIUSER_REMOVE_JOB,
                                                 (user, job.ID, len(self.jobs.queue))),
                                                multiuser_address, multiuser_port, multiuser_key)
                except:
                    self.multiuser_alive.clear()
                    logger.error('Exception in JOBS_KILLER message transfer', exc_info=True)
                    self.config.messages.add('JOBS_KILLER: Exception in message transfer: ' + repr(sys.exc_info()[0]))
                else:
                    self.multiuser_alive.set()
                    self.config.messages.add('JOBS_KILLER: Message from multiuser: ' + str(msg_back))



class SUB_CTRL(threading.Thread):

    """Controls the job submission

    Contains:
    jobs                   a Job_Collection
    multiuser_handler      the multiuser_handler
    config                 the Configurations
    finish                 an Event that should be set to terminate this Thread
    skip_jobs_submission   skips the submission by this amount of cicles
    submit_jobs            jobs are submitted only if True

    This thread looks the jobs.queue and try to submit tje jobs, 
    asking for a node to qpy-multiuser and making the submission if
    a node is given.

    Each cycle is done at each config.sleep_time_sub_ctrl seconds.

    """

    def __init__(self, jobs, multiuser_handler, config):
        """Initiate the class.

        Arguments:
        jobs
        multiuser_handler
        config
        """
        self.jobs = jobs
        self.multiuser_handler = multiuser_handler
        self.config = config

        threading.Thread.__init__(self)
        self.finish = threading.Event()

        self.skip_job_sub = 0
        self.submit_jobs = True
            
    def run(self):
        """Submits the jobs."""
        n_time_multiuser = 0
        self.jobs.initialize_old_jobs(self)
        jobs_modification = False
        if not(self.multiuser_handler.multiuser_alive.is_set()):
            self.skip_job_sub = 30
            
        while not self.finish.is_set():

            if (not(self.multiuser_handler.multiuser_alive.is_set()) and self.skip_job_sub == 0):
                self.multiuser_handler.add_to_multiuser()
                if not(self.multiuser_handler.multiuser_alive.is_set()):
                    self.skip_job_sub = 30

            if (self.submit_jobs and self.skip_job_sub == 0):

                if (len(self.jobs.Q) != 0):
                    next_jobID = self.jobs.Q[-1].ID
                    next_Ncores = self.jobs.Q[-1].n_cores
                    next_mem = self.jobs.Q[-1].mem
                    avail_node = None

                    try:
                        msg_back = message_transfer((MULTIUSER_REQ_CORE,
                                                     (user, next_jobID, next_Ncores, next_mem, len(self.jobs.queue))),
                                                    multiuser_address, multiuser_port, multiuser_key)
                    except:
                        self.multiuser_handler.multiuser_alive.clear()
                        self.config.messages.add('SUB_CTRL: Exception in message transfer: ' + str(sys.exc_info()[0]) + '; ' + str(sys.exc_info()[1]))
                        logger.error('Exception in SUB_CTRL message transfer', exc_info=True)
                        self.skip_job_sub = 30
                    else:
                        self.multiuser_handler.multiuser_alive.set()
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
                                logger.error("Exception in SUB_CTRL when submitting job", exc_info=True)
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

    """Handles the messages sent from qpy-multiuser

    Contains:
    jobs              a Job_Collection
    multiuser_alive   an Event that is set if the multiuser is alive
    config            the Configurations
    Listener_master   a Listener, to receive messages from qpy-multiuser
    port              the port of the above connection (we share it with multiuser)
    conn_key          the key of the above connection (we share it with multiuser)
    

    This Thread waits messages from the qpy-multiuser.
    When one comes, it analyses it, does whatever it is needed
    and send a message back.

    The message must be a tuple (job_type, arguments)
    where job_type is:

    FROM_MULTI_CUR_JOBS
    FROM_MULTI_FINISH

    the arguments are option dependent.
    
    """

    def __init__(self, jobs, multiuser_alive, config):
        """Initiates the class

        Arguments:
        jobs
        multiuser_alive
        config

        It opens a new connection and the corresponding Listener.
        It also tells qpy-multiuser about the present user,
        sharing the port and the key of the above connection.

        """

        threading.Thread.__init__(self)

        self.jobs = jobs
        self.multiuser_alive = multiuser_alive
        self.config = config
        self.address = read_address_file(master_conn_file)

        (self.Listener_master, self.port, self.conn_key) = establish_Listener_connection(self.address,
                                                                                         PORT_MIN_MASTER, PORT_MAX_MASTER)
        self.add_to_multiuser()

    def run( self):
        """Waits the messages from qpy-multiuser."""
        while True:
            client_master = self.Listener_master.accept()
            (msg_type, arguments) = client_master.recv()
            self.config.messages.add( 'MULTIUSER_HANDLER: Received: ' + str(msg_type) + ' -> ' + str(arguments))
                
            # Get current list of jobs
            # arguments = ()
            if (msg_type == FROM_MULTI_CUR_JOBS):
                multiuser_cur_jobs = self.jobs.multiuser_cur_jobs()
                client_master.send(multiuser_cur_jobs)
                self.multiuser_alive.set()


            # Finishi this thread
            # arguments = ()
            elif (msg_type == FROM_MULTI_FINISH):
                client_master.send( 'Finishing MULTIUSER_HANDLER.')
                self.Listener_master.close()
                break

            else:
                client_master.send( 'Unknown option: ' + str( job_type) + '\n')


    def add_to_multiuser(self):

        multiuser_cur_jobs = self.jobs.multiuser_cur_jobs()
        try:
            msg_back = message_transfer((MULTIUSER_USER,
                                         (user, self.address, self.port, self.conn_key, multiuser_cur_jobs)),
                                        multiuser_address, multiuser_port, multiuser_key)
        except:
            self.multiuser_alive.clear()
            self.config.messages.add('MULTIUSER_HANDLER: Exception in message transfer: ' + repr(sys.exc_info()[0]) + ' ' + repr(sys.exc_info()[1]))
            logger.error('Exception in MULTIUSER_HANDLER message transfer', exc_info=True)
        else:
            if (msg_back[0] ==  2):
                self.multiuser_alive.clear()
            else:
                self.multiuser_alive.set()

            self.config.messages.add('MULTIUSER_HANDLER: Message from multiuser: ' + str(msg_back))




def handle_qpy(jobs, sub_ctrl, jobs_killer, config):
    """Handles the user messages sent from qpy

    Arguments:
    sub_ctrl      a SUB_CTRL
    jobs_killer   a JOBS_KILLER
    config        the Configurations

    It opens a new connection, share the port and key with qpy
    by te corresponding files and waits for messages from qpy.
    When a message is received, it analyzes it, does whatever
    is needed and returns a message back.

    The message from qpy must be a tuple (job_type, arguments)
    where job_type is
       JOBTYPE_SUB     - submit a job                           (sub)
       JOBTYPE_CHECK   - check the jobs                         (check)
       JOBTYPE_KILL    - kill a job                             (kill)
       JOBTYPE_FINISH  - kill the master                        (finish)
       JOBTYPE_CONFIG  - show config                            (config)
       JOBTYPE_CLEAN   - clean finished jobs                    (clean)

    
    """
    address = read_address_file(master_conn_file)
    (Listener_master, port, conn_key) = establish_Listener_connection(address,
                                                                      PORT_MIN_MASTER, PORT_MAX_MASTER)
    write_conn_files(master_conn_file, address, port, conn_key)

    job_id = Job_Id(jobID_file)

    while True:
        client_master = Listener_master.accept()
        (job_type, arguments) = client_master.recv()
        config.messages.add( "handle_qpy: Received: " + str(job_type) + " -> " + str(arguments))
            
        # Send a job
        # arguments = the job info (see JOB.info)
        if (job_type == JOBTYPE_SUB):
            new_job = JOB(int(job_id), arguments, config)
            try:
                new_job.parse_options()
                if (new_job.use_script_copy):
                    first_arg = new_job.info[0].split()[0]
                    script_name = new_job._expand_script_name(first_arg)
                    if (script_name != None):
                        copied_script_name = scripts_dir + 'job_script.' + str( new_job.ID)
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
        elif (job_type == JOBTYPE_CHECK):
            if (config.sub_paused):
                msg_pause = 'Job submission is paused.\n'
            else:
                msg_pause = ''

            client_master.send(jobs.check(arguments, config) + msg_pause)

        # Kill a job
        # arguments = a list of jobIDs and status (all, queue, running)
        elif (job_type == JOBTYPE_KILL):

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
                job.status = JOB_ST_UNDONE
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
                plural = get_plural( ('job', 'jobs'), n_kill_q)
                msg += plural[1] + ' ' + plural[0] + ' removed from the queue.\n'
            if (n_kill_r):
                plural = get_plural( ('job', 'jobs'), n_kill_r)
                msg += plural[1] + ' ' + plural[0] + ' will be killed.\n'

            if (not( msg)):
                msg = 'Nothing to do: required jobs not found.\n'
            else:
                jobs.write_all_jobs()

            client_master.send(msg)


        # Finish the execution of all threads
        # argumets: no arguments
        if (job_type == JOBTYPE_FINISH):
            client_master.send( 'Stopping qpy-master driver.\n')
            Listener_master.close()
            break


        # Show status
        # No arguments (yet)
        elif (job_type == JOBTYPE_STATUS):
            try:
                msg_back = message_transfer((MULTIUSER_STATUS, ()),
                                            multiuser_address, multiuser_port, multiuser_key)
            except:
                msg = 'qpy-multiuser seems not to be running. Contact the qpy-team.\n'
                multiuser_alive.clear()
            else:
                msg = msg_back[1]
                multiuser_alive.set()

            client_master.send( msg)


        # Control queue
        # arguments: a list: [<type>, <arguments>].
        elif (job_type == JOBTYPE_CTRLQUEUE):
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
        elif (job_type == JOBTYPE_CONFIG):
            if (arguments):
                (status, msg) = config.set_key(arguments[0], arguments[1])
                msg = msg + '\n'
                config.write_on_file()
            else:
                msg = str(config)

            client_master.send(msg)

        # Clean finished jobs
        # arguments = a list of jobIDs and status (all, done, killed, undone)
        elif (job_type == JOBTYPE_CLEAN):
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
                            remove = remove or not( arg_is_id) and i == JOB_STATUS[job.status]
                            if (remove):
                                if (job.status == JOB_ST_DONE):
                                    jobs.remove(job, jobs.done)
                                elif (job.status == JOB_ST_KILLED):
                                    jobs.remove(job, jobs.killed)
                                elif (job.status == JOB_ST_UNDONE):
                                    jobs.remove(job, jobs.undone)
                                jobs.remove(job, jobs.all)
                                n_jobs += 1
                                if (os.path.isfile( notes_dir + 'notes.' + str(job.ID))):
                                    os.remove(      notes_dir + 'notes.' + str(job.ID))
                        if (not( remove)):
                            ij += 1

            if (n_jobs):
                jobs.write_all_jobs()
                plural = get_plural(('job', 'jobs'), n_jobs)
                msg = plural[1] + ' finished ' + plural[0] + ' removed.\n'
            else:
                msg = 'Nothing to do: required jobs not found.\n'

            client_master.send( msg)

        # Add and read notes
        # arguments = (jobID[, note])
        elif (job_type == JOBTYPE_NOTE):
            if (len( arguments) == 0):
                all_notes = os.listdir( notes_dir)
                msg = ''
                for n in all_notes:
                    if (n[0:6] == 'notes.'):
                        msg += n[6:] + ' '
                if (msg):
                    msg = 'You have notes for the following jobs:\n' + msg + '\n'
                else:
                    msg = 'You have no notes.\n'
            else:
                notes_file = notes_dir + 'notes.' + str(arguments[0])
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
config = Configurations(config_file)
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
    logger.error('Exception at handle_qpy', exc_info=True)

    
# Finishing qpy-master
logging.info("Finishing qpy-master")
sub_ctrl.finish.set()
check_run.finish.set()
jobs_killer.to_kill.put('kill')
message_transfer((FROM_MULTI_FINISH, ()),
                 multiuser_handler.address, multiuser_handler.port,multiuser_handler.conn_key)
os.remove(master_conn_file+'_port')
os.remove(master_conn_file+'_conn_key')
