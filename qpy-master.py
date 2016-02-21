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
import random
import glob
from optparse import OptionParser,OptionError

from qpy_general_variables import *

DEVNULL = open(os.devnull, "w")

class MyError(Exception):
    def __init__(self,msg):
        self.message=msg

class ParseError(MyError):
    pass
class HelpException(MyError):
    pass

# Parse command line options
parser = OptionParser()
parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="print messages")

parser.add_option("-m", "--multiuser",
                  action="store_true", dest="multiuser", default=False,
                  help="set to multiuser behaviour")

parser.add_option("-s", "--saveMessages",
                  action="store_true", dest="saveMessages", default=False,
                  help="set to multiuser behaviour")

parser.add_option("-c", "--cluster",
                  dest="cl",
                  help="the cluster (linux4 and hlrs available)")

parser.add_option("-i", "--iniJobID",
                  dest="iniJobID",
                  help="the first jobID to be used")

(options, args) = parser.parse_args()

verbose = options.verbose
multiuser = options.multiuser
saveMessages = options.saveMessages
cluster = options.cl
ini_job_ID = options.iniJobID

if (not (cluster)):
    sys.exit( 'Give the --cluster option.')

if (cluster != "hlrs" and cluster != "linux4"):
    sys.exit( 'Cluster must be hlrs or linux4.')

if (multiuser and cluster == "hlrs"):
    sys.exit( 'Multiuser not available on hlrs.')

if (ini_job_ID):
    try:
        ini_job_ID = int( ini_job_ID)
    except:
        sys.exit( 'The option --iniJobID receive an integer as argument.')
else:
    ini_job_ID = None

dyn_nodes = cluster == "hlrs"


# Times, all in seconds
sleep_time_sub_ctrl = 1
sleep_time_check_run = 10

# dynamical nodes allocation
max_node_time = 1814400
min_working_time = 600
max_working_time = 604800
wait_initialization_time = 10

# multiuser
multiuser_waiting_time = 15
conn_multiuser_at = 300
conn_multiuser_at_not_working = 10

max_nodes_default = -1
if (dyn_nodes):
    max_nodes_default = 3

# Some global variables
home_dir = os.environ['HOME']
user = os.environ['USER']
qpy_dir = os.path.expanduser( '~/.qpy/')
source_these_files = ['~/.bash_profile']
port_file = qpy_dir + '/port'
cur_nodes_file = qpy_dir + '/current_nodes'
known_nodes_file = qpy_dir + '/known_nodes'
jobID_file = qpy_dir + '/next_jobID'
all_jobs_file = qpy_dir + '/all_jobs'

multiuser_address = 'localhost'
multiuser_key = 'zxcvb'
multiuser_port = 9999

job_fmt_pattern_def = '%j (%s):%c (on %n; wd: %d)\n'
job_fmt_pattern = job_fmt_pattern_def


if (saveMessages):
    multiuser_messages = deque( maxlen=25)

if (not( os.path.isdir( qpy_dir))):
    os.makedirs( qpy_dir)

if (not( os.path.isfile( known_nodes_file))):
    f = open( known_nodes_file, 'w')
    f.close()

if (not( os.path.isfile( jobID_file))):
    f = open( jobID_file, 'w')
    f.write( '1')
    f.close()

if (os.path.isfile( port_file)):
    sys.exit( 'A port file was found. Is there a qpy-master instance running?')


# Attempt to connect to qpy-multiuser
class try_multiuser_connection( threading.Thread):
    def __init__( self):
        threading.Thread.__init__( self)
        self.conn = None
        self.done = threading.Event()

    def run( self):
        try:
            self.conn = Client( (multiuser_address, multiuser_port), authkey=multiuser_key)
            self.done.set()
        except:
            self.done.clear()

# Send arguments to qpy-multiuser
def send_multiuser_arguments( option, arguments):
    M = try_multiuser_connection()
    M.start()
    M.done.wait( 5.0)
    if (not( M.done.is_set())):
        try:
            kill_conn = Listener(( multiuser_address, multiuser_port), authkey = multiuser_key)
            client = kill_conn.accept()
            kill_conn.close()
            return None
        except:
            return None
    conn = M.conn
    try:
        conn.send( (option, arguments))
        msg_back = conn.recv()
        conn.close()
    except:
        return None
    if (saveMessages):
        if (not(multiuser_messages) or ((option, arguments), msg_back) != multiuser_messages[-1][0]):
            multiuser_messages.append( [ ((option, arguments), msg_back), 1])
        elif (multiuser_messages[-1][1] < 10):
            multiuser_messages[-1][1] += 1
    return msg_back


# Check if the <command> sent by ssh to <address> return the <exp_out> message without errors
def is_ssh_working( address, command, exp_out):
    ssh = subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", address, command],
                             shell=False,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    std_outerr = ssh.communicate()
    if (verbose):
        print "is_ssh_working stdout:", repr(std_outerr[0])
        print "is_ssh_working stderr:", repr( std_outerr[1])
    error = False
    for i in ssh_stderr:
        if ('No route to host' in i or 'Connection refused' in i or 'Could not resolve hostname' in i):
            error = True
    success = False
    for i in ssh_stdout:
        if (exp_out in i):
            success = True
    return success and not( error)


def get_plural( word_s, stuff):
    """ Cosmetic function: get the plural

    @param word_s tuple or list with (singular_case, plural_case)
    @param stuff list of strings or a positive int
    @return tuple (correct_case, Predicate or listing)
    example: get_plural(("job","jobs"),0) =>("jobs", "No")
             get_plural(("job","jobs"),["queued", "running", "killed"]) =>("jobs", "queued, running and killed")
    """
    if (isinstance(stuff,list)):
        if (len(stuff)==0):
            return (word_s[1], 'No')
        elif (len(stuff)==1): 
            return (word_s[0], str(stuff[0]))
        elif (len(stuff) > 1):
            ret=", ".join(stuff[:-1])+" and "+stuff[-1]
            return (word_s[1], ret)
        else:
            #Ok, this case would be really weird
            raise Exception("get_plural: negative list length? " + str(word_s) + str(stuff) + "\n Contact the qpy-team.")
    elif (isinstance(stuff, int)):
        if (stuff == 0 ):
            return (word_s[1], 'No')
        elif (stuff == 1):
            return (word_s[0], str(stuff))
        elif (stuff > 1):
            return (word_s[1], str(stuff))
        else:
            raise Exception("get_plural: negative amount?" + str(word_s) + str(stuff) + "\n Contact the qpy-team.")
    else:
        raise Exception("get_plural:stuff neither int nor list?" + str(type(stuff)) + "\n Contact the qpy-team.")



# Allocate a node. Specific for ASES
def node_alloc():
    queue_script = 'qpy --alloc ' + str( max_node_time)
    command = 'salloc -N1 -t 21-0 -K -A ithkoehn ' + queue_script + ' &'
    salloc = subprocess.Popen(command,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)
    std_outerr = salloc.communicate()
    re_res = re.match('salloc: Granted job allocation (\d+)', std_outerr[1])
    if (re_res == None):
        re_res = re.match('salloc: Pending job allocation (\d+)', std_outerr[1])
    try:
        job_id = re_res.group(1)
    except:
        if (verbose):
            print 'Error in node allocation: ' + salloc_stderr
        return None
    command = 'squeue | grep ' + job_id
    node='a'
    while (node[0] != 'n'):
        squeue = subprocess.Popen(command,
                                  shell=True,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
        std_outerr = squeue.communicate()
        saloc_stdout = std_outerr[0].split()
        node = saloc_stdout[-1]
        sleep ( wait_initialization_time)

    while (not( is_ssh_working( node, 'source ~/.bash_profile; qpy --init', 'Success on qpy init!'))):
        if (verbose):
            print "Waiting node initialization: " + node
        sleep( wait_initialization_time)
    if (verbose):
        print 'Node ' + node + ' allocated.'
    return (job_id, node)


# Deallocate a node. Specific for ASES
def node_dealloc( node_id, alloc_id):
    term_script = 'source ~/.bash_profile; qpy --term'
    dealloc = subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", node_id, term_script],
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    std_outerr = dealloc.communicate()
    if (verbose):
        print "node_dealloc stdout:", repr( std_outerr[0])
        print "node_dealloc stderr:", repr( std_outerr[1])
    command = 'scancel ' + alloc_id
    dealloc = subprocess.call( command, shell=True)
    if (verbose):
        print 'Node ' + node_id + ' deallocated.'


job_status = ['queue',   # 0
              'running', # 1
              'done',    # 2
              'killed',  # 3
              'undone']  # 4

class JobParser(OptionParser):
    """An Option Parser that does not exit the program but just raises a ParseError

    NOTE:optparse is depreciated and the overwritten functions are somewhat mentioned in the documentation.
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

#---------------------------------------------------------------
# A Job
#
class JOB():
    
    def __init__( self, jobID, job_info):
        self.ID = jobID
        self.info = job_info
        self.n_cores = 1
        self.mem = 5.0
        self.node = None
        self.status = 0
        self.re_run = False
        self.set_parser()

        self.process = None

    def fmt_for_log( self):
        job_str = str( self.ID) + ' ' + str( self.status) + ' ' + str( self.n_cores) + ' ' + str( self.mem) + '\n'
        if (self.node == None):
            job_str += 'None\n'
        else:
            job_str += self.node.node_id + '\n'
        job_str += self.info[0] + '\n'
        job_str += self.info[1] + '\n'
        return job_str

    def fmt( self):
        job_str = job_fmt_pattern
        try:
            str_node = str(self.node.node_id)
        except:
            str_node = 'None'
        for pattern, info in (('%j', str( self.ID)),
                              ('%s', job_status[self.status]),
                              ('%c', self.info[0]),
                              ('%d', self.info[1]),
                              ('%n', str_node),
                              ('%N', str(self.n_cores))
                              ):
            job_str = job_str.replace( pattern, info)
        return job_str


    def set_parser(self):
        """creates a parser for the flags that can be set for a job"""
        parser=JobParser()
        parser.add_option("-n","--cores", dest="cores", help="set the number of cores", default="1")
        parser.add_option("-m","--mem","--memory",dest="memory",help="set the memory in GB", default="5")
        parser.disable_interspersed_args()
        self.parser=parser


    def run( self):
        def out_or_err_name( job, postfix):
            assert( postfix in ['.out', '.err'])
            return '{dir}/job_{id}{postfix}'.format(dir=job.info[1], id=str(job.ID), postfix=postfix )
        command =  'export QPY_JOB_ID=' + str( self.ID) + '; '
        command += 'export QPY_NODE=' + str( self.node.node_id) + '; '
        command += 'export QPY_N_CORES=' + str( self.n_cores) + '; '
        for sf in source_these_files:
           command += 'source ' + sf + '; '
        command += 'cd ' + self.info[1] + '; ' 
        command += self.info[0]
        command += ' > ' + out_or_err_name( self, '.out') + ' 2> ' + out_or_err_name( self, '.err')
        self.process = subprocess.Popen(["ssh", self.node.node_id, command],
                                        shell = False)
        self.status = 1

    def is_running( self):
        command = 'ssh ' + self.node.node_id + ' ps -fu ' + user
        check_ps = subprocess.Popen(command,
                                    shell = True,
                                    stdout = subprocess.PIPE,
                                    stderr = subprocess.PIPE)
        (std_out, std_err) = check_ps.communicate()
        re_res = re.search( 'export QPY_JOB_ID=' + str( self.ID) + ';', std_out)
        if (re_res):
            return True
        if (self.process != None):
            self.process.communicate()
        return False


    def _scanline(self,line,option_list):
        """ Parses the lines for qpy directives and options"""
        if (re.match( '#QPY', line)):
            option_found=False
            for attr,regexp in option_list:
                re_res = regexp.search(line )
                if ( re_res is not None ):
                    try:
                        self.__setattr__(attr, int(re_res.group(1)) )
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
        option_list=[("n_cores", re.compile('n_cores[= ]?(\d*)') ),
                     ("mem"   , re.compile('mem[= ]?(\d*)'    ) )
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
            self.mem = int(options.memory)
        except (AttributeError, TypeError), ex:
            raise ParseError("Something went wrong. please contact the qpy-team\n"+ex.message)
        except ValueError:
            raise ParseError("Please supply only full numbers for memory or number of cores")
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


    def parse_options( self):
        """ Parses the input and the submission script for options"""

        self.info[0] = self._parse_command_for_options( self.info[0])
        first_arg = self.info[0].split()[0]
        script_name = self._expand_script_name(first_arg)
        self._parse_file_for_options(script_name)

#-----------------------------------------------------------------------------------
    # return a boolean, indicating whether this job obbeys the dictionary pattern or not
    # empty pattern means that everthing is required
    def asked( self, pattern):
        req = True
        for k in pattern:
            if (k == 'status'):
                req = job_status[self.status] in pattern[k]
        return req


#------------------------------------------------
# Store information about the jobs
class job_collection():
    
    def __init__( self):
        self.all = []
        self.lock_all = threading.RLock()

        self.queue = []
        self.queue_size = 0
        self.lock_queue = threading.RLock()

        self.running = []
        self.lock_running = threading.RLock()

        self.done = []
        self.lock_done = threading.RLock()

        self.killed = []
        self.lock_killed = threading.RLock()

        self.undone = []
        self.lock_undone = threading.RLock()

        self.Q = deque()
        self.lock_Q = threading.RLock()

        self.lock_file = threading.RLock()

    def write_all_jobs( self):
        self.lock_file.acquire()
        self.lock_all.acquire()
        f = open( all_jobs_file, 'w')
        for job in self.all:
            f.write( job.fmt_for_log())
        f.close()
        self.lock_file.release()
        self.lock_all.release()

    def initialize_old_jobs( self, sub_ctrl):
        self.lock_all.acquire()
        self.lock_queue.acquire()
        self.lock_running.acquire()
        self.lock_done.acquire()
        self.lock_killed.acquire()
        self.lock_undone.acquire()
        self.lock_Q.acquire()
        if (os.path.isfile( all_jobs_file)):
            with open( all_jobs_file, 'r') as f:
                i = 0
                for line in f:
                    i += 1
                    if (i%4 == 1):
                        (new_id, new_status, new_n_cores, new_mem) = line.split()
                    elif (i%4 == 2):
                        new_node = line.strip()
                    elif (i%4 == 3):
                        new_command = line.strip()
                    else:
                        new_wd = line.strip()
                        new_job = JOB( int( new_id), [new_command, new_wd])
                        new_job.n_cores = int( new_n_cores)
                        new_job.mem = float( new_mem)
                        if (new_node == 'None'):
                            new_job.node = None
                        else:
                            node_found = False
                            for node in sub_ctrl.node_list:
                                if (new_node == node.node_id):
                                    new_job.node = node
                                    node_found = True
                                    break
                            if (not( node_found)):
                                new_job.node = NODE( -1, new_node)
                                new_job.node.start()
                                sub_ctrl.node_list.append( new_job.node)
                        new_job.status = int( new_status)
                        self.all.append( new_job)
                        if (new_job.status == 0):
                            self.queue.append( new_job)
                            self.Q.appendleft( new_job)
                            self.queue_size += 1
                        elif (new_job.status == 1):
                            new_job.node.n_jobs += new_job.n_cores
                            self.running.append( new_job)
                            new_job.re_run = True
                        elif (new_job.status == 2):
                            self.done.append( new_job)
                        elif (new_job.status == 3):
                            self.killed.append( new_job)
                        elif (new_job.status == 4):
                            self.undone.append( new_job)
        self.lock_all.release()
        self.lock_queue.release()
        self.lock_running.release()
        self.lock_done.release()
        self.lock_killed.release()
        self.lock_undone.release()
        self.lock_Q.release()


# A node.
#
class NODE( threading.Thread):

    def __init__( self, max_jobs, *args):
        threading.Thread.__init__( self)

        if (dyn_nodes):
            new_node = node_alloc()
        else:
            if ( not(args)):
                sys.exit( 'Internal error: no arguments in node allocation')
            new_node = ( None, args[0])
        try:
            self.alloc_id = new_node[0]
            self.node_id = new_node[1]
        except:
            self.alloc_id = None
            self.node_id = None

        self.init_time = time()
        self.max_jobs = max_jobs
        self.n_jobs = 0
        self.queue = Queue()
        
    def run( self):

        while True:
            job = self.queue.get()
            if ( isinstance( job, str)):
                if ( job == 'kill'):
                    break

            if (verbose):
                print 'Node ' + str( self.node_id) + ':' + str( job.ID)

            job.run()
            self.n_jobs += job.n_cores

        if ( self.alloc_id != None):
            node_dealloc( self.node_id, self.alloc_id)
        
        if (verbose):
            print 'Node ' + str( self.node_id) + ' done.'


# Transform the multiuser status message in a more readable way
def analise_multiuser_status( info, check_n, check_u):
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


# return string with information on required jobs, defined by pattern
def check_jobs( jobs, pattern):
    asked_jobs = []
    jobs.lock_all.acquire()
    for job in jobs.all:
        if (job.asked( pattern)):
            asked_jobs.append( job)
    jobs.lock_all.release()

    req_jobs = ''
    for job in asked_jobs:
        j_str = job.fmt()
        req_jobs += j_str

    return req_jobs




#------------------------------------------------
# Tasks threads

#------------------------------------------------
# Check connection with multiuser
#
class CHECK_MULTIUSER( threading.Thread):
    def __init__( self, jobs, multiuser_alive):
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive

        threading.Thread.__init__( self)
        self.finish = threading.Event()

    def run( self):

        waiting_time = 0
        while not self.finish.is_set():
            if (waiting_time < 1):
                msg_back = send_multiuser_arguments( MULTIUSER_USER, [user])
                if (msg_back == None):
                    self.multiuser_alive.clear()
                else:
                    self.multiuser_alive.set()
                    if ( isinstance( msg_back[1], str) and msg_back[1] != 'User exists.'):
                        multiuser_cur_jobs = []
                        self.jobs.lock_running.acquire()
                        for job in self.jobs.running:
                            multiuser_cur_jobs.append( [job.ID, job.node.node_id, job.n_cores])
                        self.jobs.lock_running.release()
                        msg_back = send_multiuser_arguments( MULTIUSER_USER, (user, multiuser_cur_jobs))
                if (verbose):
                    print "Checking multiuser: ", msg_back
                if (self.multiuser_alive.is_set()):
                    waiting_time = conn_multiuser_at
                else:
                    waiting_time = conn_multiuser_at_not_working

            else:
                waiting_time -= 1

            sleep( 1)


#------------------------------------------------
# Check if the jobs are still running
#
class CHECK_RUN( threading.Thread):
    def __init__( self, jobs, multiuser_alive):
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive

        threading.Thread.__init__( self)
        self.finish = threading.Event()

    def run( self):

        while not self.finish.is_set():
            i = 0
            jobs_modification = False
            self.jobs.lock_running.acquire()
            jobs_to_check = list( self.jobs.running)
            self.jobs.lock_running.release()
            for job in jobs_to_check:
                if (not job.is_running() and job.status == 1):
                    job.status = 2
                    jobs_modification = True
                    self.jobs.lock_running.acquire()
                    self.jobs.lock_done.acquire()
                    self.jobs.running.remove( job)
                    self.jobs.done.append( job)
                    self.jobs.lock_running.release()
                    self.jobs.lock_done.release()
                    self.skip_job_sub = 0
                    job.node.n_jobs -= job.n_cores
                    if (multiuser and self.multiuser_alive.is_set()):
                        multiuser_args = (user, job.ID, self.jobs.queue_size)
                        msg_back = send_multiuser_arguments( MULTIUSER_REMOVE_JOB, multiuser_args)
                        if (msg_back == None):
                            self.multiuser_alive.clear()
                        if (verbose):
                            print 'Multiuser message (removing a job): ', msg_back
                else:
                    i += 1
            sleep ( sleep_time_check_run)
            if (jobs_modification):
                self.jobs.write_all_jobs()
                jobs_modification = False



#------------------------------------------------
# Kill jobs
#
class JOBS_KILLER( threading.Thread):
    def __init__( self, jobs, multiuser_alive):
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive

        threading.Thread.__init__( self)
        self.queue = Queue()

    def run( self):

        while True:
            job = self.queue.get()

            if ( isinstance( job, str)):
                if ( job == 'kill'):
                    break

            if (verbose):
                print 'Killing: ' + str( job.ID) + ' on node ' + job.node.node_id

            kill_command = 'source ~/.bash_profile; qpy --jobkill ' + str( job.ID)
            kill_p = subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no",
                                       job.node.node_id, kill_command],
                                      shell=False,
                                      stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE)
            std_outerr = kill_p.communicate()
            if (job.process != None):
                job.process.communicate()
            job.status = 3
            job.node.n_jobs -= job.n_cores
            self.jobs.lock_running.acquire()
            self.jobs.lock_killed.acquire()
            self.jobs.running.remove( job)
            self.jobs.killed.append( job)
            self.jobs.lock_running.release()
            self.jobs.lock_killed.release()
            self.jobs.write_all_jobs()

                        
            if (verbose):
                print "Killing job " + str( job.ID) + ' on node ' + job.node.node_id
                print "job_kill stdout: ", repr( std_outerr[0])
                print "job_kill stderr: ", repr( std_outerr[1])


            if (self.multiuser_alive.is_set()):
                multiuser_args = (user, job.ID, self.jobs.queue_size)
                msg_back = send_multiuser_arguments( MULTIUSER_REMOVE_JOB, multiuser_args)
                if (msg_back == None):
                    self.multiuser_alive.clear()
            

#------------------------------------------------
# Control job subimission
#
class SUB_CTRL( threading.Thread):
    def __init__( self, jobs, multiuser_alive):
        self.jobs = jobs
        self.multiuser_alive = multiuser_alive

        threading.Thread.__init__( self)
        self.finish = threading.Event()

        self.node_list = []
        self.max_nodes = max_nodes_default
        if (multiuser):
            self.max_jobs_default = -1
        else:
            self.max_jobs_default = 20
        self.skip_job_sub = 0

    def write_nodes( self):
        with  open( cur_nodes_file, 'w') as f:
            for node in self.node_list:
                f.write( node.node_id + '\n')

    def write_new_known_node( self, new_node):
        with open( known_nodes_file, 'r') as f:
            known_nodes = []
            for node in f:
                known_nodes.append( node.strip())

        if (not( new_node in known_nodes)):
            with open( known_nodes_file, 'a') as f:
                f.write( new_node + '\n')
            
    def run( self):
        if (not( multiuser)):
            self.write_nodes()
        n_time_multiuser = 0
        self.jobs.initialize_old_jobs( self)
        jobs_modification = False
        while not self.finish.is_set():

            # Dealloc the non-used node
            if (dyn_nodes):
                cur_time = time()
                for node in self.node_list:
                    if (not( node.n_jobs) and (cur_time - node.init_time) > min_working_time):
                        node.queue.put( 'kill')
                        self.node_list.remove( node)

            # Send a job, if there is space
            if (self.skip_job_sub <= 0):
                self.jobs.lock_Q.acquire()
                queue_has_jobs = len( self.jobs.Q) != 0
                if (queue_has_jobs):
                    next_jobID = self.jobs.Q[-1].ID
                    next_Ncores = self.jobs.Q[-1].n_cores
                    next_mem = self.jobs.Q[-1].mem
                    best_node = None
                    if (multiuser and self.multiuser_alive.is_set()):
                        multiuser_args = (user, next_jobID, next_Ncores, next_mem, self.jobs.queue_size)
                        msg_back = send_multiuser_arguments( MULTIUSER_REQ_CORE, multiuser_args)
                        if (msg_back == None):
                            self.multiuser_alive.clear()
                            continue
                        if (verbose):
                            print 'Multiuser message (sending job): ', msg_back
                        if (msg_back[0] == 0):
                            node_found = False
                            for node in self.node_list:
                                if (msg_back[1] == node.node_id):
                                    best_node = node
                                    node_found = True
                                    break
                            if (not( node_found)):
                                best_node = NODE( -1, msg_back[1])
                                best_node.start()
                                self.node_list.append( best_node)
                        else:
                            self.skip_job_sub = 300
                    else:
                        best_free = 0
                        cur_time = time()
                        for node in self.node_list:
                            free = node.max_jobs - node.n_jobs
                            if (free > best_free):
                                if (not( dyn_nodes) or (cur_time - node.init_time) < max_working_time):
                                    best_node = node
                                    best_free = free
                        if (best_node == None and len( self.node_list) < self.max_nodes):
                            best_node = NODE( self.max_jobs_default)
                            if (best_node.node_id):
                                best_node.start()
                                sub_ctrl.node_list.append( best_node)
                            else:
                                best_node.start()
                                best_node.queue.put( 'kill')
                                best_node = None
                                self.skip_job_sub = 180
                    if (best_node != None):
                        if (verbose):
                            print "submission_control: putting job"
                        job = self.jobs.Q.pop()
                        job.node = best_node
                        best_node.queue.put( job)
                        self.jobs.lock_running.acquire()
                        self.jobs.lock_queue.acquire()
                        self.jobs.queue.remove( job)
                        self.jobs.queue_size -= 1
                        self.jobs.running.append( job)
                        self.jobs.lock_running.release()
                        self.jobs.lock_queue.release()
                        jobs_modification = True
                self.jobs.lock_Q.release()

            else:
                self.skip_job_sub -= 1
                if (verbose):
                    print "Skipping job submission: " + str( self.skip_job_sub)

            sleep( sleep_time_sub_ctrl)
            if (jobs_modification):
                self.jobs.write_all_jobs()
                jobs_modification = False


#------------------------------------------------
# Handle the user messages sent from qpy
#
# message from client must be:
#   (job_type, arguments)
# where:
#   job_type is
#       JOBTYPE_SUB     - submit a job                           (sub)
#       JOBTYPE_CHECK   - check the jobs                         (check)
#       JOBTYPE_KILL    - kill a job                             (kill)
#       JOBTYPE_FINISH  - kill the master                        (finish)
#       JOBTYPE_NODES   - change max_nodes or add/remove a node  (nodes)
#       JOBTYPE_MAXJOBS - change max_jobs                        (njobs)
#       JOBTYPE_CONFIG  - show config                            (config)
#       JOBTYPE_CLEAN   - clean finished jobs                    (clean)
#
#   the arguments are option dependent. See below
#
def handle_qpy( sub_ctrl, check_run, check_multiuser, jobs_killer, jobs, jobId):

    while True:
        random.seed()
        port = random.randint( 10000, 20000 )
        try:
            server_master = Listener(( "localhost", port), authkey = 'qwerty')
            break
        except:
            pass
    f = open( port_file, 'w', 0)
    f.write( str( port))
    f.close()
    while True:
        client_master = server_master.accept()
        (job_type, arguments) = client_master.recv()
        if (verbose):
            print "Received: " + str(job_type) + " -> " + str(arguments)
            
        # Send a job
        # arguments = the job info (see JOB.info)
        if (job_type == JOBTYPE_SUB):
            new_job = JOB( jobId, arguments)
            try:
                new_job.parse_options()
                jobs.lock_all.acquire()
                jobs.lock_queue.acquire()
                jobs.all.append( new_job)
                jobs.queue.append( new_job)
                jobs.queue_size += 1
                jobs.lock_all.release()
                jobs.lock_queue.release()
                jobs.lock_Q.acquire()
                jobs.Q.appendleft( new_job)
                jobs.lock_Q.release()
                client_master.send( 'Job ' + str(jobId) + ' received.\n')
                jobId += 1
                with open( jobID_file, 'w') as f:
                    f.write( str( jobId))
            except HelpException,ex :
                client_master.send( ex.message)
            except ParseError, ex:
                client_master.send( 'Job  rejected.\n'+ex.message+'\n')
                
                

        # Check jobs
        # arguments: a dictionary, indicating patterns (see JOB.asked)
        elif (job_type == JOBTYPE_CHECK):
            client_master.send( check_jobs( jobs, arguments))

        # Kill a job
        # arguments = a list of jobIDs and status (all, queue, running)
        elif (job_type == JOBTYPE_KILL):

            kill_q = (( 'all' in arguments) or ('queue' in arguments))
            kill_r = (( 'all' in arguments) or ('running' in arguments))

            for st in ['all', 'queue', 'running']:
                while (st in arguments):
                    arguments.remove( st)

            jobs.lock_queue.acquire()
            jobs.lock_undone.acquire()
            jobs.lock_Q.acquire()
            n_kill_q = 0
            to_remove = []
            for job in jobs.queue:
                if (job.ID in arguments or kill_q):
                    to_remove.append( job)
            for job in to_remove:
                job.status = 4
                jobs.queue.remove( job)
                jobs.queue_size -= 1
                jobs.undone.append( job)
                jobs.Q.remove( job)
                n_kill_q += 1
            jobs.lock_queue.release()
            jobs.lock_undone.release()
            jobs.lock_Q.release()

            n_kill_r = 0
            to_remove = []
            jobs.lock_running.acquire()
            for job in jobs.running:
                if (job.ID in arguments or kill_r):
                    to_remove.append( job)
            jobs.lock_running.release()

            for job in to_remove:
                jobs_killer.queue.put( job)
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
            client_master.send( msg)

        # Finish the master execution
        # argumets: no arguments
        if (job_type == JOBTYPE_FINISH):
            client_master.send( 'Stopping qpy-master driver.\n')
            for node in sub_ctrl.node_list:
                node.queue.put( 'kill')
            sub_ctrl.finish.set()
            check_run.finish.set()
            jobs_killer.queue.put( 'kill')
            if (multiuser):
                check_multiuser.finish.set()
            break

        # Change maximum number of nodes or add/remove nodes
        # arguments: empty list, to show the nodes
        #        or  an integer, to change the maximum number of nodes
        #        or  a list: [<add/remove/forceRemove>, <'all'>, <node_1>, <node_2>, ]
        elif (job_type == JOBTYPE_NODES):
            msg = ''
            if (arguments and not( multiuser)):
                if (dyn_nodes):
                    try:
                        sub_ctrl.max_nodes = int( arguments)
                        msg = 'Maximum number of nodes changed to ' + str( arguments) + '.\n'
                    except:
                        msg = 'This should be an integer: ' + str( arguments) + '.\n'
                else:
                    if (arguments[0] == 'remove' or arguments[0] == 'forceRemove'):
                        nodes_OK = []
                        nodes_BAD = []
                        arguments.remove( arguments[0])
                        remove_all = 'all' in arguments
                        while ('all' in arguments):
                            arguments.remove( 'all')

                        i_n = 0
                        while (i_n < len( sub_ctrl.node_list)):
                            node = sub_ctrl.node_list[i_n]
                            removed = False
                            if (node.node_id in arguments or remove_all):
                                if (node.n_jobs == 0 or arguments[0] == 'forceRemove'):
                                    if (node.node_id) in arguments:
                                        arguments.remove( node.node_id)
                                    nodes_OK.append( node.node_id)
                                    node.queue.put( 'kill')
                                    sub_ctrl.node_list.remove( node)
                                    removed = True
                                elif (node.n_jobs > 0):
                                    nodes_BAD.append( node.node_id)
                                    
                            if (not( removed)):
                                i_n += 1

                        msg = ''
                        if (nodes_OK):
                            plural = get_plural( ('Node', 'Nodes'), nodes_OK)
                            msg += plural[0] + ' ' + plural[1] + ' removed.\n'
                        if (nodes_BAD):
                            plural = get_plural( (('Node', 'has'), ('Nodes', 'have')), nodes_BAD)
                            msg += plural[0][0] + ' ' + plural[1] + ' ' + plural[0][1] + ' running jobs. Use forceRemove.\n'
                        if (arguments):
                            plural = get_plural( ('Node', 'Nodes'), arguments)
                            msg += plural[0] + ' ' + plural[1] + ' not found.\n'

                    elif (arguments[0] == 'add'):
                        nodes_OK = []
                        nodes_BAD = []
                        arguments.remove( arguments[0])
                        i = 0
                        while (i < len( arguments)):
                            n_name = arguments[i]
                            is_new_node = True
                            for node in sub_ctrl.node_list:
                                if (node.node_id == n_name):
                                    is_new_node = False
                                    break
                            if (is_new_node):
                                arguments.remove( n_name)
                                if (is_ssh_working( n_name, 'hostname', n_name)):
                                    new_node = NODE( sub_ctrl.max_jobs_default, n_name)
                                    new_node.start()
                                    sub_ctrl.node_list.append( new_node)
                                    nodes_OK.append( n_name)
                                    sub_ctrl.write_new_known_node( n_name)
                                else:
                                    nodes_BAD.append( n_name)
                            else:
                                i += 1

                        msg = ''
                        if (nodes_OK):
                            plural = get_plural( ('Node', 'Nodes'), nodes_OK)
                            msg += plural[0] + ' ' + plural[1] + ' added.\n'
                        if (arguments):
                            plural = get_plural( ('Node', 'Nodes'), arguments)
                            msg += plural[0] + ' ' + plural[1] + ' already added.\n'
                        if (nodes_BAD):
                            plural = get_plural( ('node', 'nodes'), nodes_BAD)
                            msg += 'Connection on ' + plural[0]+ ' ' + plural[1] + ' seems not to work.\n'

                    sub_ctrl.write_nodes()
                    
            else:
                if (multiuser):
                    msg = 'Nodes under multiuser control:\n'
                    msg_back = send_multiuser_arguments( MULTIUSER_STATUS, ())
                    if (msg_back == None):
                        msg += 'qpy-multiuser seems not to be running. Contact the qpy-team.\n'
                    else:
                        msg += msg_back[1] + '\n'
                    for node in sub_ctrl.node_list:
                        msg = msg.replace(node.node_id + ': ', node.node_id + ': ' + str( node.n_jobs) + '/')
                else:
                    for node in sub_ctrl.node_list:
                        msg += node.node_id + ': ' + str( node.n_jobs) + '/' + str( node.max_jobs) + '\n'

            client_master.send( msg)



        # Show status
        # No arguments (yet)
        elif (job_type == JOBTYPE_STATUS):
            if (multiuser):
                msg_back = send_multiuser_arguments( MULTIUSER_STATUS, ())
                if (msg_back == None):
                    msg = 'qpy-multiuser seems not to be running. Contact the qpy-team.\n'
                else:
                    msg = analise_multiuser_status( msg_back[1], True, True)
            else:
                msg = 'qpy is not under multiuser contrl.\n'

            client_master.send( msg)


        # Change maximum number of jobs
        # arguments: a list: [<new_maxJob>, <node_1>, <node_2>, ...]. Change all nodes if no nodes were given
        elif (job_type == JOBTYPE_MAXJOBS):
            if (multiuser):
                msg = 'Nodes under multiuser control. Try "qpy status"\n'
            else:
                new_maxJob = arguments[0]
                if (len( arguments) == 1):
                    sub_ctrl.max_jobs_default = new_maxJob
                    for node in sub_ctrl.node_list:
                        node.max_jobs = new_maxJob
                    msg = 'Maximum number of jobs changed to ' + str( new_maxJob) + '.\n'
                else:
                    default_changed = False
                    if ('maxJob_default' in arguments[1:]):
                        sub_ctrl.max_jobs_default = new_maxJob
                        default_changed = True
                    while ('maxJob_default' in arguments):
                        arguments.remove( 'maxJob_default')
                    nodes_OK = []
                    nodes_BAD = []
                    for node_name in arguments[1:]:
                        not_changed = True
                        for node in sub_ctrl.node_list:
                            if (node.node_id == node_name):
                                node.max_jobs = new_maxJob
                                not_changed = False
                                nodes_OK.append( node_name)
                                break
                        if (not_changed):
                            nodes_BAD.append( node_name)

                    msg = ''
                    if (default_changed):
                        msg += 'Defaut value for maximum number of jobs changed to ' + str( new_maxJob) + '.\n'
                    if (nodes_OK):
                        plural = get_plural( ('node', 'nodes'), nodes_OK)
                        msg += 'Maximum number of jobs for ' + plural[0] + ' ' + plural[1] + ' changed to ' + str( new_maxJob) + '.\n'
                    if (nodes_BAD):
                        plural = get_plural( ('Node', 'Nodes'), nodes_BAD)
                        msg += plural[0]+ ' ' + plural[1] + ' not found.\n'

            client_master.send( msg)

        # Show current configuration
        # arguments: optionally, a pair to change the configuration: (<key>, <value>)
        elif (job_type == JOBTYPE_CONFIG):
            global job_fmt_pattern
            if (arguments):
                k = arguments[0]
                v = arguments[1]
                if (k == 'checkFMT'):
                    if (v == 'default'):
                        job_fmt_pattern = job_fmt_pattern_def
                        msg = 'Check pattern restored to the default value: ' + repr( job_fmt_pattern) + '.\n'
                    else:
                        job_fmt_pattern = v.decode('string_escape')
                        msg = 'Check pattern modified to ' + repr( job_fmt_pattern) + '.\n'
                else:
                    msg = 'Unkown key: ' + k + '\n'
            else:
                msg = 'Check pattern: ' + repr( job_fmt_pattern) + '\n'
                if (not( multiuser)):
                    msg += 'max_jobs         = ' + str( sub_ctrl.max_jobs_default) + '\n'
                if (dyn_nodes):
                    msg += 'using dynamic node allocation\n'
                    msg += 'max_nodes        = ' + str( sub_ctrl.max_nodes) + '\n'
                    msg += 'max_node_time    = ' + str( max_node_time) + '\n'
                    msg += 'min_working_time = ' + str( min_working_time) + '\n'
                    msg += 'max_working_time = ' + str( max_working_time) + '\n'
                if (multiuser):
                    msg += 'Nodes under multiuser control.\n'
                    if (saveMessages):
                        msg += 'Last multiuser messages:\n'
                        for msg in multiuser_messages:
                            msg += ' ' + str( msg[0][0][0]) + ', ' + str( msg[0][0][1]) + ': ' + str( msg[0][1][0]) + ', ' + repr( msg[0][1][1])
                            if (msg[1] > 1):
                                msg += ' (' + str(msg[1]) + 'x)'
                            msg += '\n'

            client_master.send( msg)

        # Clean finished jobs
        # arguments = a list of jobIDs and status (all, done, killed, undone)
        elif (job_type == JOBTYPE_CLEAN):
            n_jobs = 0
            for i in arguments:
                arg_is_id = isinstance( i, int)
                jobs.lock_all.acquire()
                jobs.lock_done.acquire()
                jobs.lock_killed.acquire()
                jobs.lock_undone.acquire()
                ij = 0
                while (ij < len( jobs.all)):
                    job = jobs.all[ij]
                    remove = False
                    if (job.status > 1):
                        remove = arg_is_id and i == job.ID
                        remove = remove or not( arg_is_id) and i == 'all'
                        remove = remove or not( arg_is_id) and i == job_status[job.status]
                        if (remove):
                            if (job.status == 2):
                                jobs.done.remove( job)
                            elif (job.status == 3):
                                jobs.killed.remove( job)
                            elif (job.status == 4):
                                jobs.undone.remove( job)
                            jobs.all.remove( job)
                            n_jobs += 1
                    if (not( remove)):
                        ij += 1
                jobs.lock_all.release()
                jobs.lock_done.release()
                jobs.lock_killed.release()
                jobs.lock_undone.release()

            if (n_jobs):
                jobs.write_all_jobs()
                plural = get_plural( ('job', 'jobs'), n_jobs)
                msg = plural[1] + ' finished ' + plural[0] + ' removed.\n'
            else:
                msg = 'Nothing to do: required jobs not found.\n'

            client_master.send( msg)

        else:
            client_master.send( 'Unknown option: ' + str( job_type) + '\n')


#------------------------------
if (ini_job_ID == None):
    try:
        with open( jobID_file, 'r') as f:
            ini_job_ID = int( f.read())
    except:
        ini_job_ID = 1

jobs = job_collection()

multiuser_alive = threading.Event()

if (multiuser):
    check_multiuser = CHECK_MULTIUSER( jobs, multiuser_alive)
    check_multiuser.start()
    multiuser_alive.set()
else:
    multiuser_alive.clear()
    check_multiuser = None

check_run = CHECK_RUN( jobs, multiuser_alive)
check_run.start()

jobs_killer = JOBS_KILLER( jobs, multiuser_alive)
jobs_killer.start()

sub_ctrl = SUB_CTRL( jobs, multiuser_alive)
sub_ctrl.start()

handle_qpy( sub_ctrl, check_run, check_multiuser, jobs_killer, jobs, ini_job_ID)

os.remove( port_file)

if (verbose):
    print "qpy-master main thread done!"
