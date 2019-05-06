"""Common tasks and variables for qpy

"""

from multiprocessing import connection
import termcolor.termcolor as termcolour
import threading
import random
import os
import re
import sys
import subprocess
from time import sleep
try:
    import paramiko
    is_paramiko = True
    # Check version??
except:
    is_paramiko = False


import time
import logging
import logging.handlers
import traceback

QPY_SOURCE_DIR = os.path.dirname( os.path.abspath( __file__)) + '/'
TEST_RUN = os.path.isfile( QPY_SOURCE_DIR + 'test_dir')


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



MULTIUSER_NODES          = 1
MULTIUSER_DISTRIBUTE     = 2
MULTIUSER_STATUS         = 3
MULTIUSER_SHOW_VARIABLES = 4
MULTIUSER_FINISH         = 5
MULTIUSER_START          = 6
MULTIUSER_SAVE_MESSAGES  = 7
MULTIUSER_TUTORIAL       = 8
MULTIUSER_USER           = -1
MULTIUSER_REQ_CORE       = -2
MULTIUSER_REMOVE_JOB     = -3

JOBTYPE_SUB       = 1
JOBTYPE_CHECK     = 2
JOBTYPE_KILL      = 3
JOBTYPE_FINISH    = 4
JOBTYPE_NODES     = 5 # Obsolete
JOBTYPE_MAXJOBS   = 6 # Obsolete
JOBTYPE_CONFIG    = 7
JOBTYPE_CLEAN     = 8
JOBTYPE_TUTORIAL  = 9
JOBTYPE_STATUS    = 10
JOBTYPE_RESTART   = 11
JOBTYPE_CTRLQUEUE = 12
JOBTYPE_NOTE      = 13

FROM_MULTI_CUR_JOBS = 1
FROM_MULTI_FINISH = 2

JOB_ST_QUEUE   = 0
JOB_ST_RUNNING = 1
JOB_ST_DONE    = 2
JOB_ST_KILLED  = 3
JOB_ST_UNDONE  = 4

JOB_STATUS = ['queue',   # 0
              'running', # 1
              'done',    # 2
              'killed',  # 3
              'undone']  # 4

POSSIBLE_COLOURS = ['yellow',
                    'blue',
                    'green',
                    'red',
                    'grey',
                    'magenta',
                    'cyan',
                    'white']

JOB_FMT_PATTERN_DEF = '%j (%s):%c (on %n; wd: %d)\n'

KEYWORDS={'sub':       (JOBTYPE_SUB       , 'Submits a job. Arguments: the job command'),
          'check':     (JOBTYPE_CHECK     , 'Checks the jobs. Arguments: the desired job status'),
          'kill':      (JOBTYPE_KILL      , 'Kills jobs. Argument: the jobs id'),
          'finish':    (JOBTYPE_FINISH    , 'Finishes the master execution. No arguments'),
          'config':    (JOBTYPE_CONFIG    , 'Shows the current configuration. No arguments'),
          'clean':     (JOBTYPE_CLEAN     , 'Removes finished jobs from the list. Arguments: the jobs id'),
          'tutorial':  (JOBTYPE_TUTORIAL  , 'Opens the qpy tutorial. Arguments: optional: a pattern'),
          'status':    (JOBTYPE_STATUS    , 'Shows current status. No arguments'),
          'restart':   (JOBTYPE_RESTART   , 'Restarts qpy-master. No arguments'),
          'ctrlQueue': (JOBTYPE_CTRLQUEUE , 'Fine control over the queue. Arguments: see tutorial'),
          'notes':     (JOBTYPE_NOTE      , 'Adds and reads notes. Arguments: ID and the note')
          }

MULTIUSER_KEYWORDS={'nodes':        (MULTIUSER_NODES,          'Realoads nodes file. No arguments'),
                    'distribute':   (MULTIUSER_DISTRIBUTE,     'Distributes cores: No arguments'),
                    'status' :      (MULTIUSER_STATUS,         'Shows status. No arguments'),
                    'variables' :   (MULTIUSER_SHOW_VARIABLES, 'Shows variables. No arguments'),
                    'start':        (MULTIUSER_START,          'Starts multiuser execution. No arguments'),
                    'finish':       (MULTIUSER_FINISH,         'Finishes the multiuser execution. No arguments'),
                    'saveMessages': (MULTIUSER_SAVE_MESSAGES,  'Saves messages for debugging. Arguments: true or false'),
                    'tutorial':     (MULTIUSER_TUTORIAL,       'Opens the qpy administrator tutorial. Arguments: optional: a pattern'),
                    '__user':       (MULTIUSER_USER,           'Adds user. Arguments: user_name'),
                    '__req_core':   (MULTIUSER_REQ_CORE,       'Requires a core: Arguments: user_name, jobID, n_cores, mem, queue_size'),
                    '__remove_job': (MULTIUSER_REMOVE_JOB,     'Removes a job: Arguments: user_name, job_ID, queue_size'),
                    }

PORT_MIN_MULTI  = 10000
PORT_MAX_MULTI  = 20000
PORT_MIN_MASTER = 20001
PORT_MAX_MASTER = 60000



def configure_root_logger(base_file,level):
    """Set up a logger."""
    rootLogger = logging.getLogger()
    rootLogger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# note that the TimedRotatingFileHandler adds time info to the filenames e.g.:
# mylog.log-> mylog.log.2017-12-...
# change every day at midnight; one week should be enough
    ch = logging.handlers.TimedRotatingFileHandler(
        filename = str(base_file),
        when='midnight',
        interval=1,
        backupCount = 7,
        delay=False
        )
    ch.setFormatter(formatter)
    rootLogger.addHandler(ch)
    #ch2 = logging.StreamHandler(sys.stdout)
    #ch2.setFormatter(formatter)
    #rootLogger.addHandler(ch2)
    return rootLogger

def log_exception(msg):
    """Return a string with 'msg' and the exception traceback."""
    exc_type, exc_value, exc_traceback = sys.exc_info()
    log_message = msg + ':\n'
    log_message += "  Value: " + str(exc_value) + "\n"
    log_message += "  Type:  " + str(exc_type) + "\n"
    log_message += "  Traceback:\n"
    for tb in traceback.extract_tb(exc_traceback):
        log_message + "    in {0}, line {1:d}: {2}, {3}\n".format(tb[0], tb[1], str(tb[2]), tb[3])
    return log_message


def get_all_children(x, parent_of):
    """Get all children and further generations
    
    Arguments:
    x (str)            The element whose children we are looking for
    parent_of (dict)   Gives the parents of each element
    
    Return:
    A list with all chidren and further generations of
    x, according to the family tree defined by parent_of
    """
    cur_child = []
    for p in parent_of:
        if (parent_of[p] == x):
            cur_child.append(p)
    all_children = []
    for c in cur_child:
        all_children = get_all_children( c, parent_of)
    all_children.extend( cur_child)
    return all_children


def string_to_int_list(x):
    """Parse a string to a list of int
    
    Arguments:
    x (str)    A string to be parsed to integers
    
    Behaviour:
    '1,2,3'    -> [1,2,3]
    '1-3'      -> [1,2,3]
    '1-3,6-10' -> [1,2,3,6,7,8,9,10]
    
    Return:
    a list of integers.
    """
    res = []
    for entry in x.split(',') :
        range_ = [ int(num) for num in entry.split('-')] # raises ValueError on 3-
        if (len(range_) not in [1,2] ):
            raise IndexError("No multivalue ranges")
        res.extend( range( range_[0], range_[-1]+1)) #works correctly for len(range_ ) == 1
    return res


def kill_master_instances(user, address, qpy_master_command):
    """Kill all qpy-master instances from this user.
    
    Behaviour:
    It does it only from the same source directory.
    """
    ps_stdout = node_exec(address, ["ps", "-fu", user], get_outerr = True, mode='popen')
    ps_stdout = ps_stdout[0].split( '\n')
    for l in ps_stdout:
        if re.search(qpy_master_command+'$', l) != None:
            pid = l.split()[1]
            node_exec(address, "kill " + pid, get_outerr = False, mode='popen')
            sys.stdout.write( 'Killing older qpy-master instance: ' + pid + '\n')


def start_master_driver(user, address, qpy_master_command):
    """Start qpy-master and exit.
    
    TODO:
    Only main programs should exit.
    """
    sys.stdout.write("Starting qpy-master driver... It takes a few seconds, be patient.\n")
    sleep(5.)
    kill_master_instances(user, address, qpy_master_command)
    node_exec(address, qpy_master_command + ' > /dev/null 2> /dev/null', get_outerr = False, mode='popen')
    exit()


def establish_Listener_connection(address, port_min, port_max, port=None, conn_key=None):
    """Create a Listener connection

    Arguments:
    address (str)     The address of the connection
    port_min (int)    The minimum value that a randomly generated
                      port can have
    port_max (int)    The maximum value that a randomly generated
                      port can have
    port (int)        (optional, default = None) The port of the
                      connection. If None, randomly generates one
    conn_key (uint)   (optional, default = None) The key of the
                      connection. If None, randomly generates one
    
    Return:
    The tuple (List_master, port, key)
    where List_master is the Listener object
    port is the port for the connection.
    key is the key to the connection.
    """
    if conn_key == None:
        random.seed()
        conn_key = os.urandom(30)
    if port == None:
        while True:
            port = random.randint(port_min, port_max)
            try:
                List_master = connection.Listener((address, port), authkey = conn_key)
                break
            except:
                pass
    else:
        try:
            List_master = connection.Listener((address, port), authkey = conn_key)
        except:
            List_master = None
    return (List_master, port, conn_key)


def write_conn_files(f_name, address, port, conn_key):
    """Write the connection information to files."""
    f = open(f_name+'_address', 'w')
    f.write(address)
    f.close()
    f = open(f_name+'_port', 'w')
    f.write(str(port))
    f.close()
    f = open(f_name+'_conn_key', 'w')
    f.write(conn_key)
    f.close()

def read_address_file(f_name):
    """Read the connection address from file."""
    try:
        f = open(f_name+'_address', 'r')
        address = f.read().strip()
        f.close()
    except:
        address = 'localhost'
    return address

def read_conn_files(f_name):
    """Reads the connection information from files."""
    address = read_address_file(f_name)
    try:
        f = open(f_name+'_port', 'r')
        port = int(f.read())
        f.close()
    except:
        port = None
    try:
        f = open(f_name+'_conn_key', 'r')
        conn_key = f.read()
        f.close()
    except:
        conn_key = None
    return address, port, conn_key

def true_or_false(v):
    """Return True or False, depending on the string v."""
    if (v.lower() == 'true'):
        return True
    elif (v.lower() == 'false'):
        return False
    else:
        raise Exception('Neither true nor false.')

def get_plural(word_s, stuff):
    """Get the plural
    
    Arguments:
    word_s (tuple, list)      Contains (singular_case, plural_case)
    stuff (list, strings,     The stuff to be checked to be multiple
           positive int)      or single
    
    Behaviour:
    Analyse 'stuff' and return the correct thing, in plural
    or singular. Most a cosmetic function to have gramatically
    correct output.
    
    Return:
    A tuple (correct_case, Predicate or listing)
    
    Raise:
    An Exception if we cannot deal with stuff
    
    Examples:
    get_plural(("job","jobs"),0) => ("jobs", "No")
    get_plural(("job","jobs"),["queued", "running", "killed"]) => ("jobs", "queued, running and killed")
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


def message_transfer(msg, address, port, key, timeout=5.0):
    """Send and receive a message.
    
    Arguments:
    msg (str)          The message to be transfer
    address (str)      The address of the Listener
    port (int)         The port of the connection
    key (uint)         The key for the connection
    wait (int, float)  (optional, default = 5.0).
                       The waiting time in seconds until it
                       gives up the attempt to connect
    
    Behaviour:
    This function send a message to a Listener,
    wait for a message back from the Listener and returns it.
    If the Listener takes too long to accept the connection, 
    an exception is raised.
    
    Returns:
    The message back from the connection.
    
    Raise:
    An Exception if the connection is not established.
    """
    def my_init_timeout():
        return time.time() + timeout
    connection._init_timeout = my_init_timeout
    
    try:
        conn = connection.Client((address, port), authkey=key)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        raise Exception("Connection not successful: " + str(exc_value))
    
    conn.send(msg)
    back_msg = conn.recv()
    conn.close()
    return back_msg

def node_exec(node, command, get_outerr = True, mode="paramiko", pKey_file = None, localhost_popen_shell=False):
    """ Execute a command by ssh
    
    Arguments:
    node (str)         Where to execute
    command (str)      The command to be executed
    get_outerr (bool)  (optional, default = True).
                       If True, returns a tuple with the
                       stdout and stderr of the command.
    mode (str)         (optional, default = "paramiko")
                       The mode for the connection.
                       Possible modes:
                       "paramiko"
                       "popen"
    pKey_file          (optional, default = None)
    localhost_popen_shell (bool)
                       (optional, default = False)
                       Value for the argument shell of
                       subprocess.Popen used when node is
                       localhost.
    
    Return:
    The tuple (stdout, stderr), if the optional argument get_outerr
    is set to True
    
    Raise:
    SSH Exceptions if there is a problem in the SSH connection
    """
    if node == 'localhost':

        if isinstance(command, str) and not localhost_popen_shell: # Just to make it work with localhost as node...
            command = command.split()
        if (get_outerr):
            ssh = subprocess.Popen(command, shell = localhost_popen_shell,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            std_outerr = ssh.communicate()
            return(std_outerr)
        else:
            ssh = subprocess.Popen(command, shell = localhost_popen_shell)
            return

    elif (mode == "paramiko" and is_paramiko):
        if isinstance(command, list):
            command = ' '.join(command)
        if pKey_file is not None:
            k = paramiko.RSAKey.from_private_key_file(pKey_file)
        else:
            k = None
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(node, pkey = k)
        except paramiko.BadHostKeyException:
            raise Exception( "SSH error: server\'s host key could not be verified")
        except paramiko.AuthenticationException:
            raise Exception( "SSH error: authentication failed")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise Exception( "SSH error: Connection error" + str(exc_type) + "  " + str(exc_value))

        if (get_outerr):
            (stdin, stdout, stderr) = ssh.exec_command( command)
            out = stdout.read()
            err = stderr.read()
            stdin.flush()
            stdout.close()
            stderr.close()
            ssh.close()
            return (out, err)

        else:
            ssh.exec_command(command)
            sleep(1.)
            ssh.close()
            return

    elif (mode == "popen"):
        if isinstance(command, str):
            command = command.split()
        if (get_outerr):
            ssh = subprocess.Popen(['ssh', node] + command, shell = False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            std_outerr = ssh.communicate()
            return(std_outerr)
        else:
            ssh = subprocess.Popen(['ssh', node] + command, shell = False)
            return
        
    else:
        raise Exception("Unknown mode for node_exec")


class MULTIUSER_JOB():
    """Class to represent a running job
    
    Attributes:
    user (str)      The user that runs this job
    ID (int)        The job ID
    n_cores (int)   Number of cores
    mem (float)     Required memory
    node (str)      The node where this job runs
    
    Behaviour:
    This is a much simple class than JOB, and it represents
    the job as seen by qpy-multiuser.
    """
    def __init__( self, user, jobID, mem, n_cores, node):
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
        return self.__dict__ == other.__dict__

    def __str__(self):
        """String representation."""
        return str(self.ID)  + ": node = " + self.node + ", n_cores = " + str(self.n_cores) + ", mem = " + str(self.mem)


class Messages():
    """The messages for debbuging, generated at run time.
    
    Attributes:
    save (bool)       (default = False) If True, messages are saved
    messages (list)   A list with the messages, as [M, n_times], where
                      M is a string (the message itself) and n_times is
                      the number of time the messages was sent.
    max_len (int)     (default = 100) Maximum number of
                      messages (not counting repetitions)
    
    Behaviour:
    This class handles and stores the messages obtained
    from exceptions and others.
    Repeated messages sent one after another are not doubly
    stored, just a counter is raised.
    If number of messages gets larger than max_len, older messages
    are removed.
    """
    
    def __init__(self):
        """Initialise the class."""
        self.save = False
        self.messages = []
        self.max_len = 100

    def __len__(self):
        """Returns the number of messages."""
        return len( self.messages)

    def __repr__(self):
        """Retruns important informations about the messages."""
        return "Messages<save=" + str(self.save) + ";max_len=" + str(self.max_len) + ";len=" + len(self.messages) + ">"

    def __str__(self):
        """Retruns a formatted version of the messages."""
        x = ''
        for m in self.messages:
            x += m[0]
            if (m[1] > 1):
                x += ' (' + str(m[1]) + 'x)'
            x += '\n'
        return x

    def clean(self):
        """Delete all messages"""
        self.messages = []

    def add(self, M):
        """Add a new message M"""

        if (self.save):
            if (len(self.messages) == 0 or self.messages[-1][0] != M):
                self.messages.append([M, 1])
            else:
                self.messages[-1][1] += 1
            if (len(self) > self.max_len):
                self.messages.pop(0)



class Configurations():
    """The current configuration of qpy.
    
    Attributes:
    config_file (str)        The file with configurations
    messages (Messages)      Messages to the user
    job_fmt_pattern (str)    A pattern for the check
    use_colour (bool)        (default = True) If True, check is coloured
                             (for terminal outputs)
    colour_scheme (list)     Colours to the check command
    use_script_copy (bool)   (default = True) If True, save scripts to execute original versions
    sub_paused (True)        If True, job submission is paused
    default_attr (list)      A list with the default node attributes
    and_attr (list)          A list with attributes to be added with "and"
    or_attr (list)           A list with attributes to be added with "or"
    sleep_time_sub_ctrl (int)      (default = 1) Time that sub_ctrl is
                                   paused between each check
    sleep_time_check_run (int)     (default = 10) Time that check_run is
                                   paused between each check
    source_these_files (list)      (default = ['~/.bash_profile'])
                                   files to be sourced in every job run
    ssh_p_key_file                 (defaut = None)
    logger_level (string, int)     (default = 'warning')
    logger                         A logger to print messages to file
                                   (global) master_log_file
    
    Behaviour:
    Contains the messages
    and the several variables that can be customized
    by the user.
    
    See also:
    Messages
    """
    def __init__(self, config_file):
        """Initialise a new set of configurations
        
        Arguments:
        config_file (str)    The file with configurations
        
        Behaviour:
        Read configurations from the file config_file or initiate them
        with some appropriate default values and then write on the file
        """
        self.config_file = config_file
        self.messages = Messages()
        self.job_fmt_pattern = JOB_FMT_PATTERN_DEF
        self.use_colour = True
        self.colour_scheme = POSSIBLE_COLOURS[:5]
        self.use_script_copy = False
        self.sub_paused = False
        self.default_attr = []
        self.and_attr = []
        self.or_attr = []
        self.sleep_time_sub_ctrl = 1
        self.sleep_time_check_run = 10
        self.source_these_files = ['~/.bash_profile']
        self.ssh_p_key_file = None
        self.logger_level = 'warning'
        self.logger = configure_root_logger(master_log_file, logging.WARNING)

        if (os.path.isfile(self.config_file)):
            f = open(self.config_file, 'r')
            for l in f:
                l_spl = l.split()
                if (not(l_spl)):
                    continue
                key = l_spl[0]
                if (key == 'checkFMT'):
                    val = l.strip()[10:-1]
                elif (key == 'job_fmt_pattern'):
                    val = l.strip()[17:-1]
                elif (len(l_spl) == 1):
                    val = ()
                elif (len(l_spl) == 2):
                    val = l_spl[1]
                else:
                    val = l_spl[1:]
                (status, msg) = self.set_key(key, val)
                if (status != 0):
                    self.messages.add('Reading config file: ' + msg)
            f.close()
        else:
            print "Initialising config file."
            self.write_on_file()

    def set_key(self, k, v):
        """Set a new configuration value
        
        Arguments:
        k   key
        v   value
        
        Behaviour:
        Set the value given by v to the configuration
        key k.
        
        Return:
        The tuple (status, msg) where status is:
        0 - successfull
        1 - key problem
        2 - value problem
        msg (str) is a informative message.
        
        TODO: change return status to Exception?
        """
        if (k == 'checkFMT' or k == 'job_fmt_pattern'): # job_fmt_pattern: obsolete
            if (v == 'default'):
                self.job_fmt_pattern = JOB_FMT_PATTERN_DEF
                msg = 'Check pattern restored to the default value: ' + repr(self.job_fmt_pattern) + '.'
                status = 0
            else:
                self.job_fmt_pattern = v.decode('string_escape')
                msg = 'Check pattern modified to ' + repr(self.job_fmt_pattern) + '.'
                status = 0

        elif (k == 'paused_jobs'):
            try:
                self.sub_paused = true_or_false(v)
            except:
                msg = "Key for paused_jobs must be true or false."
                status = 2
            else:
                msg = "paused_jobs set to " + str(self.sub_paused) + '.'
                status = 0

        elif (k == 'defaultAttr'):
            try:
                self.default_attr = [v] if isinstance(v, str) else v
            except:
                msg = "Key for " + k + " must be a string."
                status = 2
            else:
                if v:
                    msg = k + " set to " + ' '.join(self.default_attr) + '.'
                else:
                    msg = k + " unset."
                status = 0

        elif (k == 'andAttr'):
            try:
                self.and_attr = [v] if isinstance(v, str) else v
            except:
                msg = "Key for " + k + " must be a string."
                status = 2
            else:
                if v:
                    msg = k + " set to " + ' '.join(self.and_attr) + '.'
                else:
                    msg = k + " unset."
                status = 0

        elif (k == 'orAttr'):
            try:
                self.or_attr = [v] if isinstance(v, str) else v
            except:
                msg = "Key for " + k + " must be a string."
                status = 2
            else:
                if v:
                    msg = k + " set to " + ' '.join(self.or_attr) + '.'
                else:
                    msg = k + " unset."
                status = 0

        elif (k == 'copyScripts' or k == 'use_script_copy'): # use_script_copy: obsolete
            try:
                self.use_script_copy = true_or_false(v)
            except:
                msg = "Key for copyScripts must be true or false."
                status = 2
            else:
                msg = "copyScripts set to " + str(self.use_script_copy)
                status = 0

        elif (k == 'saveMessages' or k == 'save_messages'):
            try:
                self.messages.save = true_or_false(v)
            except:
                msg = "Key for saveMessages must be true or false."
                status = 2
            else:
                msg = "saveMessages set to " + str(self.messages.save)
                status = 0

        elif (k == 'maxMessages'):
            try:
                self.messages.max_len = int(v)
            except:
                msg = "Key for maxMessages must be and integer."
                status = 2
            else:
                msg = "maxMessages set to " + str(self.messages.max_len)
                status = 0

        elif (k == 'ssh_p_key_file' or k == 'ssh_pKey'):
            if (v == 'None'):
                self.ssh_p_key_file = None
            else:
                self.ssh_p_key_file = v

            msg = "ssh_pKey set to " + str(self.ssh_p_key_file)
            status = 0

        elif (k == 'cleanMessages'):
            self.messages.clean()
            msg = "Messages were cleand."
            status = 0

        elif (k == 'loggerLevel'):
            if v in ['debug', 'DEBUG']:
                vnew = logging.DEBUG
            elif v in ['info', 'INFO']:
                vnew = logging.INFO
            elif v in ['warning', 'WARNING']:
                vnew = logging.WARNING
            elif v in ['error', 'ERROR']:
                vnew = logging.ERROR
            elif v in ['critical', 'CRITICAL']:
                vnew = logging.CRITICAL
            else:
                try:
                    vnew = int(v)
                except:
                    vnew = None

            if vnew is not None:
                self.logger_level = v
                self.logger.setLevel(vnew)
                msg = 'Logger level set to ' + v
                status = 0
            else:
                msg = 'Unknown logging level: ' + v
                status = 1

        elif (k == 'colour' or k == 'use_colour'):
            try:
                self.use_colour = true_or_false(v)
            except:
                msg = "Key for colour must be true or false."
                status = 2
            else:
                msg = "colour set to " + str(self.use_colour)
                status = 0

        elif (k == 'coloursScheme'):
            colours_ok = True
            for i in v:
                if (not(i in POSSIBLE_COLOURS)):
                    msg = 'Unknown colour: ' + i + '.\n'
                    status = 2
                    colours_ok = False
                    break
            if (len(v) != 5):
                msg = 'Give five colours for coloursScheme.\n'
                status = 2
                colours_ok = False
            if (colours_ok):
                self.colour_scheme = list(v)
                msg = 'Colours scheme changed.\n'
                status = 0

        elif (k == 'sleepTimeSubCtrl'):
            try:
                self.sleep_time_sub_ctrl = float(v)
            except:
                msg = "Key for sleepTimeSubCtrl must be a float number."
                status = 2
            else:
                msg = "sleepTimeSubCtrl set to " + str(self.sleep_time_sub_ctrl) + '.'
                status = 0

        elif (k == 'sleepTimeCheckRun'):
            try:
                self.sleep_time_check_run = float(v)
            except:
                msg = "Key for sleepTimeCheckRun must be a float number."
                status = 2
            else:
                msg = "sleepTimeCheckRun set to " + str(self.sleep_time_check_run) + '.'
                status = 0

        elif (k == 'sourceTheseFiles'):
            if (isinstance(v, list)):
                self.source_these_files = v
                msg = "sourceTheseFiles set to " + str(self.source_these_files) + '.'
                status = 0
            elif (isinstance(v, str)):
                self.source_these_files = [v]
                msg = "sourceTheseFiles set to " + str(self.source_these_files) + '.'
                status = 0
            else:
                msg = "sourceTheseFiles should receive a file name or a list of files."
                status = 2

        else:
            msg = 'Unknown key: ' + k
            status = 1

        return (status, msg)

    def write_on_file(self):
        """Write the current configurations in the file."""
        f = open(self.config_file, 'w')
        f.write('paused_jobs '  + str(self.sub_paused)      + '\n')
        f.write('saveMessages ' + str(self.messages.save)   + '\n')
        f.write('maxMessages '  + str(self.messages.max_len)+ '\n')
        f.write('loggerLevel '  + str(self.logger_level)    + '\n')
        f.write('defaultAttr '  + ' '.join(self.default_attr) + '\n')
        f.write('orAttr '       + ' '.join(self.or_attr)    + '\n')
        f.write('andAttr '      + ' '.join(self.and_attr)   + '\n')
        f.write('checkFMT '     +repr(self.job_fmt_pattern) + '\n')
        f.write('ssh_pKey '     + str(self.ssh_p_key_file)  + '\n')
        f.write('copyScripts '  + str(self.use_script_copy) + '\n')
        f.write('colour '       + str(self.use_colour)      + '\n')
        f.write('coloursScheme '
                 + str(self.colour_scheme[0]) + ' '
                 + str(self.colour_scheme[1]) + ' '
                 + str(self.colour_scheme[2]) + ' '
                 + str(self.colour_scheme[3]) + ' '
                 + str(self.colour_scheme[4]) + '\n')
        f.write('sleepTimeSubCtrl '  + str(self.sleep_time_sub_ctrl)  + '\n')
        f.write('sleepTimeCheckRun ' + str(self.sleep_time_check_run) + '\n')
        f.write('sourceTheseFiles ')
        for i in self.source_these_files:
            f.write(i + ' ')
        f.write('\n')
        f.close()

    def __str__(self):
        """A formatted version of the configurations."""
        msg = 'Check pattern: ' + repr(self.job_fmt_pattern) + '\n'
        msg += 'Using a copied version of run script: ' + str(self.use_script_copy) + '\n'
        msg += 'Using coloured check: ' + str(self.use_colour) + '\n'
        msg += 'Sleeping time in submission control: ' + str(self.sleep_time_sub_ctrl) + '\n'
        msg += 'Sleeping time in check run: ' + str(self.sleep_time_check_run) + '\n'
        if self.default_attr:
            msg += 'Default node attributes: ' + ' '.join(self.default_attr) + '\n'
        if self.and_attr:
            msg += '"and" node attributes: ' + ' '.join(self.and_attr) + '\n'
        if self.or_attr:
            msg += '"or" node attributes: ' + ' '.join(self.or_attr) + '\n'
        if self.ssh_p_key_file is not None:
            msg += 'Using ssh private key from ' + self.ssh_p_key_file + '\n'
        if (len(self.source_these_files)):
            msg += 'These files are sourced when running a job:\n'
            for i in self.source_these_files:
                msg += '  ' + i + '\n'
        if (self.use_colour):
            msg += 'Colours:\n'
            for i in range(len(JOB_STATUS)):
                msg += '  - ' + termcolour.colored(JOB_STATUS[i], self.colour_scheme[i])
                msg += ' (' + self.colour_scheme[i] + ')\n'
        if (self.sub_paused):
            msg += 'Job submission is paused\n'
        if (self.messages.save):
            msg += 'A maximum of ' + str(self.messages.max_len) + ' messages are being saved\n'
        if (len(self.messages) > 0):
            msg += 'Last messages:\n' + str(self.messages) + '\n'
        msg += 'Logger level: ' + str(self.logger_level) + '\n'

        return msg


class Job_Id():
    """The job ID.
    
    Attributes:
    file (str)       The file where the job ID is stored
    current (int)    The ID for the next job
    
    Behaviour:
    This class initiate a job Id for the jobs and increment
    them. It automatically writes the next ID to be used on a file
    in case of crash or restart.
    """

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

