"""qpy-master - set the main driver for qpy

History:
    29 May 2015 - Pradipta and Yuri
"""
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
import traceback

import  qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_useful_cosmetics as qpyutil
import qpy_communication as qpycomm
from qpy_configurations import Configurations
from qpy_job import JobId, MultiuserJob

#TODO: improve exception handling
class MyError(Exception):
    def __init__(self,msg):
        self.message=msg

class ParseError(MyError):
    pass

class HelpException(MyError):
    pass



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


class JobParser(OptionParser):
    """An Option Parser that does not exit the program, but just raises a ParseError
    
    NOTE:
    optparse is deprecated and the overwritten
    functions are somewhat mentioned in the documentation.
    
    TODO:
    replace it by argparse
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
    """A job submitted by the user.
    
    Attributes:
    ID (int)                A number to identify the job
    info (list)             A list of strings with the command (in [0]) and
                            the calling directory (in [1])
    n_cores (int)           Number of cores used by the job
    mem (float)             Memory requested for this job
    node_attr (list)        The node attributes for this job
    node (str)              The node name where this job is running
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
    rerun (bool)            If True, it means that this job started to run in
                            a previous instance of qpy-master
    queue_time (datetime)   When the job was put in queue (that is, submited
                            by the user)
    start_time (datetime)   When the job started to run
    end_time (datetime)     When the job finished (as detected by this program.
                            It should not be used to check execution time
                            accurately)
    runDuration (datetime)  Duration of the job execution
    
    Behaviour:
    This class has all the information about individual jobs.
    """

    __slots__=("ID","info","n_cores","mem","node_attr","node","status","use_script_copy","cp_script_to_replace","re_run","queue_time","start_time","end_time","runDuration","parser")

    def __init__(self, jobID, job_info, config):
        """Initiate the class.
        
        Arguments:
        jobID (int)                 The job identifier
        job_info (list)             A list of strings with the command (in [0]) and
                                    the calling directory (in [1])
        config (Configurations)     The qpy configurations
        """
        self.ID = jobID
        self.info = job_info
        self.n_cores = 1
        self.mem = 5.0
        self.node_attr = []
        self.node = None
        self.status = qpyconst.JOB_ST_QUEUE
        self.use_script_copy = config.use_script_copy
        self.cp_script_to_replace = None
        self.re_run = False
        self.set_parser()
        self.queue_time = datetime.datetime.today()
        self.start_time = None
        self.end_time = None
        self.runDuration = None

    def run_duration(self):
        """Return the running time or the queue time.
        
        Behaviour:
        If the job is running, return the current running time;
        If the job is in the queue, return for how long it is in queue;
        If the job has been finished, return self.runDuration and
        sets it if is still None.
        """
        if (self.status == qpyconst.JOB_ST_QUEUE):
            try:
                return datetime.datetime.today() - self.queue_time
            except:
                return None
        if (self.status == qpyconst.JOB_ST_RUNNING):
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
        """Return a formatted string with main information about the job."""
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

        if ('%K' in job_str):
            if (os.path.isfile(qpysys.notes_dir + 'notes.' + str(self.ID))):
                f = open(      qpysys.notes_dir + 'notes.' + str(self.ID), 'r')
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
                              ('%R', str(self.run_duration()))
                              ):
            job_str = job_str.replace(pattern, info)
        return job_str

    def set_parser(self):
        """Create a parser for the flags that can be set for a job."""
        parser=JobParser()
        parser.add_option("-n","--cores", dest="cores", help="set the number of cores", default="1")
        parser.add_option("-m","--mem","--memory",dest="memory",help="set the memory in GB", default="5")
        parser.add_option("-a","--node_attr","--attributes",dest="node_attr",help="set the attributes for node", default='')
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
        command += 'ulimit -Sv ' + str( max(self.mem*1.5,10)*1048576) + '; '
        for sf in config.source_these_files:
           command += 'source ' + sf + '; '
        command += 'cd ' + self.info[1] + '; ' 
        try:
            command += self.info[0].replace( self.cp_script_to_replace[0], 'sh ' + self.cp_script_to_replace[1], 1)
        except:
            command += self.info[0]
        command += ' > ' + out_or_err_name( self, '.out') + ' 2> ' + out_or_err_name( self, '.err')

        config.logger.debug('Command: ' + command)
        try:
            qpycomm.node_exec(self.node,
                              command,
                              get_outerr=False,
                              pKey_file=config.ssh_p_key_file,
                              localhost_popen_shell=(self.node == 'localhost'))
        except:
            config.logger.error("Exception in run", exc_info=True)
            raise Exception( "Exception in run: " + str(sys.exc_info()[1]))

        self.start_time = datetime.datetime.today()
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
        (std_out, std_err) = qpycomm.node_exec(self.node,
                                               command,
                                               pKey_file=config.ssh_p_key_file)
        re_res = re.search('export QPY_JOB_ID=' + str( self.ID) + ';', std_out)
        if (re_res):
            return True
        return False


    def _scanline(self,line,option_list):
        """Parse the lines for qpy directives and options."""
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
                        if (attr == 'node_attr'):
                            self.node_attr = re_res.group(1).split()
                        if (attr == 'cpScript'):
                            self.use_script_copy = True if (re_res.group(1) == 'true') else False
                        option_found=True
                    except ValueError:
                        raise ParseError("Invalid Value for {atr} found.".format(atr=attr))
            if ( not option_found ):
                raise ParseError("QPY directive found, but no options supplied."\
                             "Please remove the #QPY directive if you don't want to supply a valid option.")


    def _parse_file_for_options(self,file_name):
        """Parse a submission script file for options set in the script.
        
        Arguments::
        file_name (str)     the file name
        
        Behaviour:
        The options can be set in the script by:
        
        #QPY <key>=<value>
        #QPY <key> = <value>
        #QPY <key> <value>
        
        where the possible key/values are:
        n_cores     number of cores
        mem         memory
        node_attr   nodes attributes
        cpScript    true or false, for copy script
        """
        option_list=[("n_cores"  , re.compile('n_cores\s*=?\s*(\d*)' ) ),
                     ("mem"      , re.compile('mem\s*=?\s*(\d*)'     ) ),
                     ("node_attr", re.compile('node_attr\s*=?\s*(.+)') ),
                     ("cpScript" , re.compile('cpScript\s*=?\s*(\w*)') )
                     ]
        try:
            with open( file_name, 'r') as f:
                for line in f:
                    self._scanline(line,option_list)
        except:
            pass # Is an executable or file_name = None

    def _parse_command_for_options(self, command):
        """Parse the command for options.
        
        Arguments:
        command (str)   the command to be parsed
        
        Behaviour:        
        Set the options found in the command and return the
        command free of these options.
        
        Return:
        The command, without the parsed options.
        
        Raise:
        ParseError, if the parse was not successful
        """
        try:
            options,command = self.parser.parse_args( command.split())
            self.n_cores = int(options.cores)
            self.mem = float(options.memory)
            if options.node_attr:
                self.node_attr = options.node_attr.split('##')
            else:
                self.node_attr = []
            if (options.cpScript == None and options.orScript == None):
                pass
            elif (options.cpScript != None and options.orScript != None):
                raise ParseError("Please, do not supply both cpScript and orScript")
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
        """Expand a filename to its absolute name, UNIX only.
        
        TODO:
        Make it compatible to other systems.
        """
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
        """Parse the input and the submission script for options."""
        self.info[0] = self._parse_command_for_options(self.info[0])
        first_arg = self.info[0].split()[0]
        script_name = self._expand_script_name(first_arg)
        self._parse_file_for_options(script_name)


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
            if (k == 'status'):
                req = req or qpyconst.JOB_STATUS[self.status] in pattern[k]
            elif ( k == "job_id"):
                req = req or self.ID in pattern[k]
            elif k == 'dir':
                req = req or self.info[1] in pattern[k]
        return req


class Job_Collection():
    """Store information about the jobs.
    
    Attributes:
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
        """Initiate the class.
        
        Arguments:
        config (Configurations)    qpy configurations
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
        """Pop job of Q, and return it."""
        with self.lock:
            job = self.Q.pop()
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
                    asked_jobs.append( job)
        req_jobs = ''
        for job in asked_jobs:
            j_str = job.fmt(config.job_fmt_pattern)
            if (config.use_colour):
                j_str = termcolour.colored(j_str, config.colour_scheme[job.status])
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
                cur_jobs.append(MultiuserJob(qpysys.user, job.ID, job.mem, job.n_cores, job.node))
        return cur_jobs

    def initialize_old_jobs(self, sub_ctrl):
        """Initialize jobs from file (global) all_jobs_file."""
        with self.lock:
            if (os.path.isfile(qpysys.all_jobs_file)):
                with open(qpysys.all_jobs_file, 'r') as f:
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
                            lspl = line.split()
                            new_wd = lspl[0]
                            new_node_attr = [] if len(lspl) == 1 else lspl[1:]
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
                            new_job.node_attr = new_node_attr
                            self.all.append( new_job)
                            if (new_job.status == qpyconst.JOB_ST_QUEUE):
                                self.queue.append( new_job)
                                self.Q.appendleft( new_job)
                            elif (new_job.status == qpyconst.JOB_ST_RUNNING):
                                self.running.append( new_job)
                                new_job.re_run = True
                            elif (new_job.status == qpyconst.JOB_ST_DONE):
                                self.done.append( new_job)
                            elif (new_job.status == qpyconst.JOB_ST_KILLED):
                                self.killed.append( new_job)
                            elif (new_job.status == qpyconst.JOB_ST_UNDONE):
                                self.undone.append( new_job)


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
            new_job = JOB(int(job_id), arguments, config)
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
