# qpy - control the nodes distribution for several users
#
# 26 December 2015 - Pradipta and Yuri
# 06 January 2016 - Pradipta and Yuri
from time import sleep
import os
import sys
import subprocess
import re
from collections import namedtuple
import math
from optparse import OptionParser
import threading
from qpyCommon import *
import logging

if (TEST_RUN):
    qpy_multiuser_dir = os.path.expanduser( '~/.qpy-multiuser-test/')
else:
    qpy_multiuser_dir = os.path.expanduser( '~/.qpy-multiuser/')

if (not( os.path.isdir( qpy_multiuser_dir))):
    os.makedirs( qpy_multiuser_dir)
os.chmod( qpy_multiuser_dir, 0700)

nodes_file = qpy_multiuser_dir + 'nodes'
allowed_users_file = qpy_multiuser_dir + 'allowed_users'
cores_distribution_file = qpy_multiuser_dir + 'distribution_rules'
user_conn_file = qpy_multiuser_dir + 'connection_'
multiuser_conn_file = qpy_multiuser_dir + 'multiuser_connection'
multiuser_log_file = qpy_multiuser_dir + 'multiuser_log'

nodes_list = []
nodes = {}
users = {}
N_cores = 0
N_min_cores = 0
N_used_cores  = 0
N_used_min_cores = 0
N_outsiders = 0

nodes_check_lock = threading.RLock()
nodes_check_alive = True
nodes_check_time = 300



logging.basicConfig(filename=multiuser_log_file,level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

class NODE():
    """A node from the qpy-multiuser point of view.

    Contains:


    """
    def __init__( self, name, max_cores):
        self.name = name
        self.max_cores = max_cores

        self.messages = Messages()
        self.messages.save = True

        self.is_up = False

        self.n_used_cores = 0
        self.pref_multicores = False
        self.req_mem = 2.0

        self.total_mem = 0.0
        self.free_mem_real = 0.0
        self.n_outsiders = 0


    def check(self):
        """Check several things in the node.

        Returns a named tuple with the information:

        is_up           True if the node is up
        n_outsiders     the number of cores used by non-qpy processes
        total_mem       total memory of the node
        free_mem_real   free memory of node
        """

        info = namedtuple('nodeInfo', ['is_up','n_outsiders','total_mem','free_mem_real'])
        info.is_up = True

        command = "top -b -n1 | sed -n '8,50p'"
        try:
            (std_out, std_err) = node_exec(self.name, command)
        except:
            logging.exception("finding the number of untracked jobs failed for node: %s",self.name)
            self.messages.add('outsiders: Exception: ' + repr(sys.exc_info()[0]))
            info.is_up = False
            info.n_outsiders = 0
        else:
            std_out = std_out.split("\n")
            n_jobs = 0
            for line in std_out:
                line_spl = line.split()
                if float(line_spl[8].replace(',','.')) > 50:
                    n_jobs += 1
                else:
                    break
            info.n_outsiders = max(n_jobs - self.n_used_cores, 0)

        command = "free -g"
        try:
            (std_out, std_err) = node_exec(self.name, command)
        except:
            logging.exception("finding the the free memory failed for node: %s",self.name)
            self.messages.add('memory: Exception: ' + repr(sys.exc_info()[0]))
            info.is_up = False
            info.free_mem_real = 0.0
            info.total_mem = 0.0
        else:
            std_out = std_out.split("\n")
            info.total_mem = float(std_out[1].split()[1])
            if (len(std_out) == 5):
                info.free_mem_real = float(std_out[2].split()[3])
            else:
                info.free_mem_real = float(std_out[1].split()[6])
            logging.info("node %s is up",self.name)
        return info


class USER():

    """A user from the qpy-multiuser point of view

    Contains:
    name       The user
    port       port to the qpy-master connection 
    conn_key   key to the qpy-master connection

    min_cores    only for the user
    extra_cores  preferably for the user
    max_cores    maximum that can be used: N_cores - min_cores (of other users)
    n_used_cores current number of used cores
    cur_jobs     current jobs
    messages     Debugging messages


    """

    def __init__( self, name, address, port, conn_key):
        """Initiate the class
        
        Arguments:
        name       The user
        port       port to the qpy-master connection 
        conn_key   key to the qpy-master connection
        """
        self.messages = Messages()
        self.messages.save = True

        self.name = name
        self.address = address
        self.port = port
        self.conn_key = conn_key

        self.min_cores = 0
        self.extra_cores = 0
        self.max_cores = 0
        self.n_used_cores = 0
        self.n_queue = 0
        self.cur_jobs = []

    def remove_job( self, jobID):
        """Remove the job"""
        global N_used_cores, N_used_min_cores
        for job in self.cur_jobs:
            if (job.ID == jobID):
                self.n_used_cores -= job.n_cores
                nodes[job.node].n_used_cores -= job.n_cores
                nodes[job.node].req_mem -= job.mem
                N_used_cores -= job.n_cores
                if (self.n_used_cores < self.min_cores):
                    N_used_min_cores -= min( job.n_cores, self.min_cores - self.n_used_cores)
                self.cur_jobs.remove( job)
                return 0
        return 1

    def add_job( self, job):
        """Add job to a specific node."""
        global N_used_cores, N_used_min_cores
        
        self.cur_jobs.append( job)
        self.n_used_cores += job.n_cores
        nodes[job.node].n_used_cores += job.n_cores
        nodes[job.node].req_mem += job.mem

        N_used_cores += job.n_cores

        if (self.n_used_cores < self.min_cores):
            N_used_min_cores += job.n_cores
        elif (self.n_used_cores < self.min_cores + job.n_cores):
            N_used_min_cores += job.n_cores + self.min_cores - self.n_used_cores

        return 0


    def request_node( self, jobID, num_cores, mem):
        """Tries to give a space to the user

        Arguments:
        jobID      the job ID
        num_cores  requested number of cores
        mem        requested memory

        This is the main function of qpy-multiuser.
        It deceides whether the requested resource can be
        given or not and, if so, give a node thet fulfills
        this requirement.
        """
        global N_used_cores, N_used_min_cores
        space_available = False
        
        N_free_cores = (N_cores - N_used_cores) - (N_min_cores - N_used_min_cores)
        free_cores = N_free_cores >= num_cores

        if (self.n_used_cores + num_cores <= self.min_cores):
            space_available = True

        else:
            use_others_resource = self.n_used_cores + num_cores > self.min_cores + self.extra_cores

            if (use_others_resource):

                N_users_with_queue = 1
                N_extra = 0
                for user,info in users.iteritems():
                    if (user == self.name):
                        continue
                
                    if (info.n_queue > 0 and info.n_used_cores >= info.min_cores + info.extra_cores):
                        N_users_with_queue += 1

                    elif (info.n_queue == 0):
                        N_extra += min(info.extra_cores + info.min_cores - info.n_used_cores, info.extra_cores)

                N_extra_per_user = N_extra/N_users_with_queue
                if (self.n_used_cores + num_cores <= self.min_cores + self.extra_cores + N_extra_per_user and free_cores):
                    space_available = True
                    
            else:
                space_available = free_cores 

        if (space_available):
            best_node = None
            best_free = 0
            nodes_check_lock.acquire()
            if (num_cores == 1):
                for node, info in nodes.iteritems():
                    free = info.max_cores - info.n_outsiders - info.n_used_cores
                    if (not(info.pref_multicores) and free > best_free and (info.total_mem-info.req_mem) > mem and info.free_mem_real > mem):
                        best_node = node
                        best_free = free
            if (best_node == None):
                for node in nodes_list:
                    if (nodes[node].is_up and
                        nodes[node].max_cores  - nodes[node].n_outsiders - nodes[node].n_used_cores >= num_cores and
                        nodes[node].total_mem - nodes[node].req_mem > mem):
                        best_node = node
                        break
            nodes_check_lock.release()

            if (best_node == None):
                return 1

            new_job = MULTIUSER_JOB( self.name, jobID, mem, num_cores, best_node)
            self.add_job( new_job)

            return best_node

        return 2


def load_users():
    """Load the users.

    For each user given in the file allowed_users,
    attempt to obtain the port and the key for the
    connection with their qpy-master (from MULTIUSER_HANDLER)
    and request the current running jobs
    """
    global users
    allowed_users = []
    f = open( allowed_users_file, 'r')
    for line in f:
        allowed_users.append( line.strip())
    f.close()
    for user in allowed_users:
        print user
        address, port, conn_key = read_conn_files(user_conn_file + user)
        if (port != None and conn_key != None):
            new_user = USER(user, address, port, conn_key)
            try:
                cur_jobs = message_transfer((FROM_MULTI_CUR_JOBS, ()),
                                            new_user.address, new_user.port, new_user.conn_key)
            except:
                pass
            else:
                for job in cur_jobs:
                    new_user.add_job( job)
                users[user] = new_user

    distribute_cores()


def load_nodes():
    """Load the nodes
    
    The nodes are given in the file nodes_file
    TODO: Explain the file format
    
    """
    global N_cores
    global nodes_list
    try:
        f = open( nodes_file, 'r')
    except:
        return -1
    nodes_in_file = []
    cores_in_file = []
    nodes_for_multicores = []
    for line in f:
        line_spl = line.split() # [<node_name>, <max_jobs>, [M]]
        try:
            nodes_in_file.append( line_spl[0])
            cores_in_file.append( int( line_spl[1]))
        except:
            f.close()
            return -2
        if ('M' in line_spl[2:]):
            nodes_for_multicores.append( line_spl[0])
    f.close()
    # Put messages
    for n, c in zip( nodes_in_file, cores_in_file):
        if (n in nodes):
            N_cores += c - nodes[n].max_cores
            nodes[n].max_cores = c
        else:
            new_node = NODE( n, c)
            nodes[n] = new_node
            N_cores += new_node.max_cores
    nodes_to_remove = []
    nodes_list = []
    for n in nodes:
        if (n in nodes_in_file):
            if (n in nodes_for_multicores):
                nodes[n].pref_multicores = True
                nodes_list.insert(0, n)
            else:
                nodes[n].pref_multicores = False
                nodes_list.append( n)
        else:
            nodes_to_remove.append( n)

    for n in nodes_to_remove:
        N_cores -= nodes[n].max_cores
        nodes.pop( n)
    return 0


def distribute_cores():
    """Distributes the cores.

    The distribution is made based on the file cores_distribution_file

    TODO: explain the file format
    """
    try:
        f = open( cores_distribution_file, 'r')
    except:
        return -1
    line = f.readline()
    line_spl = line.split()
    dist_type = line_spl[0]
    min_cores = 0
    users_min = {}
    users_extra = {}
    if (len( line_spl) > 1):
        if (line_spl[1] != 'minimum'):
            return -2
        try:
            min_cores = int(line_spl[2])
        except:
            return -2
    # General minimum cores
    left_cores = N_cores
    for user in users:
        users_min[user] = min_cores
        left_cores -= min_cores
    if (left_cores < 0):
        return -3
    # Even distribution
    if (dist_type == 'even'):
        n_users = len( users)
        if (n_users == 0):
            return 0
        N_per_user = left_cores/n_users
        for user in users:
            users_extra[user] = N_per_user
        left_cores = left_cores - N_per_user*n_users
    # Explicit distribution
    elif (dist_type == 'explicit'):
        for line in f:
            line_spl = line.split()
            user = line_spl[0]
            if (not( user in users)):
                continue
            value = line_spl[1].split('+') 
            if (len( value) > 2 or len( value) == 0):
                return -2
            if (len( value) == 2):
                try:
                    min_cores = int(value[0])
                    left_cores += users_min[user] - min_cores
                    users_min[user] = min_cores
                    value.pop(0)
                except:
                    return -1
            users_extra[user] = value[0]
        if (left_cores < 0):
            return -3
        left_cores_original = left_cores
        for user,info in users_extra.iteritems():
            try:
                if (info[-1] == '%'):
                    N_per_user = float(info[:-1])
                    N_per_user = int(N_per_user*left_cores_original/100)
                else:
                    N_per_user = int(info)
            except:
                return -2
            add_cores = N_per_user
            left_cores -= add_cores
            users_extra[user] = add_cores
    # Unknown type of distribution
    else:
        return -2
    f.close()
    # Equally share left cores
    while (left_cores != 0):
        for user in users_extra:
            if (left_cores == 0):
                break
            users_extra[user] += int(math.copysign(1,left_cores))
            left_cores += math.copysign(1,-left_cores)
    # Finally put into the users variable
    global N_min_cores, N_used_min_cores
    N_min_cores = 0
    N_used_min_cores = 0
    for user in users:
        try:
            users[user].min_cores = users_min[user]
        except:
            users[user].min_cores = 0
        N_min_cores += users[user].min_cores
        N_used_min_cores += min( users[user].min_cores, users[user].n_used_cores)
        try:
            users[user].extra_cores = users_extra[user]
        except:
            users[user].extra_cores = 0
    for user in users:
        users[user].max_cores = N_cores - N_min_cores + users[user].min_cores
    return 0

#-----------------------------------------------------------------
#  handle_yyy methods. Take the arguments of a request and return the string to be send back.

def handle_reload_nodes(args):
    """ handles a request to reload the nodes

    args: ()
    """
    assert len(()) ==0
    status = load_nodes()
    return status,{
        0  : 'Nodes loaded.',
        -1 : 'Nodes loading failed. Problem when openning {0}.'.format( nodes_file),
        -2 : 'Nodes loading failed. Check {0}.'.format( nodes_file),
    }.get(status, 'Nodes loading failed.')

def handle_redistribute_cores(args):
    """handles a request to redistribute cores.

    args: ()
    """
    assert len(()) ==0
    status = distribute_cores()
    return status,{
        0 : 'Cores distributed.',
        -1: 'Cores distribution failed. Problem when openning {0}.'.format(cores_distribution_file),
        -2: 'Cores distribution failed. Check {0}.'.format(cores_distribution_file),
        -3: 'Cores distribution failed. Not enough cores.',
    }.get(status, default='Cores distribution failed.')

#---------------------------------------#
#  formatting functions. 
def format_general_variables():
    variables = [
        ('N_cores', N_cores),
        ('N_min_cores', N_min_cores),
        ('N_used_cores', N_used_cores),
        ('N_used_min_cores', N_used_min_cores),
        ('N_outsiders',N_outsiders),
        ]
    format_spec = '{0: <16} = {1}'
    return '\n'.join( format_spec.format(*pair) for pair in variables)

def format_jobs(jobs):
    format_spec='          {0}'
    return '\n'.join(format_spec.format(job)for job in jobs)

def format_messages(messages):
    return '  Last messages:\n' + str(info.messages) + '----------'

def format_user(user,info):
    fields = [
        ('min_cores',info.min_cores),
        ('extra_cores',info.extra_cores),
        ('max_cores',info.max_cores),
        ('n_used_cores',info.n_used_cores),
        ('n_queue',info.n_queue),
    ]
    format_spec = '  {0: <12} = {1}'
    infos = '\n'.join( format_spec.format(*pair) for pair in fields)
    current_jobs = format_jobs(info.cur_jobs)
    messages = "\n"+format_messages(info.messages) if len(info.messages) >0 else ''
    return '\n'.join([user,
                     infos,
                     current_jobs]) + messages
def format_users(users):
    return "\n".join(format_user(user,info) for user,info in users.iteritems())

def format_node(node,info):
    fields = [
        ('max_cores',info.max_cores),
        ('n_used_cores',info.n_used_cores),
        ('n_outsiders',info.n_outsiders),
        ('total_mem',info.total_mem),
        ('req_mem',info.req_mem),
        ('free_mem_real',info.free_mem_real),
        ('pref_multicores',info.pref_multicores),
        ('is_up',info.is_up),
    ]
    format_spec = '  {0: <15} = {1}'
    infos = '\n'.join( format_spec.format(*pair) for pair in fields)
    messages = "\n"+format_messages(info.messages) if len(info.messages) >0 else ''
    return infos + messages

def format_nodes(nodes):
    return "\n".join(format_node(node,info) for node,info in nodes.iteritems())


def handle_show_variables(args):
    """handles a request to show the variables defining the current status of this service
    
    args: ()
    """
    assert len(()) == 0
    with nodes_check_lock:
        return 0,"{general}\n{theusers}\n{thenodes}\n".format(
            general=format_general_variables(),
            theusers = format_users(users),
            thenodes = format_nodes(nodes)
        )

def handle_show_status(args):
    """handles a request for the general multiuser status

    args : () or (user_name)
    """
    sep1 = '-'*60 + '\n'
    sep2 = '='*60 + '\n'
    headerN =  '                       cores              memory (GB)\n'
    headerN += 'node                used  total      used     req   total\n'
    headerU =  'user                using cores        queue size\n' + sep1

    msgU = ''
    format_spec = '{0:22s} {1:<5d}' + ' '*13 + '{2:<5d}\n'
    for user in sorted(users):
        msgU += format_spec.format( user,
                                    users[user].n_used_cores,
                                    users[user].n_queue)
    msgU = headerU + msgU + sep2 if msgU else 'No users.\n'

    msgN = ''
    format_spec = '{0:20s} {1:<5d} {2:<5d}' + ' '*2 + '{3:>7.1f} {4:>7.1f} {5:>7.1f}\n'
    with nodes_check_lock:
        for node in nodes:
            down=' (down)' if not( nodes[node].is_up) else ''
            msgN += format_spec.format( node + down,
                                        nodes[node].n_used_cores + nodes[node].n_outsiders,
                                        nodes[node].max_cores,
                                        nodes[node].total_mem - nodes[node].free_mem_real,
                                        nodes[node].req_mem,
                                        nodes[node].total_mem)
    msgN = headerN + sep1 + msgN + sep2 if msgN else 'No nodes.\n'
    status = 0
    msg_used_cores = 'There are {0} out of a total of {1} cores being used.\n'.format(
        N_used_cores + N_outsiders,
        N_cores)
    return status, msgU +msgN + msg_used_cores
        
def handle_save_messages(args):
    """handles a request to start saving messages

    args: (save_messages)
    """
    for user in users:
        users[user].messages.save= args[0]
    for node in nodes:
        nodes[node].messages.save= args[0]
    status = 0
    return status, 'Save messages set to {0}.\n'.format(args[0])
    
    
def handle_client():
    """Handles the user messages sent from the client

    No arguments

    It opens a new connection using the multiuser connection
    parameters and waits for messages.
    When a message is received, it analyzes it, does whatever
    is needed and returns a message back.

    The message from qpy must be a tuple (action_type, arguments)
    where action_type can be:
       MULTIUSER_NODES
       MULTIUSER_DISTRIBUTE
       MULTIUSER_SHOW_VARIABLES
       MULTIUSER_STATUS
       MULTIUSER_SAVE_MESSAGES
       MULTIUSER_FINISH
       MULTIUSER_USER
       MULTIUSER_REQ_CORE
       MULTIUSER_REMOVE_JOB

    """
    global N_cores, N_used_cores
    global N_min_cores, N_used_min_cores

    multiuser_address, multiuser_port, multiuser_key = read_conn_files(multiuser_conn_file)

    try:
        conn, multiuser_port, multiuser_key = establish_Listener_connection(multiuser_address,
                                                                            PORT_MIN_MULTI, PORT_MAX_MULTI,
                                                                            port=multiuser_port,
                                                                            conn_key=multiuser_key)

        write_conn_files(multiuser_conn_file, multiuser_address, multiuser_port, multiuser_key)

    except:
        logging.exception("Error when establishing connection. Is there already a qpy-multiuser instance?")
        return

    while True:
        logging.info("Starting main loop.")

        try:
            client = conn.accept()
            (action_type, arguments) = client.recv()
            logging.info("Received request: %s arguments:%s",str(action_type), str(arguments))
        except:
            logging.exception("Connection failed")
            continue

        # Reload the nodes
        # arguments = ()
        if (action_type == MULTIUSER_NODES):
            status,msg = handle_reload_nodes(arguments)


        # Redistribute cores
        # arguments = ()
        elif (action_type == MULTIUSER_DISTRIBUTE):
            status,msg = handle_redistribute_cores(arguments)


        # Show important variables
        # arguments = ()
        elif (action_type == MULTIUSER_SHOW_VARIABLES):
            status,msg = handle_show_variables(arguments)

        # Show status
        # arguments = () or (user_name)
        elif (action_type == MULTIUSER_STATUS):
            status,msg = handle_show_status(arguments)


        # Start saving messages
        # arguments = (save_messages)
        elif (action_type == MULTIUSER_SAVE_MESSAGES):
            status,msg = handle_save_messages(arguments)

        # Finish qpy-multiuser
        # arguments = ()
        elif (action_type == MULTIUSER_FINISH):
            client.send( (0, 'Finishing qpy-multiuser.'))
            client.close()
            break


        # Add a user or sync user info
        # arguments = (user_name, port, conn_key, cur_jobs)
        elif (action_type == MULTIUSER_USER):
            user = arguments[0]
            address = arguments[1]
            port = arguments[2]
            conn_key = arguments[3]
            new_cur_jobs = arguments[4]

            if (user in users):
                status = 0
                users[user].address = address
                users[user].port = port
                users[user].conn_key = conn_key

                same_list = True
                if (len( new_cur_jobs) != len( users[user].cur_jobs)):
                    same_list = False
                else:
                    for i in range(len(new_cur_jobs)):
                        if (new_cur_jobs[i] != users[user].cur_jobs[i]):
                            same_list = False
                            break

                if (same_list):
                    status = 0
                    msg = 'User exists.'
                else:
                    status = 1
                    msg = 'User exists, but with a different job list.'

            else:
                allowed_users = []
                try:
                    f = open( allowed_users_file, 'r')
                    for line in f:
                        allowed_users.append( line.strip())
                    f.close()
                except:
                    pass
                if (user in allowed_users):
                    status = 0
                    new_user = USER(user, address, port, conn_key)
                    for job in new_cur_jobs:
                        new_user.add_job( job)
                    users[user] = new_user

                    if (distribute_cores() != 0):
                        msg = 'User added. Cores distribution failed.'
                    else:
                        msg = 'User added.'
                else:
                    status = 2
                    msg = 'Not allowed user.'

            for user in users:
                write_conn_files(user_conn_file+user, 
                                 users[user].address,
                                 users[user].port,
                                 users[user].conn_key)

        # Add a job
        # arguments = (user_name, jobID, n_cores, mem, queue_size)
        elif (action_type == MULTIUSER_REQ_CORE):
            user       = arguments[0] # str
            jobID      = arguments[1] # int
            n_cores    = arguments[2] # int
            mem        = arguments[3] # float
            queue_size = arguments[4] # int
            try:
                status = users[user].request_node( jobID, n_cores, mem)
                if (isinstance( status, str)): # The node name
                    msg = status
                    status = 0
                    users[user].n_queue = queue_size - 1
                else:
                    users[user].n_queue = queue_size
                    if (status == 1):
                        msg = 'No node with this requirement.'
                    elif (status == 2):
                        msg = 'No free cores.'
            except KeyError:
                status = -1
                msg = 'User does not exists.'
            except Exception as ex:
                status = -2
                template = "WARNING: An exception of type {0} occured - add a job.\nArguments:\n{1!r}\nContact the qpy-team."
                msg = template.format(type(ex).__name__, ex.args)


        # Remove a job
        # arguments = (user_name, jobID, queue_size)
        elif (action_type == MULTIUSER_REMOVE_JOB):
            user       = arguments[0] # str
            jobID      = arguments[1] # int
            queue_size = arguments[2] # int
            try:
                status = users[user].remove_job( jobID)
                users[user].n_queue = queue_size
                if (status == 0):
                    msg = 'Job removed.'
                elif (status == 1):
                    msg = 'Job not found.'
            except KeyError:
                status = -1
                msg = 'User does not exists.'
            except Exception as ex:
                status = -2
                template = "WARNING: An exception of type {0} occured - remove a job.\nArguments:\n{1!r}\nContact the qpy-team."
                msg = template.format(type(ex).__name__, ex.args)


        # Unknown option
        else:
            status = -1
            msg = 'Unknown option: ' + str( action_type)


        # Send message back
        try:
            client.send((status, msg))
        except:
            # TODO: print exception in a log file
            continue


class CHECK_NODES(threading.Thread):

    """Checks the nodes regularly

    Contains:
    finish            an Event that should be set to terminate this Thread

    This Thread enter in the nodes regularly and checks if thuy are up,
    their memory and outsiders jobs

    This check is done at each nodes_check_time seconds

    """

    def __init__(self):
        """Initialte the class"""
        threading.Thread.__init__(self)
        self.finish = threading.Event()

    def run(self):
        """Checks the nodes."""
        global N_outsiders
        while not self.finish.is_set():
            nodes_info = {}
            try:
                for node in nodes:
                    logging.info("checking %s",node)
                    nodes_info[node] = nodes[node].check()
                    logging.info("done with %s",node)
                nodes_check_lock.acquire()
                N_outsiders += nodes_info[node].n_outsiders - nodes[node].n_outsiders
                for node in nodes:
                    nodes[node].is_up = nodes_info[node].is_up
                    nodes[node].n_outsiders = nodes_info[node].n_outsiders
                    nodes[node].total_mem = nodes_info[node].total_mem
                    nodes[node].free_mem_real = nodes_info[node].free_mem_real
                nodes_check_lock.release()
            except:
                logging.exception("Error in CHECK_NODES")
            self.finish.wait(nodes_check_time)


# ---------------------------------------
load_nodes()
load_users()

check_nodes = CHECK_NODES()
check_nodes.start()

try:
    handle_client()
except:
    logging.error('Exception at handle_client:'  + repr(sys.exc_info()[0]) + ',' + repr(sys.exc_info()[1]) + ',' + repr(sys.exc_info()[1]))


logging.info('Finishing main thread of qpy-multiuser')
check_nodes.finish.set()

