"""
qpy - control the nodes distribution for several users

History:
  26 December 2015 - Pradipta and Yuri
  06 January 2016 - Pradipta and Yuri
"""
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
import logging.handlers
import traceback

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
multiuser_log_file = qpy_multiuser_dir + 'multiuser.log'

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


logger = configure_root_logger(multiuser_log_file, logging.WARNING)

class NODE():
    """A node from the qpy-multiuser point of view.

    Attributes:

    name (str)               Name for the node
    max_cores (int)          Total number of cores this node has available
    messages (Messages)      Messages
    is_up (bool)             If True, the node is up, accessible and running
                             If False, if means that ssh could not reach it
    n_used_cores (int)       Number of cores that are being used in the moment
    pref_multicores (bool)   If True, it means that this core is preferred format
                             multicores jobs
    req_mem (float)          Total memory requested by the jobs that are running
    total_mem (float)        Total memory available in this node (obtained by
                             system's command free)
    free_mem_real (float)    Free memory on this node ((obtained by system's
                             command free)
    n_outsiders (int)        Number of jobs that are running in this node that
                             are not under qpy control (obtained by system's
                             commant top)
    attributes (list)        A list, with the attributes of this node
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

        self.attributes = []

    def check(self):
        """Check several things in the node.

        Returns a named tuple with the information:

        is_up           True if the node is up
        n_outsiders     the number of cores used by non-qpy processes
        total_mem       total memory of the node
        free_mem_real   free memory of node

        This function DOES NOT change the attributes of
        the node (except for adding messages), what should
        be done by the caller if desired.
        """
        info = namedtuple('nodeInfo', ['is_up','n_outsiders','total_mem','free_mem_real'])
        info.is_up = True

        command = "top -b -n1"# | sed -n '8,50p'"
        try:
            (std_out, std_err) = node_exec(self.name, command)
            # dumb: should be improved
            std_out = '\n'.join(std_out.split('\n')[8:max(len(std_out),50)])
        except:
            logger.exception("finding the number of untracked jobs failed for node: %s",self.name)
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
            logger.exception("finding the the free memory failed for node: %s",self.name)
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
            logger.info("node %s is up",self.name)
        return info

    def has_attributes(self, node_attr):
        """Check if the node satisfy the attributes requirement

        Arguments:
        node_attr (list)   A logical expression about attributes

        Return True if the node has the attributes described by
        the list node_attr. It should be a list of strings, such
        that after joining each entry with space and replacing the
        attributes by True or False, a valid python logical expression
        is obtained.

        Return True if is not a valid expression.
        """
        keywords = ['not', 'and', 'or', '(', ')']
        if len(node_attr) == 0:
            return True
        expression = (' '.join(node_attr)).replace('(', ' ( ').replace(')', ' ) ')
        expression = expression.split()
        expression = map(lambda x: x if x in keywords else str(x in self.attributes), expression)
        try:
            a = eval(' '.join(expression))
        except:
            a = True

        logger.debug('In has_attributes: node_attr = ' + str(node_attr))
        logger.debug('In has_attributes: expression = ' + str(expression))
        logger.debug('In has_attributes: result (Node) = ' + str(a) + str(self.name))

        return a


class USER():
    """A user from the qpy-multiuser point of view.

    This class contains the main functions to handle the interaction
    of the user within qpy-multiuser. In particular, it grants the usage
    of cores to the qpy-master of the user.

    Atributes:
    name          The user
    address       Address to the qpy-master connection
    port          Port to the qpy-master connection
    conn_key      Key to the qpy-master connection

    min_cores     Number of cores guaranteed to be available to the user
    extra_cores   Number of cores to be used preferably by the user
    max_cores     Maximum number of cores that can be used by this user
                  in any time = (N_cores - min_cores of all other users)
    n_used_cores  Current number of used cores
    n_queue       Number of jobs in the queue of the qpy-master of this user
    cur_jobs      Current jobs

    messages      Debugging messages
    """

    def __init__( self, name, address, port, conn_key):
        """Initiate the class
        
        Arguments:
        name       The user
        address    Address to the qpy-master connection
        port       Port to the qpy-master connection
        conn_key   Key to the qpy-master connection
        """
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
        
        self.messages = Messages()
        self.messages.save = True


    def remove_job( self, jobID):
        """Remove a job from the user and from the nodes.
        
        Arguments:
        jobID     The ID number of the job
        
        Return 0 if success or 1 if the job was not found
        """
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
        """Add a new job to a specific node.
        
        Arguments:
        job     The job, as an instance of MULTIUSER_JOB
        
        Return 0
        """
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


    def request_node( self, jobID, num_cores, mem, node_attr):
        """Tries to give a space to the user
        
        Arguments:
        jobID       The job ID
        num_cores   Requested number of cores
        mem         Requested memory
        node_attr   Attributes that the node must fulfil
        
        This is the main function of qpy-multiuser.
        It decides whether the requested resource can be
        given or not, and, if so, give a node that fulfils
        this requirement.
        
        How it works:
        
        General idea:
        i) If the user is using less than its min_cores and
        the number of cores he is requesting is still within
        min_cores, he should receive the cores
        
        ii) If the user is using more than min_cores, and there
        are still free cores (without considering the min_cores
        of all other users, that should be free if not in use),
        he should receive the cores if 1) He is using less than
        the extra cores he has or 2) no other user has jobs in
        their queues
        
        This means that, if there is available resource,
        he should receive cores, unless this will use the
        min_cores of other users. If the resource is limited,
        and several users have jobs in their queues, the mumber
        of used cores by each user should stay around
        min_cores + extra cores
        """
        global N_used_cores, N_used_min_cores
        space_available = False
        
        logger.debug('jobID, attribute: '+ str(jobID) + str(node_attr) )
        
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
                    if (info.has_attributes(node_attr) and
                        not(info.pref_multicores) and
                        free > best_free and
                        (info.total_mem-info.req_mem) > mem and
                        info.free_mem_real > mem):
                        best_node = node
                        best_free = free
            if (best_node == None):
                for node in nodes_list:
                    if (nodes[node].is_up and
                        info.has_attributes(node_attr) and
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

    For each user given in the file of the global variable allowed_users,
    attempt to obtain the port and the key for the connection with their
    qpy-master (from MULTIUSER_HANDLER) and request the current running jobs.
    It also redistributes the cores.
    
    The file should contain just the username of the users, one in each line.
    """
    global users
    allowed_users = []
    f = open( allowed_users_file, 'r')
    for line in f:
        allowed_users.append( line.strip())
    f.close()
    for user in allowed_users:
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
    """Load the nodes.
    
    The nodes are given in the file on the global variable
    nodes_file.
    
    The file should be formatted as:
    
    <node1> <n cores node1> [M] [<node1, attr1> [<node1, attr2> ...]]
    <node1> <n cores node1> [M] [<node2, attr1> [<node2, attr2> ...]]
    
    Each line of the file starts with the node name to be loaded.
    The node name is followed by the number of cores that this node
    has available and, optionally, a sequence of attributes of this
    node. In particular, the attribute "M" means that this node is
    preferred for multicores jobs. The attributes are strings that
    can be used to select or avoid particular nodes by the user,
    see USER.request_node.
    """
    global N_cores
    global nodes_list
    try:
        f = open( nodes_file, 'r')
    except:
        return -1
    nodes_in_file = []
    cores_in_file = []
    attr_in_file = []
    nodes_for_multicores = []
    for line in f:
        line_spl = line.split()
        try:
            nodes_in_file.append( line_spl[0])
            cores_in_file.append( int( line_spl[1]))
        except:
            f.close()
            return -2
        if ('M' in line_spl[2:]):
            nodes_for_multicores.append( line_spl[0])
        if len(line_spl) > 2:
            attr_in_file.append(line_spl[2:])
        else:
            attr_in_file.append([])
    f.close()
    # Put messages
    for i in range(len(nodes_in_file)):
        n = nodes_in_file[i]
        c = cores_in_file[i]
        if (n in nodes):
            N_cores += c - nodes[n].max_cores
            nodes[n].max_cores = c
            nodes[n].attributes = attr_in_file[i]
        else:
            new_node = NODE( n, c)
            new_node.attributes = attr_in_file[i]
            nodes[n] = new_node
            N_cores += new_node.max_cores
        if 'M' in nodes[n].attributes:
            nodes[n].attributes.remove('M')
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
    """Distribute the cores.
    
    The distribution is made based on the file in the
    global variable cores_distribution_file.
    
    The file should de formated as:
    
    <dist type = even, explicit> [minimum <n min cores>]
    [<user_1>=[<user_1 min>+]<user_1 extra>[%]]
    [<user_2>=[<user_2 min>+]<user_2 extra>[%]]
    ...
    
    That is: the first line starts with "even" or "explicit"
    and is followed by, optionally, bu "minimum" and a number.
    If "even" is given, the cores are distributed equally among
    all users. If "explicit is given, the number of cores
    of each user must be given explicitly in the following lines.
    If "minimum" is also passed, the number that follows is the
    minimum number of cores the users will have (that is only
    their and is guaranteed).
    
    The lines that describe the number of cores of each
    user should start with the user name and an equal, "="
    sign. After the "=" comes the number of extra cores that
    that user has. If it comes followed by an "%" sign, it means
    the percentage of total cores (except the all minimum cores),
    otherwise it means directly the number of extra cores.
    Before the number of extra cores, the number of minimum cores
    for that user can be passed (separated by an "+" sign),
    that has priority over the value given in the first line.
    
    Any remaining cores are shared as equally as possible as extra
    to all users.
    
    See file examples/distribution_rules for some examples.
    
    Return:
    0    success
    -1   if file not found
    -2   for any syntax error in the file
    -3   if exceed the number of cores
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
                    return -2
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
    }.get(status, 'Cores distribution failed.')

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
    return '  Last messages:\n' + str(messages) + '----------'

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
        ('attributes',info.attributes),
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
    sep1 = '-'*70 + '\n'
    sep2 = '='*70 + '\n'
    headerN =  '                                 cores              memory (GB)\n'
    headerN += 'node                          used  total      used     req   total\n'
    headerU =  'user                          using cores        queue size\n' + sep1

    msgU = ''
    format_spec = '{0:32s} {1:<5d}' + ' '*13 + '{2:<5d}\n'
    for user in sorted(users):
        msgU += format_spec.format( user,
                                    users[user].n_used_cores,
                                    users[user].n_queue)
    msgU = headerU + msgU + sep2 if msgU else 'No users.\n'

    msgN = ''
    format_spec = '{0:30s} {1:<5d} {2:<5d}' + ' '*2 + '{3:>7.1f} {4:>7.1f} {5:>7.1f}\n'
    with nodes_check_lock:
        for node in nodes:
            down=' (down)' if not( nodes[node].is_up) else ''
            len_node_row = len(down) + len(node) + sum(map(len,nodes[node].attributes)) + \
                len(nodes[node].attributes) + 2
            if len_node_row > 28 or not(nodes[node].attributes):
                attr = ''
            else:
                attr = ' [' + ','.join(nodes[node].attributes) + ']'
            msgN += format_spec.format( node + attr + down,
                                        nodes[node].n_used_cores + nodes[node].n_outsiders,
                                        nodes[node].max_cores,
                                        nodes[node].total_mem - nodes[node].free_mem_real,
                                        nodes[node].req_mem,
                                        nodes[node].total_mem)
            if len_node_row > 28 and nodes[node].attributes:
                msgN += '    [' + ','.join(nodes[node].attributes) + ']\n'
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
    
def handle_sync_user_info(args):
    """handles a request to synchronize user info

    args: user_name, address,port, conn_key, cur_jobs
    """
    user,address,port,conn_key,new_cur_jobs = args
    try:
        if user in users:
            users[user].address = address
            users[user].port = port
            users[user].conn_key = conn_key
            same_list = len(new_cur_jobs) == len(users[user].cur_jobs) \
                        and all(new_job == old_job for new_job, old_job in zip(new_cur_jobs, users[user].cur_jobs))
            return ( 0,'User exists' ) if same_list else (1, 'User exists but with a different job list.')
        else:
            try:
                with open(allowed_users_file,'r') as f:
                    allowed_users =  list(line.strip() for line in f)
            except:
                allowed_users = []
            if user in allowed_users:
                new_user = USER(user, address, port, conn_key)
                for job in new_cur_jobs:
                    new_user.add_job(job)
                users[user] = new_user
                return (0,'User added') if distribute_cores() == 0 else \
                    (0, 'User added. Cores distribution failed.')
            else:
                return 2,'Not allowed user'
    finally:
        for user in users:
            write_conn_files(user_conn_file+user,
                             users[user].address,
                             users[user].port,
                             users[user].conn_key)


def handle_add_job(args):
    """ handles request to add a job

    args: (user_name, jobID, n_cores, mem, queue_size, node_attr)
    """
    if len(args) == 5: # old style. Can be removed when all masters are updated
        user, jobID, n_cores, mem, queue_size = args
        node_attr = []
    else:
        user, jobID, n_cores, mem, queue_size, node_attr = args
    assert isinstance(user,str)
    assert isinstance(jobID,int)
    assert isinstance(n_cores,int)
    assert isinstance(mem,float) or isinstance(mem,int)
    assert isinstance(queue_size,int)
    assert isinstance(node_attr,list)
    try:
        status = users[user].request_node(jobID,n_cores,mem,node_attr)
        logger.debug('I am here: ' + str(status))
        if isinstance(status,str):
            users[user].n_queue = queue_size -1
            return 0,status
        else:
            users[user].n_queue = queue_size
            return (1,'No node with this requirement.') if status == 1 \
                else (2,'No free cores.')
    except KeyError:
        return -1, 'User does not exists.'
    except Exception as ex:
        return -2,"WARNING: An exception of type {0} occured - add a job.\nArguments:\n{1!r}\nContact the qpy-team.".format(type(ex).__name__, ex.args)

def handle_remove_job(args):
    """handles a request to remove a job

    args : (user_name, jobID, queue_size)
    """
    user, jobID, queue_size = args
    assert isinstance(user, str)
    assert isinstance(jobID, int)
    assert isinstance( queue_size, int)
    try:
        status = users[user].remove_job(jobID)
        users[user].n_queue = queue_size
        return status,{0:'Job removed.',
                       1:'Job not found'}[status]
    except KeyError:
        return -1,'User does not exists.'
    except Exception as ex:
        return -2,'WARNING: an exception of type {0} occured - remove a job.\nArguments:\n{1!r}\nContact the qpy-team.'.format(type(ex).__name__, ex.args)

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
        logger.exception("Error when establishing connection. Is there already a qpy-multiuser instance?")
        return
    if conn is None:
        return
    
    while True:
        logger.info("Starting main loop.")
        try:
            client = conn.accept()
            (action_type, arguments) = client.recv()
        except:
            logger.exception("Connection failed")
        else:
            logger.info("Received request: %s arguments:%s",str(action_type), str(arguments))
        try:
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
                status, msg = handle_sync_user_info(arguments)


            # Add a job
            # arguments = (user_name, jobID, n_cores, mem, queue_size)
            elif (action_type == MULTIUSER_REQ_CORE):
                status, msg = handle_add_job(arguments)

            # Remove a job
            # arguments = (user_name, jobID, queue_size)
            elif (action_type == MULTIUSER_REMOVE_JOB):
                status, msg = handle_remove_job(arguments)
            # Unknown option
            else:
                status, msg =  -1, 'Unknown option: ' + str( action_type)
        except Exception as ex:
            logger.exception("An error occured")
            template = 'WARNING: an exception of type {0} occured.\nArguments:\n{1!r}\nContact the qpy-team.'
            try:
                client.send((-10,template.format(type(ex).__name__, ex.args) ))
            except Exception:
                logger.exception("An error occured while returning a message.")
                pass
        except BaseException as ex:
            logger.exception("An error occured")
            template = 'WARNING: an exception of type {0} occured.\nArguments:\n{1!r}\nContact the qpy-team. qpy-multiuser is shutting down.'
            try:
                client.send((-10,template.format(type(ex).__name__, ex.args) ))
            except Exception:
                logger.exception("An error occured while returning a message.")
                pass
            finally:
                break
        else:
            try:
                client.send( (status,msg))
            except:
                logger.exception("An error occured while returning a message.")
                continue


class CHECK_NODES(threading.Thread):
    """Check the nodes regularly.

    Attributes:
    finish            an Event that should be set to terminate this Thread

    This Thread enters in the nodes regularly and checks if they are up,
    their memory, and outsiders jobs

    This check is done at each (global) nodes_check_time seconds
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
                    logger.info("checking %s",node)
                    nodes_info[node] = nodes[node].check()
                    logger.info("done with %s",node)
                nodes_check_lock.acquire()
                N_outsiders += nodes_info[node].n_outsiders - nodes[node].n_outsiders
                for node in nodes:
                    nodes[node].is_up = nodes_info[node].is_up
                    nodes[node].n_outsiders = nodes_info[node].n_outsiders
                    nodes[node].total_mem = nodes_info[node].total_mem
                    nodes[node].free_mem_real = nodes_info[node].free_mem_real
                nodes_check_lock.release()
            except:
                logger.exception("Error in CHECK_NODES")
            self.finish.wait(nodes_check_time)


# ---------------------------------------
load_nodes()
load_users()

check_nodes = CHECK_NODES()
check_nodes.start()

try:
    handle_client()
except:
    logging.exception("Exception at handle_client")

logger.info('Finishing main thread of qpy-multiuser')
check_nodes.finish.set()

