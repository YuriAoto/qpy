# qpy - control the nodes distribution for several users
#
# 26 December 2015 - Pradipta and Yuri
# 06 January 2016 - Pradipta and Yuri
from multiprocessing.connection import Listener
from time import sleep
import os
import sys
import subprocess
import re
import math
from optparse import OptionParser
import threading
from qpy_general_variables import *

qpy_source_dir = os.path.dirname( os.path.abspath( __file__)) + '/'
test_run = os.path.isfile( qpy_source_dir + 'test_dir')

if (test_run):
    qpy_multiuser_dir = os.path.expanduser( '~/.qpy-multiuser-test/')
else:
    qpy_multiuser_dir = os.path.expanduser( '~/.qpy-multiuser/')

if (not( os.path.isdir( qpy_multiuser_dir))):
    os.makedirs( qpy_multiuser_dir)

nodes_file = qpy_multiuser_dir + 'nodes'
allowed_users_file = qpy_multiuser_dir + 'allowed_users'
cores_distribution_file = qpy_multiuser_dir + 'distribution_rules'

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

multiuser_address = 'localhost'
multiuser_key = 'zxcvb'
if (test_run):
    multiuser_port = 9998
else:
    multiuser_port = 9999

parser = OptionParser()
parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="print messages")
(options, args) = parser.parse_args()
verbose = options.verbose


# Node informations
class NODE():
    def __init__( self, name, max_cores):
        self.name = name
        self.is_up = self.is_ssh_working()
        self.max_cores = max_cores
        self.n_used_cores = 0
        self.n_outsiders = 0
        self.pref_multicores = False
        self.total_mem = 0
        self.free_mem = 0
        self.free_mem_real = 0

        if (self.is_up):
            self.memory()
            self.free_mem = self.total_mem - 5.0

    # Check if the <command> sent by ssh to <address> return the <exp_out> message without errors
    def is_ssh_working( self):
        ssh = subprocess.Popen(["ssh", "-o", "StrictHostKeyChecking=no", self.name, 'hostname'],
                                 shell=False,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        std_outerr = ssh.communicate()
        out = std_outerr[0]
        err = std_outerr[1]
        return not ('No route to host' in err or 'Connection refused' in err or 'Could not resolve hostname' in err) and (self.name in out)


    # Get the really used memory
    def memory( self):
        command = "free -g"
        mem_details = subprocess.Popen(["ssh", self.name , command],
                                       shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
        std_outerr = mem_details.communicate()
        mem_stdout = std_outerr[0].split( '\n')
        self.total_mem = float(mem_stdout[1].split()[1])
        if (len(mem_stdout) == 5):
            self.free_mem_real = float(mem_stdout[2].split()[3])
        else:
            self.free_mem_real = float(mem_stdout[1].split()[6])
        return self.free_mem_real

    # Get the number of cores used by outsiders
    def outsiders( self):

        try:
            command = "top -b -n1 | sed -n '8,50p'"
            free_details = subprocess.Popen(["ssh", self.name , command],
                                            shell=False,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
            std_outerr = free_details.communicate()
            ssh_stdout = std_outerr[0].split( '\n')
            n_jobs = 0
            for line in ssh_stdout:
                line_spl = line.split()
                if float(line_spl[8].replace(',','.')) > 50:
                    n_jobs += 1
                else:
                    break

            self.n_outsiders = max(n_jobs - self.n_used_cores, 0)

        except:
            self.n_outsiders = 0

        return self.n_outsiders


# User informations
# self.min_cores    -> only for the user
# self.extra_cores  -> preferably for the user
# self.max_cores    -> maximum that can be used: N_cores - min_cores (of other users)
# self.n_used_cores -> current number of used cores
#
class USER():
    def __init__( self, name):
        self.name = name
        self.min_cores = 0
        self.extra_cores = 0
        self.max_cores = 0
        self.n_used_cores = 0
        self.n_queue = 0
        self.cur_jobs = [] # each job = (jobID, node, num_cores, mem)

    # Add a job, if possible
    def add_job( self, jobID, num_cores, mem):
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
                    if (not(info.pref_multicores) and free > best_free and info.free_mem > mem and info.free_mem_real > mem):
                        best_node = node
                        best_free = free
            if (best_node == None):
                for node in nodes_list:
                    if (nodes[node].is_up and
                        nodes[node].max_cores  - nodes[node].n_outsiders - nodes[node].n_used_cores >= num_cores and
                        nodes[node].free_mem > mem):
                        best_node = node
                        break
            nodes_check_lock.release()

            if (best_node == None):
                return 1

            self.cur_jobs.append( (jobID, best_node, num_cores, mem))
            self.n_used_cores += num_cores
            nodes[best_node].n_used_cores += num_cores
            nodes[best_node].free_mem -= mem
            N_used_cores += num_cores

            if (self.n_used_cores < self.min_cores):
                N_used_min_cores += num_cores
            elif (self.n_used_cores < self.min_cores + num_cores):
                N_used_min_cores += num_cores + self.min_cores - self.n_used_cores

            return best_node

        return 2

    # Remove the job
    def remove_job( self, jobID):
        global N_used_cores, N_used_min_cores
        for job in self.cur_jobs:
            if (job[0] == jobID):
                self.n_used_cores -= job[2]
                nodes[job[1]].n_used_cores -= job[2]
                nodes[job[1]].free_mem += job[3]
                N_used_cores -= job[2]
                if (self.n_used_cores < self.min_cores):
                    N_used_min_cores -= min( job[2], self.min_cores - self.n_used_cores)
                self.cur_jobs.remove( job)
                return 0
        return 1


# Load nodes file
def load_nodes():
    global N_cores
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
    if (verbose):
        print "load_nodes:"
        print "nodes: ", nodes_in_file
        print "cores: ", cores_in_file
        print "nodes for multi: ", nodes_for_multicores
    for n, c in zip( nodes_in_file, cores_in_file):
        if (n in nodes):
            N_cores += c - nodes[n].max_cores
            nodes[n].max_cores = c
        else:
            new_node = NODE( n, c)
            nodes[n] = new_node
            N_cores += new_node.max_cores
    nodes_to_remove = []
    global nodes_list
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


# Distribute cores
def distribute_cores( ):
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
    if (verbose):
        print 'distribute_cores:'
        print 'dist_type: ' + dist_type
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
    if (verbose):
        print 'users_min:   ', users_min
        print 'users_extra: ', users_extra
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


# Get action from client and execute it
# message from client must be:
#
#    int          tuple
#   (action_type, arguments)
#              
#  'arguments' is option dependent. See below.
#  Message sent back:
#
#    int     str
#   (status, msg)
#
def handle_client( check_nodes):
    global N_cores, N_used_cores
    global N_min_cores, N_used_min_cores
    if (verbose):
        print "handle_client: ready"
    conn = Listener(( multiuser_address, multiuser_port), authkey = multiuser_key)
    while True:
        try:
            client = conn.accept()
            (action_type, arguments) = client.recv()
        except:
            # TODO: print exception in a log file
            continue
        if (verbose):
            print "Received: " + str(action_type) + " -> " + str(arguments)


        # Reload the nodes
        # arguments = ()
        if (action_type == MULTIUSER_NODES):
            status = load_nodes()
            if (status == 0):
                msg = 'Nodes loaded.'
            elif (status == -1):
                msg = 'Nodes loading failed. Problem when openning ' + nodes_file + '.'
            elif (status == -2):
                msg = 'Nodes loading failed. Check ' + nodes_file + '.'
            else:
                msg = 'Nodes loading failed.'


        # Redistribute cores
        # arguments = ()
        elif (action_type == MULTIUSER_DISTRIBUTE):
            status = distribute_cores()
            if (status == 0):
                msg = 'Cores distributed.'
            elif (status == -1):
                msg = 'Cores distribution failed. Problem when openning ' + cores_distribution_file + '.'
            elif (status == -2):
                msg = 'Cores distribution failed. Check ' + cores_distribution_file + '.'
            elif (status == -3):
                msg = 'Cores distribution failed. Not enough cores.'
            else:
                msg = 'Cores distribution failed.'


        # Show important variables
        # arguments = ()
        elif (action_type == MULTIUSER_SHOW_VARIABLES):
            status = 0
            nodes_check_lock.acquire()
            msg = ''
            msg += 'N_cores          = ' + str( N_cores)           + '\n'
            msg += 'N_min_cores      = ' + str( N_min_cores)       + '\n'
            msg += 'N_used_cores     = ' + str( N_used_cores)      + '\n'
            msg += 'N_used_min_cores = ' + str( N_used_min_cores)  + '\n'
            msg += 'N_outsiders      = ' + str( N_outsiders)       + '\n'
            msg += 'users:\n'
            for user, info in users.iteritems():
                msg += user + ':\n'
                msg += '  min_cores    = ' + str( info.min_cores)    + '\n'
                msg += '  extra_cores  = ' + str( info.extra_cores)  + '\n'
                msg += '  max_cores    = ' + str( info.max_cores)    + '\n'
                msg += '  n_used_cores = ' + str( info.n_used_cores) + '\n'
                msg += '  n_queue      = ' + str( info.n_queue)      + '\n'
                msg += '  cur_jobs     :' + '\n'
                for cj in info.cur_jobs:
                    msg += '          ' + repr( cj) + '\n'
            msg += 'nodes:\n'
            for node, info in nodes.iteritems():
                msg += node + ':\n'
                msg += '  max_cores       = ' + str( info.max_cores)       + '\n'
                msg += '  n_used_cores    = ' + str( info.n_used_cores)    + '\n'
                msg += '  n_outsiders     = ' + str( info.n_outsiders)     + '\n'
                msg += '  total_mem       = ' + str( info.total_mem)       + '\n'
                msg += '  free_mem        = ' + str( info.free_mem)        + '\n'
                msg += '  free_mem_real   = ' + str( info.free_mem_real)   + '\n'
                msg += '  pref_multicores = ' + str( info.pref_multicores) + '\n'
                msg += '  is_up           = ' + str( info.is_up) + '\n'
            nodes_check_lock.release()

        # Show status
        # arguments = () or (user_name)
        elif (action_type == MULTIUSER_STATUS):

            sep1 = '-'*60 + '\n'
            sep2 = '='*60 + '\n'
            headerN =  '                       cores              memory (GB)\n'
            headerN += 'node                used  total      used     req   total\n'
            headerU =  'user                using cores        queue size\n' + sep1

            nodes_ordered = []
            for node in nodes:
                nodes_ordered.append( node)
            nodes_ordered.sort()

            users_ordered = []
            for user in users:
                users_ordered.append( user)
            users_ordered.sort()

            status = 0

            msgU = ''
            format_spec = '{0:22s} {1:<5d}' + ' '*13 + '{2:<5d}\n'
            for user in users_ordered:
                msgU += format_spec.format( user,
                                            users[user].n_used_cores,
                                            users[user].n_queue)
            if (msgU):
                msgU = headerU + msgU + sep2
            else:
                msgU = 'No users.\n'

            msgN = ''
            format_spec = '{0:20s} {1:<5d} {2:<5d}' + ' '*2 + '{3:>7.1f} {4:>7.1f} {5:>7.1f}\n'
            nodes_check_lock.acquire()
            for node in nodes_ordered:
                down=' (down)' if not( nodes[node].is_up) else ''
                msgN += format_spec.format( node + down,
                                            nodes[node].n_used_cores + nodes[node].n_outsiders,
                                            nodes[node].max_cores,
                                            nodes[node].total_mem - nodes[node].free_mem_real,
                                            nodes[node].total_mem - nodes[node].free_mem,
                                            nodes[node].total_mem)
            nodes_check_lock.release()
            if (msgN):
                msgN = sep1 + msgN + sep2
                msgN = headerN + msgN
            else:
                msgN = 'No nodes.\n'

            msg = msgU + msgN
            msg += 'There are ' + str( N_used_cores + N_outsiders) + ' out of a total of ' + str( N_cores) + ' cores being used.\n'


        # Finish qpy-multiuser
        # arguments = ()
        elif (action_type == MULTIUSER_FINISH):
            check_nodes.finish.set()
            client.send( (0, 'Finishing qpy-multiuser.'))
            client.close()
            break


        # Add a user
        # arguments = (user_name[, cur_jobs])
        elif (action_type == MULTIUSER_USER):
            user = arguments[0] # str
            if (len( arguments) == 2):
                new_cur_jobs = arguments[1] # list (as USER.cur_jobs)
            else: 
                new_cur_jobs = None
            if (user in users):
                status = 0
                msg = ['User exists.']
            else:
                # Add allowed users
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
                    new_user = USER( user)
                    users[user] = new_user
                    if (distribute_cores() != 0):
                        msg = 'User added. Cores distribution failed.'
                    else:
                        msg = 'User added.'
                else:
                    status = 1
                    msg = 'Not allowed user.'
            # replace the cur_jobs list
            if (user in users and new_cur_jobs != None ):
                N_used_cores -= users[user].n_used_cores
                N_used_min_cores -= min( users[user].min_cores, users[user].n_used_cores)
                for job in users[user].cur_jobs:
                    nodes[job[1]].n_used_cores -= job[2]
                    nodes[job[1]].free_mem += job[3]
                users[user].cur_jobs = new_cur_jobs
                users[user].n_used_cores = 0
                for job in users[user].cur_jobs:
                    nodes[job[1]].n_used_cores += job[2]
                    if (len(job) > 3):
                        nodes[job[1]].free_mem -= job[3]
                    else: # for old version of qpy-mster
                        nodes[job[1]].free_mem -= 5.0
                    users[user].n_used_cores += job[2]
                N_used_cores += users[user].n_used_cores
                N_used_min_cores += min( users[user].min_cores, users[user].n_used_cores)


        # Add a job
        # arguments = (user_name, jobID, n_cores, mem, queue_size)
        elif (action_type == MULTIUSER_REQ_CORE):
            user       = arguments[0] # str
            jobID      = arguments[1] # int
            n_cores    = arguments[2] # int
            mem        = arguments[3] # float
            queue_size = arguments[4] # int
            try:
                status = users[user].add_job( jobID, n_cores, mem)
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
            client.send( (status, msg))
        except:
            # TODO: print exception in a log file
            continue





# Check nodes temporarily
class CHECK_NODES( threading.Thread):
    def __init__( self):
        threading.Thread.__init__( self)
        self.finish = threading.Event()

    def run( self):
        global N_outsiders

        while not self.finish.is_set():
            for node in nodes:

                nodes_check_lock.acquire()

                nodes[node].is_up = nodes[node].is_ssh_working()
                if (nodes[node].is_up):
                    N_outsiders -= nodes[node].n_outsiders
                    nodes[node].outsiders()
                    N_outsiders += nodes[node].n_outsiders
                    nodes[node].memory()

                nodes_check_lock.release()

            sleep( nodes_check_time)


load_nodes()

check_nodes = CHECK_NODES()
check_nodes.daemon = True
check_nodes.start()

handle_client( check_nodes)
