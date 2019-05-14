""" qpy - Classes and functions related to nodes

"""
import sys
from collections import namedtuple
import threading

from qpy_parser import parse_node_info
import qpy_system as qpysys
import qpy_logging as qpylog
import qpy_communication as qpycomm

class UsersNode(object):
    """A node from the point of view of a qpy user
    
    Attributes:
    name (str)        Node name
    address (str)     Node address (hostname, where one can ssh)
    """
    __slots__ = (
        'name',
        'address')

    def __init__(self, name, address):
        """Initialise the class"""
        self.name = name
        self.address = address

    def __str__(self):
        """String representation: just the name"""
        return self.name

    def __repr__(self):
        """Full representation: <name>=<address>"""
        return self.name + '=' + self.address

    @classmethod
    def from_string(cls, string):
        """Return object from string, as "<name>=<address>"; see __repr__."""
        try:
            name, address = string.split('=')
        except:
            name = address = string
        return cls(name, address)

    
class Node(object):
    """A node from the qpy-multiuser point of view.
    
    Attributes:
    
    name (str)               Name for the node
    address (str)            Address of the node
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
    load (float)             The current load of the node, according to top
    total_disk (float)       The total amount of available disk space on TMPDIR
    free_disk (float)        The current free amount of available disk space on TMPDIR
    logger (Logger)          A logger
    """
    __slots__ = ('name',
                 'address',
                 'max_cores',
                 'messages',
                 'is_up',
                 'n_used_cores',
                 'pref_multicores',
                 'req_mem',
                 'total_mem',
                 'free_mem_real',
                 'n_outsiders',
                 'attributes',
                 'load',
                 'total_disk',
                 'free_disk',
                 'logger')
    def __init__(self, name, max_cores, logger):
        self.name = name
        self.address = name
        self.max_cores = max_cores
        self.messages = qpylog.Messages()
        self.messages.save = True
        self.is_up = False
        self.n_used_cores = 0
        self.pref_multicores = False
        self.req_mem = 2.0
        self.total_mem = 0.0
        self.free_mem_real = 0.0
        self.n_outsiders = 0
        self.attributes = []
        self.load = 0.0
        self.total_disk = 0.0
        self.free_disk = 0.0
        self.logger = logger

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
        info = namedtuple('nodeInfo',
                          ['is_up',
                           'n_outsiders',
                           'total_mem',
                           'free_mem_real',
                           'load',
                           'total_disk',
                           'free_disk'])
        info.is_up = True

        command = "top -b -n1"# | sed -n '8,50p'"
        try:
            (std_out, std_err) = qpycomm.node_exec(self.address,
                                                   command)
        except:
            self.logger.exception(
                "finding the number of untracked jobs failed for node: %s",
                self.name)
            self.messages.add('outsiders: Exception: ' + repr(sys.exc_info()[0]))
            info.is_up = False
            info.n_outsiders = 0
            info.load = 0
        else:
            std_out = std_out.split("\n")
            n_jobs = 0
            # slightly improved parsing of top output
            # read "load" from header and get running processes from the output
            # that follows after the line starting with "PID  USER  ..."
            start_count = 0
            for line in std_out:
                line_spl = line.split()
                # read load from header
                if start_count == 0:
                   if len(line_spl) > 9:
                       try:
                           load_index = line_spl.index('load')
                       except ValueError:
                           pass
                       else:
                           info.load = float(line_spl[load_index+3].replace(',',''))
                   if len(line_spl) > 2 and line_spl[0] == 'PID' and line_spl[1] == 'USER':
                      start_count = 1  # start counting jobs from next line on
                else:
                   if float(line_spl[8].replace(',','.')) > 50:
                       n_jobs += 1
                   else:
                       break
            info.n_outsiders = max(n_jobs - self.n_used_cores, 0)

        command = "free -g"
        try:
            (std_out, std_err) = qpycomm.node_exec(self.address,
                                                   command)
        except:
            self.logger.exception(
                "finding the the free memory failed for node: %s",
                self.name)
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
            self.logger.info("node %s is up",self.name)
        command = "df -BG `dirname $TMPDIR`" # probably too specific assumption
        try:
            (std_out, std_err) = qpycomm.node_exec(self.address, command)
        except:
            self.logger.exception("finding the the free disk space failed for node: %s",self.name)
            self.messages.add('disk: Exception: ' + repr(sys.exc_info()[0]))
            info.is_up = False
            info.free_disk = 0.0
            info.total_disk = 0.0
        else:
            self.logger.debug('on '+self.name+': finding this: '+ std_out)
            std_out = std_out.split("\n")
            if len(std_out)>1:
               info.total_disk = float(std_out[1].split()[1].replace('G',''))
               info.free_disk = float(std_out[1].split()[3].replace('G',''))
            else:
               self.logger.error("parsing the df command failed for node: %s",self.name)
               info.total_disk = 0.0
               info.free_disk = 0.0

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
        expression = map(lambda x:
                         x if x in keywords else str(x in self.attributes),
                         expression)
        try:
            a = eval(' '.join(expression))
        except:
            a = True
        self.logger.debug('In has_attributes: node_attr = '
                            + str(node_attr))
        self.logger.debug('In has_attributes: expression = '
                            + str(expression))
        self.logger.debug('In has_attributes: result (Node) = '
                            + str(a) + str(self.name))
        return a


class NodesCollection(object):
    """Store information about all nodes
    
    TODO: use an iterator, but check thread safety before
    """
    
    __slots__ = (
        'names',
        'all_',
        'check_lock',
        'check_alive',
        'check_time',
        'N_cores',
        'N_min_cores',
        'N_used_cores',
        'N_used_min_cores',
        'N_outsiders',
        'logger')
    
    def __init__(self, logger):
        """Initilise the class
        """
        self.all_ = {}
        self.names = []
        self.check_lock = threading.RLock()
        self.check_alive = True
        self.check_time = 300
        self.N_cores = 0
        self.N_min_cores = 0
        self.N_used_cores  = 0
        self.N_used_min_cores = 0
        self.N_outsiders = 0
        self.logger = logger

    def load_nodes(self):
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
        try:
            f = open(qpysys.nodes_file, 'r')
        except:
            return -1
        nodes_in_file = []
        cores_in_file = []
        addresses_in_file = []
        attr_in_file = []
        nodes_for_multicores = []
        for line in f:
            try:
                name, n_cores, address, multicore, attributes = parse_node_info(line)
            except:
                self.logger.exception('Exception when parsing node info:')
                f.close()
                return -2
            nodes_in_file.append(name)
            addresses_in_file.append(address)
            cores_in_file.append(n_cores)
            if multicore:
                nodes_for_multicores.append(name)
            attr_in_file.append(attributes)
        f.close()
        # Put messages
        for i in range(len(nodes_in_file)):
            n = nodes_in_file[i]
            c = cores_in_file[i]
            if (n in self.all_):
                self.N_cores += c - self.all_[n].max_cores
                self.all_[n].max_cores = c
                self.all_[n].attributes = attr_in_file[i]
            else:
                new_node = Node(n, c, self.logger)
                new_node.address = addresses_in_file[i]
                new_node.attributes = attr_in_file[i]
                self.all_[n] = new_node
                self.N_cores += new_node.max_cores
            if 'M' in self.all_[n].attributes:
                self.all_[n].attributes.remove('M')
        nodes_to_remove = []
        names = []
        for n in self.all_:
            if n in nodes_in_file:
                if n in nodes_for_multicores:
                    self.all_[n].pref_multicores = True
                    names.insert(0, n)
                else:
                    self.all_[n].pref_multicores = False
                    names.append(n)
            else:
                nodes_to_remove.append(n)
        for n in nodes_to_remove:
            self.N_cores -= self.all_[n].max_cores
            self.all_.pop(n)
        return 0

    
class CheckNodes(threading.Thread):
    """Check the nodes regularly.
    
    Attributes:
    finish            an Event that should be set to terminate this Thread
    
    This Thread enters in the nodes regularly and checks if they are up,
    their memory, and outsiders jobs
    
    This check is done at each (global) check_time seconds
    """

    __slots__ = (
        'finish',
        'all_nodes',
        'logger')
    
    def __init__(self, all_nodes, logger):
        """Initialte the class"""
        threading.Thread.__init__(self)
        self.finish = threading.Event()
        self.all_nodes = all_nodes
        self.logger = logger

    def run(self):
        """Checks the nodes."""
        while not self.finish.is_set():
            nodes_info = {}
            try:
                for node in self.all_nodes.all_:
                    self.logger.info("checking %s",node)
                    nodes_info[node] = self.all_nodes.all_[node].check()
                    self.logger.info("done with %s",node)
                with self.all_nodes.check_lock:
                    self.all_nodes.N_outsiders += (nodes_info[node].n_outsiders
                                                   - self.all_nodes.all_[node].n_outsiders)
                    for node in self.all_nodes.all_:
                        self.all_nodes.all_[node].is_up = nodes_info[node].is_up
                        self.all_nodes.all_[node].n_outsiders = nodes_info[node].n_outsiders
                        self.all_nodes.all_[node].total_mem = nodes_info[node].total_mem
                        self.all_nodes.all_[node].free_mem_real = nodes_info[node].free_mem_real
                        self.all_nodes.all_[node].load = nodes_info[node].load
                        self.all_nodes.all_[node].total_disk = nodes_info[node].total_disk
                        self.all_nodes.all_[node].free_disk = nodes_info[node].free_disk
            except:
                self.logger.exception("Error in CHECK_NODES")
            self.finish.wait(self.all_nodes.check_time)

