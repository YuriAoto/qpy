""" qpy - Classes and functions related to nodes

"""
import sys
from collections import namedtuple
import threading
import logging

import qpy_system as qpysys
import qpy_logging as qpylog
import qpy_communication as qpycomm
from qpy_exceptions import qpyConnectionError


class UsersNode:
    """A node from the point of view of a qpy user
    
    Attributes:
    -----------
    name (str)
        Node name
    address (str)
        Node address (hostname, where one can ssh)
    """
    __slots__ = (
        'name',
        'address')

    def __init__(self, name, address=None):
        """Initialise the node"""
        self.name = name
        self.address = name if address is None else address

    def __str__(self):
        """String representation: just the name"""
        return self.name

    def __repr__(self):
        """Full representation: <name>=<address>"""
        return self.name + '=' + self.address

    @classmethod
    def from_string(cls, string):
        """Return object from the string '<name>[=<address>]'"""
        try:
            name, address = string.split('=')
        except ValueError:
            name = address = string
        return cls(name, address)


def _check_n_cpu(node_address):
    """Get the number of cpus in the node"""
    command = "nproc"
    msg = ''
    try:
        std_out, std_err = qpycomm.node_exec(node_address, command)
    except Exception as e:
        msg = str(e)
        conn_success = False
        n_cpus = 0
    else:
        conn_success = True
        n_cpus = int(std_out)
    return msg, conn_success, n_cpus


def _check_node_top(node_address, thr_cpu_usage=50):
    """Check some information from "top" on a node
    
    Parameters:
    -----------
    node_address (str)
        The address of the node
    thr_cpu_usage (float)
        The threshold for considering a running proccess
        A proccess is considered when the %CPU is larger
        than this value
    
    Return:
    -------
    The following tuple:
    
    msg, conn_success, load, n_proc
    
    That are, respectivelly, a messege; True or False indicating if
    the connection was successful; the load, and the number of
    processes;
    
    """
    command = "top -b -n1"  # | sed -n '8,50p'"
    msg = ''
    try:
        std_out, std_err = qpycomm.node_exec(node_address, command)
    except Exception as e:
        msg = str(e)
        conn_success = False
        load = 0
        n_outsiders = 0
    else:
        conn_success = True
        std_out = std_out.split("\n")
        n_jobs = 0
        start_count = 0
        for line in std_out:
            line_spl = line.split()
            if start_count == 0:
                if len(line_spl) > 9:
                    try:
                        load_index = line_spl.index('load')
                    except ValueError:
                        pass
                    else:
                        load = float(line_spl[load_index+3].replace(',', ''))
                if (len(line_spl) > 2
                    and line_spl[0] == 'PID'
                        and line_spl[1] == 'USER'):
                    start_count = 1
            else:
                if float(line_spl[8].replace(',', '.')) > 50:
                    n_jobs += 1
                else:
                    break
    return msg, conn_success, load, n_jobs


def _check_node_disk(node_address, disk_path):
    """Check the disk usage of a node
    
    Parameters:
    -----------
    node_address (str)  The address of the node
    disk_path (str)     The disk to be checked
    
    Return:
    -------
    The following tuple:
    
    msg, conn_success, load, n_proc
    
    That are, respectivelly, a messege; True or False indicating if
    the connection was successful; the total disk size; and its free space
    
    """
    command = f'df -BG {disk_path}'
    msg = ''
    try:
        std_out, std_err = qpycomm.node_exec(node_address, command)
    except Exception as e:
        msg = str(e)
        conn_success = False
        used_disk = -1.0
        total_disk = -1.0
    else:
        std_out_spl = std_out.split("\n")
        conn_success = True
        if len(std_out_spl) > 1:
            std_out_spl = std_out_spl[1].split()
            total_disk = float(std_out_spl[1].replace('G', ''))
            used_disk = float(std_out_spl[2].replace('G', ''))
        else:
            msg= f"Parsing the df command failed:\n{std_out}"
            total_disk = -1.0
            used_disk = -1.0
    return msg, conn_success, total_disk, used_disk


def _check_node_memory(node_address):
    """Check the memory information of a node"""
    command = "free -g"
    conn_success = True
    msg = ''
    try:
        std_out, std_err = qpycomm.node_exec(node_address, command)
    except Exception as e:
        msg = str(e)
        conn_success = False
        used_mem = 0.0
        total_mem = 0.0
    else:
        std_out = std_out.split("\n")[1].split()
        total_mem = float(std_out[1])
        used_mem = float(std_out[2])
    return msg, conn_success, used_mem, total_mem


class Node:
    """A node from the qpy-multiuser point of view.
    
    Attributes:
    -----------
    
    name (str)
        Name for the node. This identifies the node for the users and for
        qpy-multiuser
    
    address (str)
        Address of the node. This is what is used to establish a connection
        with the node.
    
    attributes (list of str)
        The attributes of this node. These are labels that can be used to
        match the desired attributes, to select or avoid particular nodes
        by the user.
    
    is_up (bool)
        If True, the node is up, accessible and running
        If False, if means that ssh could not reach it
    
    pref_multicores (bool)
        If True, it means that this core is preferred for multicores jobs
    
    max_cores (int)
        Number of cores this node have available to be used.
    
    n_used_cores (int)
        Number of cores being used on this node in the moment
    
    n_outsiders (int)
        Number of processes that are running in this node that are not under qpy
        control (obtained from the system)
    
    n_free_cores (int, property)
        max_cores - n_used_cores - n_outsiders
    
    load (float)
        The current load of the node
    
    total_mem (float)
        Total memory available in this node, in GB. This can be set
        or obtained from th node
    
    req_mem (float)
        Total memory requested by the processes that are running, in GB
    
    used_mem (float)
        (Really) Used memory on this node, in GB. This is obtained from
        the system, in contrast to what been requested from the qpy users.
        Note that, if the total memory of the node is explicitly given and
        smaller that the actual total memory of the node, used_mem can
        be larger than total_mem
    
    avail_mem (float, property)
        total_mem - req_mem
    
    free_mem (float, property)
        total_mem - used_mem
    
    scratch_dir (str)
        A path to a disk whose space is to be checked
    
    total_disk (float)
        The total disk space (defined by scratch_dir)
    
    used_disk (float)
        The current free disk space (defined by scratch_dir)
    
    messages (Messages)
        Messages; TODO: associate to logging
    
    logger (Logger)
        A logger
    """
    __slots__ = ('name',
                 'address',
                 'attributes',
                 'is_up',
                 'pref_multicores',
                 'max_cores',
                 'n_used_cores',
                 'n_outsiders',
                 'load',
                 'total_mem',
                 'req_mem',
                 'used_mem',
                 'scratch_dir',
                 'total_disk',
                 'used_disk',
                 'messages',
                 'logger')
    
    def __init__(self, name):
        self.name = name
        self.address = name
        self.max_cores = 0
        self.messages = qpylog.Messages()
        self.messages.save = True
        self.is_up = False
        self.n_used_cores = 0
        self.pref_multicores = False
        self.total_mem = 0.0
        self.req_mem = 2.0
        self.used_mem = 0.0
        self.n_outsiders = 0
        self.attributes = []
        self.scratch_dir = None
        self.load = 0.0
        self.total_disk = 0.0
        self.used_disk = 0.0
        self.logger = qpylog.configure_logger(qpysys.multiuser_log_file,
                                              level=logging.DEBUG,
                                              logger_name=f'node {name}')

    def __str__(self):
        """String version of node"""
        _node_format_spec = ('{0:30s} {1:>5d} {2:>5d} {3:>7.1f}'
                             + '  '
                             + '{4:>7.1f} {5:>7.1f} {6:>7.1f} {7:>12s}')
        down = ' (down)' if not self.is_up else ''
        len_node_row = (len(down) + len(self.name)
                        + sum(map(len, self.attributes))
                        + len(self.attributes) + 2)
        if len_node_row > 28 or not(self.attributes):
            attr = ''
        else:
            attr = ' [' + ','.join(self.attributes) + ']'
        disk_info = (''
                     if (self.total_disk < 0.0 or self.used_disk < 0.0) else
                     f'{self.used_disk:>5.0f}  {self.total_disk:>5.0f}')
        x = _node_format_spec.format(self.name + attr + down,
                                     self.n_used_cores + self.n_outsiders,
                                     self.max_cores,
                                     self.load,
                                     self.used_mem,
                                     self.req_mem,
                                     self.total_mem,
                                     disk_info)
        if len_node_row > 28 and self.attributes:
            x += '    [' + ','.join(self.attributes) + ']'
        return x

    def __repr__(self):
        fields = [
            ('max_cores', self.max_cores),
            ('n_used_cores', self.n_used_cores),
            ('n_outsiders', self.n_outsiders),
            ('total_mem', self.total_mem),
            ('req_mem', self.req_mem),
            ('used_mem', self.used_mem),
            ('attributes', self.attributes),
            ('pref_multicores', self.pref_multicores),
            ('is_up', self.is_up),
        ]
        format_spec = '  {0: <15} = {1}'
        infos = '\n'.join(format_spec.format(*pair) for pair in fields)
        messages = ("\n" + _format_messages(self.messages)
                    if len(self.messages) > 0 else
                    '')
        return f'{self.name}\n' + infos + messages

    @classmethod
    def from_string(cls, x):
        """Construct a node from a string
        
        The string with the node info should be in the following format:
        
        <name> [key1=value1] [key2=value2] ...
        
        Each line of the file starts with the node name to be loaded.
        The node name is followed by a sequence of pairs key/value,
        separated by a equal, "=", sign (no spaces between the "=" and
        the key/value. All keys are optional. Theirmeaning and default
        values are:
        
        address
            The address of the node. If not given, name is used
        
        cores (int-able)
            The maximum number of processes this node can receive.
            If not passed, the value is obtained from the machine,
            see check()
        
        memory (float-able)
            The maximum memory this node can use.
            If not passed, the value obtained from the machine,
            see check()
        
        pref_multicore ('true' or 'false', case insensitive, default 'false')
            If it is a node preferred for multicore jobs
        
        attributes
            A string with one or more attributes of the node.
            Several attributes can be passed separated by comma, or from
            several of this key
        
        Parameters:
        -----------
        x (str)  The string with the nodes info.
        
        Raise:
        ------
        ValueError if the format of the string is not undestood
        
        """
        lspl = x.split()
        name = lspl[0]
        new_node = cls(name)
        for keyval in lspl[1:]:
            try:
                k, v = keyval.split('=')
            except ValueError:
                raise ValueError(
                    f'Expected a pair <key>=<value>, but got {keyval}')
            if k == 'address':
                new_node.address = v
            elif k == 'cores':
                new_node.max_cores = int(v)
            elif k == 'memory':
                new_node.total_mem = float(v)
            elif k == 'pref_multicore':
                if v.lower() == 'false':
                    new_node.pref_multicore = False
                elif v.lower() == 'true':
                    new_node.pref_multicore = True
                else:
                    raise ValueError('Give "false" or "true" for pref_multicore')
            elif k == 'attributes':
                new_node.attributes.extend(v.split(','))
            else:
                raise ValueError(f'Unknown key for creating a Node: {k}')
        return new_node

    @property
    def n_free_cores(self):
        return self.max_cores - self.n_used_cores - self.n_outsiders

    @property
    def avail_mem(self):
        return self.total_mem - self.req_mem

    @property
    def free_mem(self):
        return self.total_mem - self.used_mem

    def check(self):
        """Check several things in the node.
        
        Returns a named tuple with the information:
        
        is_up           True if the node is up
        n_cores         The number of cores in the node
        n_outsiders     the number of cores used by non-qpy processes
        total_mem       total memory of the node
        used_mem        used memory of node
        load            the load of that node
        total_disk      the size of the scratch disk (see class documentation)
        used_disk       the free amount of the scratch disk
        
        This function DOES NOT change the attributes of
        the node (except for adding messages), what should
        be done by the caller if desired.
        
        TODO:
        -----
        Better if this is done with one connection, instead of one for each property
        """
        info = namedtuple('NodeInfo',
                          ['is_up',
                           'n_cores',
                           'n_outsiders',
                           'total_mem',
                           'used_real',
                           'load',
                           'total_disk',
                           'used_disk'])
        info.is_up = True
        if self.max_cores == 0:
            (msg,
             info.is_up,
             info.n_cores) = _check_n_cpu(self.address)
        (msg,
         info.is_up,
         info.load,
         n_jobs) = _check_node_top(self.address)
        if info.is_up:
            self.logger.info('Load and untracked jobs checked')
        else:
            self.logger.warning('After load and untracked jobs:\n',
                                msg)
        info.n_outsiders = max(n_jobs - self.n_used_cores, 0)
        (msg,
         info.is_up,
         info.used_mem,
         info.total_mem) = _check_node_memory(self.address)
        if info.is_up:
            self.logger.info('Memory checked')
        else:
            self.logger.warning('After memory:\n',
                                msg)
        if self.scratch_dir is None:
            info.total_disk = -1.0
            info.used_disk = -1.0
        else:
            (msg,
             info.is_up,
             info.total_disk,
             info.used_disk) = _check_node_disk(self.address, self.scratch_dir)
            if info.is_up:
                self.logger.info('Disk usage checked')
            else:
                self.logger.warning('After disk usage:\n',
                                    msg)
        return info

    def has_attributes(self, req_attr):
        """Check if the node satisfy the attributes requirement
        
        Return True if the node has the attributes described by
        the list req_attr. It should be a list of strings, such
        that after joining each entry with space and replacing the
        attributes by True or False, a valid python logical expression
        is obtained.
        
        Arguments:
        ----------
        req_attr (list)
            A logical expression about attributes
        
        Return:
        -------
        The final evaluation of the expression.
        If is not a valid expression, returns True!
        
        TODO:
        -----
        Perhaps raise an Exception if the expression is not valid,
        and the caller decide if the job is used??
        """
        keywords = ['not', 'and', 'or', '(', ')']
        if len(req_attr) == 0:
            return True
        expression = (' '.join(req_attr)).replace('(',
                                                  ' ( ').replace(')',
                                                                 ' ) ')
        expression = expression.split()
        expression = [x
                      if x in keywords else
                      str(x in self.attributes or x == self.name)
                      for x in expression]
        try:
            res = eval(' '.join(expression))
        except:
            res = True
        self.logger.debug('\n'
                          'nodes attributes      %s\n'
                          'original expression   %s\n'
                          'parsed expression     %s\n'
                          'evaluated expression  %s\n',
                          self.attributes,
                          req_attr,
                          expression,
                          res)
        return res

    def free_resource(self, n_cores, mem):
        """Free n_cores of cores and mem of memory"""
        self.n_used_cores -= n_cores
        self.req_mem -= mem

    def allocate_resource(self, n_cores, mem):
        """Allocate n_cores of cores and mem of memory"""
        self.n_used_cores += n_cores
        self.req_mem += mem


class NodesCollection:
    """Collects all the nodes and store information about them
    
    
    Attributes:
    -----------
    n_cores (int)
        Total number of cores of all nodes
    
    n_min_cores (int)
        Total number of "min cores" of all nodes
    
    n_used_cores (int)
        Total number of used cores of all nodes
    
    n_used_min_cores (int)
        Total number of used "min cores" of all nodes
    
    n_outsiders (int)
        Total number of outsieders in all nodes
    
    
    Data Model:
    -----------
    If nodes is a NodesCollection and name is a string, then
    
    len(nodes)        the number of nodes
    
    nodes[name]       get the Node with that name
    
    name in nodes     return True that is the name of a node
    
    for n in nodes:   Iterates over the Nodes (not their name)
    
    Thread safety:
    --------------
    The class CheckNodes runs in another Thread and thus operations
    on some attributes should be done within the following lock:
        
    with self.check_lock:
        ...
    
    Some functions are not thread safe, e.g, add_node;
    For these case calls for these functions should be within the above
    cotext manager.
    Other functions, e.g., load_from_file and remove_node, are
    thread safe (the lock is made within the function) and thus they
    should not be within the context manager.
        
    """
    
    __slots__ = (
        '_the_nodes',
        '_the_names',
        '_n_cores',
        '_n_min_cores',
        '_n_used_cores',
        '_n_used_min_cores',
        '_n_outsiders',
        'check_lock',
        'check_alive',
        'check_time',
        'logger')

    def __init__(self):
        """Initilise the class
        """
        self.check_lock = threading.RLock()
        self.check_alive = True
        self.logger = qpylog.configure_logger(qpysys.multiuser_log_file,
                                              level=logging.DEBUG,
                                              logger_name='nodes')
        self.empty_nodes()
        self.check_time = 300

    def empty_nodes(self):
        self._the_nodes = []
        self._the_names = []
        self._n_cores = 0
        self._n_min_cores = 0
        self._n_used_cores = 0
        self._n_used_min_cores = 0
        self._n_outsiders = 0

    def __str__(self):
        """String version of a nodes collection"""
        sep1 = '-'*88 + '\n'
        sep2 = '='*88 + '\n'
        headerN = ('                                   cores'
                   + '                    memory (GB)'
                   + '       disk (GB)\n')
        headerN += ('node                            '
                    + 'used  total   load'
                    + '     used     req   total  used  total\n'
                    + sep1 )
        with self.check_lock:
            x = (headerN + '\n'.join(list(map(str, self._the_nodes))) + '\n' + sep2
                 if self._the_nodes else
                 'No nodes.')
            x += (f'There are {self.n_used_cores + self.n_outsiders}'
                  f' out of a total of {self.n_cores} cores being used.')
        return x

    def __repr__(self):
        variables = [
            ('n_cores', self.n_cores),
            ('n_min_cores', self.n_min_cores),
            ('n_used_cores', self.n_used_cores),
            ('n_used_min_cores', self.n_used_min_cores),
            ('n_outsiders', self.n_outsiders),
        ]
        format_spec = '{0: <16} = {1}'
        gen_var = '\n'.join(format_spec.format(*pair) for pair in variables)
        return (gen_var + '\n\n'
                + ('No nodes\n' if len(self) == 0 else 'Nodes:\n------\n\n')
                + ('\n'.join(repr(node) for node in self)))

    def __len__(self):
        return len(self._the_nodes)

    def __getitem__(self, k):
        try:
            i = self._the_names.index(k)
        except ValueError:
            raise KeyError(f'Node {k} is ont in the collection')
        return self._the_nodes[i]

    def __contains__(self, x):
        return x in self._the_names

    def __iter__(self):
        return iter(self._the_nodes)

    def items(self):
        return iter(zip(self._the_names, self._the_nodes))

    @property
    def n_cores(self):
        return self._n_cores

    @property
    def n_min_cores(self):
        return self._n_min_cores

    @property
    def n_used_cores(self):
        return self._n_used_cores

    @property
    def n_used_min_cores(self):
        return self._n_used_min_cores

    @property
    def n_outsiders(self):
        return self._n_outsiders

    def add_node(self, n):
        """Add a new node, n.
        
        This is NOT thread safe, see class documentation.
        """
        self._the_nodes.append(n)
        self._the_names.append(n.name)
        self._n_cores += n.max_cores

    def remove_node(self, name):
        """Remove a node by its name
        
        This IS thread safe, see class documentation.
        """
        with self.check_lock:
            i = self._the_names.index(name)
            self._n_cores -= self._the_nodes[i].max_cores
            del self._the_names[i]
            del self._the_nodes[i]

    def load_from_file(self, filename):
        """Load the nodes fro file filename
        
        This function IS thread safe, see class documentation.
        
        Each line of the file should be formatted as described in
        Node.from_string
        
        Raise:
        OSError        For problems when accessing the file
        ParseError     If there is a problmen when parsing file
        """
        with self.check_lock:
            self.empty_nodes()
            with open(filename, 'r') as f:
                for line in f:
                    self.add_node(Node.from_string(line))

    def check(self):
        """Check all nodes and update their status"""
        nodes_info = []
        try:
            for node in self:
                self.logger.info("Checking %s", node.name)
                nodes_info.append(node.check())
                self.logger.info("Done checking %s", node.name)
            with self.check_lock:
                for node, info in zip(self._the_nodes, nodes_info):
                    self._n_outsiders += (info.n_outsiders - node.n_outsiders)
                    node.is_up = info.is_up
                    if node.max_cores == 0:
                        node.max_cores = info.n_cores
                    node.n_outsiders = info.n_outsiders
                    if node.total_mem == 0.0:
                        node.total_mem = info.total_mem
                    node.used_mem = info.used_mem
                    node.load = info.load
                    node.total_disk = info.total_disk
                    node.used_disk = info.used_disk
        except:
            self.logger.exception("Error when checking nodes")

    def free_resource(self, node, n_cores, mem, n_min_cores):
        """Free n_cores and mem on node"""
        self[node].free_resource(n_cores, mem)
        self._n_used_cores -= n_cores
        self._n_used_min_cores -= n_min_cores

    def allocate_resource(self, node, n_cores, mem, n_min_cores):
        """Allocate n_cores and mem on node"""
        self[node].allocate_resource(n_cores, mem)
        self._n_used_cores += n_cores
        self._n_used_min_cores += n_min_cores


class CheckNodes(threading.Thread):
    """Check the nodes regularly.
    
    This Thread enters in the nodes regularly and checks if they are up,
    their memory, and outsiders jobs
    
    This check is done at each (global) check_time seconds
    
    Attributes:
    -----------
    finish            an Event that should be set to terminate this Thread
    
    nodes             the nodes
    
    """

    __slots__ = ('finish', 'nodes', 'logger')

    def __init__(self, nodes):
        """Initiate the class"""
        threading.Thread.__init__(self)
        self.finish = threading.Event()
        self.nodes = nodes

    def run(self):
        """Checks the nodes."""
        while not self.finish.is_set():
            self.nodes.check()
            self.finish.wait(self.nodes.check_time)
