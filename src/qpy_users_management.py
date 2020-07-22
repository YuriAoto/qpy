""" qpy - Functions and classes related to users

"""
from math import copysign

import qpy_system as qpysys
import qpy_logging as qpylog
import qpy_communication as qpycomm
from qpy_job import MultiuserJob
from qpy_exceptions import *
import qpy_constants as qpyconst

class User(object):
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

    __slots__ = (
        'name',
        'address',
        'port',
        'conn_key',
        'min_cores',
        'extra_cores',
        'max_cores',
        'n_used_cores',
        'n_queue',
        'cur_jobs',
        'messages')
        
    def __init__(self, name, address, port, conn_key):
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
        
        self.messages = qpylog.Messages()
        self.messages.save = True

    def remove_job(self, jobID, nodes):
        """Remove a job from the user and from the nodes.
        
        Arguments:
        jobID     The ID number of the job
        
        Return 0 if success or 1 if the job was not found
        """
        for job in self.cur_jobs:
            if (job.ID == jobID):
                self.n_used_cores -= job.n_cores
                nodes.all_[job.node].n_used_cores -= job.n_cores
                nodes.all_[job.node].req_mem -= job.mem
                nodes.N_used_cores -= job.n_cores
                if (self.n_used_cores < self.min_cores):
                    N_used_min_cores -= min(job.n_cores,
                                            self.min_cores - self.n_used_cores)
                self.cur_jobs.remove( job)
                return 0
        return 1

    def add_job(self, job, nodes):
        """Add a new job to a specific node.
        
        Arguments:
        job     The job, as an instance of MultiuserJob
        
        Return 0
        """
        self.cur_jobs.append(job)
        self.n_used_cores += job.n_cores
        nodes.all_[job.node].n_used_cores += job.n_cores
        nodes.all_[job.node].req_mem += job.mem
        nodes.N_used_cores += job.n_cores
        if (self.n_used_cores < self.min_cores):
            nodes.N_used_min_cores += job.n_cores
        elif (self.n_used_cores < self.min_cores + job.n_cores):
            nodes.N_used_min_cores += (job.n_cores
                                                       + self.min_cores
                                                       - self.n_used_cores)
        return 0

    def request_node(self, jobID, num_cores, mem, node_attr, users, nodes):
        """Tries to give a space to the user
        
        Arguments:
        jobID       The job ID
        num_cores   Requested number of cores
        mem         Requested memory
        node_attr   Attributes that the node must fulfil
        users       UsersCollection
        
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
        space_available = False
        
        users.logger.debug('In request_node. jobID, attribute: '
                            + str(jobID)
                            + str(node_attr) )
        
        N_free_cores = ((nodes.N_cores - nodes.N_used_cores)
                        - (nodes.N_min_cores - nodes.N_used_min_cores))
        free_cores = N_free_cores >= num_cores
        if self.n_used_cores + num_cores <= self.min_cores:
            space_available = True
        else:
            use_others_resource = (self.n_used_cores + num_cores
                                   > self.min_cores + self.extra_cores)
            if use_others_resource:
                N_users_with_queue = 1
                N_extra = 0
                for user, info in users.all_.items():
                    if user == self.name:
                        continue
                    if (info.n_queue > 0
                        and (info.n_used_cores >=
                             info.min_cores + info.extra_cores)
                        ):
                        N_users_with_queue += 1
                    elif info.n_queue == 0:
                        N_extra += min(info.extra_cores + info.min_cores
                                       - info.n_used_cores,
                                       info.extra_cores)
                N_extra_per_user = N_extra // N_users_with_queue
                if (self.n_used_cores + num_cores <=
                    self.min_cores + self.extra_cores + N_extra_per_user
                    and free_cores):
                    space_available = True
            else:
                space_available = free_cores 
        if space_available:
            best_node = None
            best_free = 0
            with nodes.check_lock:
                if num_cores == 1:
                    for node, info in nodes.all_.items():
                        free = info.max_cores - info.n_outsiders - info.n_used_cores
                        if (info.has_attributes(node_attr) and
                            not(info.pref_multicores) and
                            free > best_free and
                            (info.total_mem - info.req_mem) > mem and
                            info.free_mem_real > mem):
                            best_node = node
                            best_free = free
                if best_node is None:
                    for node, info in nodes.all_.items():
                        if (info.is_up
                            and info.has_attributes(node_attr)
                            and (info.max_cores
                                 - info.n_outsiders
                                 - info.n_used_cores >= num_cores)
                            and (info.total_mem
                                 - info.req_mem > mem)):
                            best_node = node
                            break
            if best_node == None:
                return 1
            new_job = MultiuserJob(self.name, jobID, mem, num_cores, best_node)
            self.add_job(new_job, nodes)
            return best_node + '=' + nodes.all_[best_node].address
        return 2

class UsersCollection(object):
    """


    """
    __slots__ = ('all_', 'logger')

    def __init__(self, logger):
        """ Initialise the class"""
        self.all_ = {}
        self.logger = logger

    def load_users(self, nodes):
        """Load the users.

        For each user given in the file of the global variable allowed_users,
        attempt to obtain the port and the key for the connection with their
        qpy-master (from MULTIUSER_HANDLER) and request the current running jobs.
        It also redistributes the cores.
        
        The file should contain just the username of the users, one in each line.
        """
        allowed_users = []
        try:
            f = open(qpysys.allowed_users_file, 'r')
        except IOError:
            return
        for line in f:
            allowed_users.append( line.strip())
        f.close()
        for user in allowed_users:
            address = qpycomm.read_address_file(qpysys.user_conn_file + user)
            try:
                port, conn_key = qpycomm.read_conn_files(
                    qpysys.user_conn_file + user)
            except:
                pass
            else:
                new_user = User(user, address, port, conn_key)
                try:
                    cur_jobs = qpycomm.message_transfer((qpyconst.FROM_MULTI_CUR_JOBS, ()),
                                                        new_user.address,
                                                        new_user.port,
                                                        new_user.conn_key, timeout=2.0)
                except:
                    pass
                else:
                    for job in cur_jobs:
                        new_user.add_job(job, nodes)
                    self.all_[user] = new_user
        self.distribute_cores(nodes)


    def distribute_cores(self, nodes):
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
            f = open(qpysys.cores_distribution_file, 'r')
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
        left_cores = nodes.N_cores
        for user in self.all_:
            users_min[user] = min_cores
            left_cores -= min_cores
        if (left_cores < 0):
            return -3
        # Even distribution
        if (dist_type == 'even'):
            n_users = len(self.all_)
            if (n_users == 0):
                return 0
            N_per_user = left_cores // n_users
            for user in self.all_:
                users_extra[user] = N_per_user
            left_cores = left_cores - N_per_user*n_users
        # Explicit distribution TODO: there was some bug here...
        elif (dist_type == 'explicit'):
            for line in f:
                line_spl = line.split()
                user = line_spl[0]
                if not user in self.all_:
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
            for user, info in users_extra.items():
                try:
                    if (info[-1] == '%'):
                        N_per_user = float(info[:-1])
                        N_per_user = int(N_per_user * left_cores_original // 100)
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
                users_extra[user] += int(copysign(1, left_cores))
                left_cores += copysign(1, -left_cores)
        # Finally put into the users variable
        nodes.N_min_cores = 0
        nodes.N_used_min_cores = 0
        for user in self.all_:
            try:
                self.all_[user].min_cores = users_min[user]
            except:
                self.all_[user].min_cores = 0
            nodes.N_min_cores += self.all_[user].min_cores
            nodes.N_used_min_cores += min(self.all_[user].min_cores,
                                             self.all_[user].n_used_cores)
            try:
                self.all_[user].extra_cores = users_extra[user]
            except:
                self.all_[user].extra_cores = 0
        for user in self.all_:
            self.all_[user].max_cores = (nodes.N_cores
                                     - nodes.N_min_cores
                                     + self.all_[user].min_cores)
        return 0
