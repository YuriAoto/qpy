""" qpy - Functions and classes related to users

"""
from math import copysign
import logging

import qpy_system as qpysys
import qpy_logging as qpylog
import qpy_communication as qpycomm
from qpy_job import MultiuserJob
import qpy_constants as qpyconst
from qpy_parser import ParseError


class NoNodeAvailableError(Exception):
    pass


def get_allowed_users():
    """Return a list of all allowed users."""
    try:
        with open(qpysys.allowed_users_file, 'r') as f:
            x = list(line.strip() for line in f if line.strip())
    except:
        x = []
    return x

class User:
    """A user from the qpy-multiuser point of view.
    
    This class contains the main functions to handle the interaction
    of the user within qpy-multiuser. In particular, it grants the usage
    of cores to the qpy-master of the user.
    
    Atributes:
    name          The user name
    address       Address to the qpy-master connection
    port          Port to the qpy-master connection
    conn_key      Key to the qpy-master connection
    
    min_cores     Number of cores guaranteed to be available to the user
    extra_cores   Number of cores to be used preferably by the user
    max_cores     Maximum number of cores that can be used by this user
                  in any time = (n_cores - min_cores of all other users)
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
        'logger',
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
        
        self.logger = qpylog.configure_logger(qpysys.multiuser_log_file,
                                              level=logging.DEBUG,
                                              logger_name=f'user {name}')
        
        self.messages = qpylog.Messages()
        self.messages.save = True

    def __str__(self):
        """String version of a user"""
        _user_format_spec = '{0:32s} {1:<5d}' + ' '*13 + '{2:<5d}'
        return _user_format_spec.format(self.name,
                                        self.n_used_cores,
                                        self.n_queue)

    def __repr__(self):
        """Description of variables of a user"""
        fields = [
            ('min_cores', self.min_cores),
            ('extra_cores', self.extra_cores),
            ('max_cores', self.max_cores),
            ('n_used_cores', self.n_used_cores),
            ('n_queue', self.n_queue)
        ]
        format_spec = '  {0: <12} = {1}'
        infos = '\n'.join(format_spec.format(*pair) for pair in fields)
        current_jobs = '\n'.join('          {0}'.format(job) for job in self.cur_jobs)
        messages = (('\n  Last messages:\n' + str(messages) + '----------')
                    if len(self.messages) > 0 else
                    '')
        return '\n'.join([self.name,
                          infos,
                          current_jobs]) + messages

    def remove_job(self, jobID, nodes):
        """Remove a job from the user and from the nodes.
        
        Arguments:
        jobID (int)
            The ID number of the job
        nodes
            The qpynodes.NodesCollection
        
        Return:
        None
        
        Raise:
        ValueError if the job was not found
        """
        for job in self.cur_jobs:
            if job.ID == jobID:
                self.n_used_cores -= job.n_cores
                if self.n_used_cores < self.min_cores:
                    n_min_cores = min(job.n_cores,
                                      self.min_cores - self.n_used_cores)
                else:
                    n_min_cores = 0
                nodes.free_resource(job.node,
                                    job.n_cores,
                                    job.mem,
                                    n_min_cores)
                self.cur_jobs.remove(job)
                return
        raise ValueError('job not found!')

    def add_job(self, job, nodes):
        """Add a new job to a specific node.
        
        Arguments:
        job     The job, as an instance of MultiuserJob
        
        Return:
        None
        """
        self.cur_jobs.append(job)
        self.n_used_cores += job.n_cores
        if self.n_used_cores < self.min_cores:
            n_min_cores = job.n_cores
        elif self.n_used_cores < self.min_cores + job.n_cores:
            n_min_cores= (job.n_cores
                          + self.min_cores
                          - self.n_used_cores)
        else:
            n_min_cores = 0
        nodes.allocate_resource(job.node,
                                job.n_cores,
                                job.mem,
                                n_min_cores)

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
        
        Return:
        a string with the node that can be allocated to the user
        
        Raise:
        NoNodeAvailableError
        
        """
        space_available = False
        n_free_cores = ((nodes.n_cores - nodes.n_used_cores)
                        - (nodes.n_min_cores - nodes.n_used_min_cores))
        free_cores = n_free_cores >= num_cores
        self.logger.debug('Requesting resource:\n'
                          '  jobID = %s\n'
                          '  requested cores = %d\n'
                          '  requested memory = %.2f\n'
                          '  attribute = %s\n'
                          'Nodes info:\n'
                          '  n_cores = %d\n'
                          '  n_used_cores = %d\n'
                          '  n_min_cores = %d\n'
                          '  n_used_min_cores = %d\n'
                          '  n_free_cores = %d\n',
                          jobID,
                          num_cores,
                          mem,
                          node_attr,
                          nodes.n_cores,
                          nodes.n_used_cores,
                          nodes.n_min_cores,
                          nodes.n_used_min_cores,
                          n_free_cores)
        if self.n_used_cores + num_cores <= self.min_cores:
            space_available = True
        else:
            use_others_resource = (self.n_used_cores + num_cores
                                   > self.min_cores + self.extra_cores)
            if use_others_resource:
                n_users_with_queue = 1
                n_extra = 0
                for user in users:
                    if user.name == self.name:
                        continue
                    if (user.n_queue > 0
                        and (user.n_used_cores >=
                             user.min_cores + user.extra_cores)):
                        n_users_with_queue += 1
                    elif user.n_queue == 0:
                        n_extra += min(user.extra_cores + user.min_cores
                                       - user.n_used_cores,
                                       user.extra_cores)
                n_extra_per_user = n_extra // n_users_with_queue
                if (self.n_used_cores + num_cores <=
                    self.min_cores + self.extra_cores + n_extra_per_user
                        and free_cores):
                    space_available = True
            else:
                space_available = free_cores
        self.logger.debug('Space available? %s', space_available)
        if space_available:
            best_node = None
            best_free_cores = 0
            with nodes.check_lock:
                if num_cores == 1:
                    for node in nodes:
                        if (node.has_attributes(node_attr)
                            and not(node.pref_multicores)
                            and node.n_free_cores > best_free_cores
                            and node.avail_mem > mem
                                and node.free_mem > mem):
                            best_node = node
                            best_free_cores = node.n_free_cores
                if best_node is None:
                    for node in nodes:
                        if (node.is_up
                            and node.has_attributes(node_attr)
                            and (node.max_cores
                                 - node.n_outsiders
                                 - node.n_used_cores >= num_cores)
                            and node.avail_mem > mem):
                            best_node = node
                            break
            if best_node is None:
                self.logger.debug("No node with the requirement.")
                raise NoNodeAvailableError('No node with this requirement.')
            self.logger.debug('Best node: %s', best_node.name)
            self.add_job(MultiuserJob(self.name,
                                      jobID,
                                      mem,
                                      num_cores,
                                      best_node.name),
                         nodes)
            return best_node.name + '=' + best_node.address
        raise NoNodeAvailableError('No free cores.')


class UsersCollection:
    """Store a group of users
    
    
    """
    __slots__ = ('_the_users', 'logger')

    def __init__(self):
        """ Initialise the class"""
        self._the_users = {}
        self.logger = qpylog.configure_logger(qpysys.multiuser_log_file,
                                              level=logging.DEBUG,
                                              logger_name=f'users')

    def __str__(self):
        """String version of a group os users"""
        sep1 = '-'*88
        sep2 = '='*88
        header = (
            'user                          using cores        queue size\n')
        x = ('No users.'
             if len(self) == 0 else
             header + sep1 + '\n'
             + '\n'.join(list(map(str, self))) + '\n' + sep2)
        return x

    def __repr__(self):
        return 'Users:\n------\n\n' + '\n'.join(repr(user) for user in self)

    def __len__(self):
        return len(self._the_users)

    def __getitem__(self, k):
        return self._the_users[k]

    def __contains__(self, x):
        return x in self._the_users

    def __iter__(self):
        return iter(self._the_users.values())
    
    def names(self):
        return iter(self._the_users)
    
    def items(self):
        return iter(self._the_users.items())

    def add_user(self, username, nodes,
                 address=None,
                 port=None,
                 conn_key=None,
                 running_jobs=None):
        """Add a new user
        
        Parameters:
        -----------
        username (str)
            The user name.
        
        address (str or None):
            The connection address. If None, get it from the connection files
        
        port (int or None):
            The connection port. If None, get it from the connection files
        
        conn_key (str or None):
            The connection key. If None, get it from the connection files
        
        running_jobs (list of MultiuserJob or None)
            The running jobs of the user. If None asks to the users's master
        
        """
        self.logger.info('Adding new user: %s', username)
        if address is None:
            address = qpycomm.read_address_file(qpysys.user_conn_file + username)
        if port is None or conn_key is None:
            port, conn_key = qpycomm.read_conn_files(qpysys.user_conn_file + username)
        new_user = User(username, address, port, conn_key)
        self.logger.debug('A user for %s has been created.', username)
        if running_jobs is None:
            try:
                self.logger.info('Requesting jobs from %s', username)
                running_jobs = qpycomm.message_transfer(
                    (qpyconst.FROM_MULTI_CUR_JOBS, ()),
                    new_user.address,
                    new_user.port,
                    new_user.conn_key, timeout=2.0)
                self.logger.info('Jobs from %s obtained!', username)
            except:
                self.logger.exception('Failed when reading jobs from %s',
                                      username)
                running_jobs = []
        for job in running_jobs:
            new_user.add_job(job, nodes)
        self._the_users[username] = new_user

    def load_users(self, nodes):
        """Load the users.
        
        For each user given in the file of the global variable allowed_users,
        attempt to obtain the port and the key for the connection with their
        qpy-master (from MULTIUSER_HANDLER) and request the current running
        jobs.
        It also redistributes the cores.
        
        The file should contain just the username of the users, one in each
        line.
        """
        for username in get_allowed_users():
            try:
                self.add_user(username, nodes)
            except:
                self.logger.exception("Failed when adding %s", username)
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
        None
        
        Raise:
        ParseError                 for any syntax error in the file
        NoNodeAvailableError       if exceed the number of cores
        """
        f = open(qpysys.cores_distribution_file, 'r')
        line = f.readline()
        line_spl = line.split()
        dist_type = line_spl[0]
        min_cores = 0
        users_min = {}
        users_extra = {}
        if (len(line_spl) > 1):
            if line_spl[1] != 'minimum':
                raise ParseError('Not equal "minimum"')
            try:
                min_cores = int(line_spl[2])
            except:
                raise ParseError('Error to read min_cores')
        # General minimum cores
        left_cores = nodes.n_cores
        for username in self.names():
            users_min[username] = min_cores
            left_cores -= min_cores
        if (left_cores < 0):
            raise NoNodeAvailableError(
                'Requirement exceeds the number of cores')
        # Even distribution
        if (dist_type == 'even'):
            if (len(self) == 0):
                return 0
            n_per_user = left_cores // len(self)
            for username in self.names():
                users_extra[username] = n_per_user
            left_cores = left_cores - n_per_user*len(self)
        # Explicit distribution TODO: there was some bug here...
        elif (dist_type == 'explicit'):
            for line in f:
                line_spl = line.split()
                user = line_spl[0]
                if user not in self:
                    continue
                value = line_spl[1].split('+')
                if len(value) > 2 or len(value) == 0:
                    raise ParseError('len(value) not correct')
                if len(value) == 2:
                    try:
                        min_cores = int(value[0])
                        left_cores += users_min[user] - min_cores
                        users_min[user] = min_cores
                        value.pop(0)
                    except:
                        raise ParseError('len(value) not correct')
                users_extra[user] = value[0]
            if (left_cores < 0):
                raise NoNodeAvailableError(
                    'Requirement exceeds the number of cores')
            left_cores_original = left_cores
            for username, info in users_extra.items():
                try:
                    if (info[-1] == '%'):
                        n_per_user = float(info[:-1])
                        n_per_user = int(n_per_user * left_cores_original
                                         // 100)
                    else:
                        n_per_user = int(info)
                except:
                    raise ParseError('len(value) not correct')
                add_cores = n_per_user
                left_cores -= add_cores
                users_extra[username] = add_cores
        # Unknown type of distribution
        else:
            raise ParseError('Unknown type of distribution: ' + dist_type)
        f.close()
        # Equally share left cores
        while (left_cores != 0):
            for username in users_extra:
                if (left_cores == 0):
                    break
                users_extra[username] += int(copysign(1, left_cores))
                left_cores += copysign(1, -left_cores)
        # Finally put into the users variable
        nodes._n_min_cores = 0  # BAD! nodes  should change _n_...
        nodes._n_used_min_cores = 0  # BAD! nodes  should change _n_...
        for user in self:
            try:
                user.min_cores = users_min[user.name]
            except:
                user.min_cores = 0
            nodes._n_min_cores += user.min_cores# BAD! nodes  should change _n_...
            nodes._n_used_min_cores += min(user.min_cores, # BAD! nodes  should change _n_...
                                           user.n_used_cores)
            try:
                user.extra_cores = users_extra[user.name]
            except:
                user.extra_cores = 0
        for user in self:
            user.max_cores = nodes.n_cores - nodes.n_min_cores + user.min_cores
        return 0
