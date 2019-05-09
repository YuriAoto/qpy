"""A job (a code being or to be executed in a node)

"""
__version__ = '0.0'
__author__ = 'Yuri Alexandre Aoto'

class JobId():
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

class MultiuserJob():
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
        return (str(self.ID)+ ": node = " + self.node
                + ", n_cores = " + str(self.n_cores)
                + ", mem = " + str(self.mem))


