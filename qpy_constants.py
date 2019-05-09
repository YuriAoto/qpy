"""Main constants for qpy

"""

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

KEYWORDS = {
    'sub': (JOBTYPE_SUB,
            'Submits a job. Arguments: the job command'),
    'check': (JOBTYPE_CHECK,
              'Checks the jobs. Arguments: the desired job status'),
    'kill': (JOBTYPE_KILL,
             'Kills jobs. Argument: the jobs id'),
    'finish': (JOBTYPE_FINISH,
               'Finishes the master execution. No arguments'),
    'config': (JOBTYPE_CONFIG,
               'Shows the current configuration. No arguments'),
    'clean': (JOBTYPE_CLEAN,
              'Removes finished jobs from the list. Arguments: the jobs id'),
    'tutorial': (JOBTYPE_TUTORIAL,
                 'Opens the qpy tutorial. Arguments: optional: a pattern'),
    'status': (JOBTYPE_STATUS,
               'Shows current status. No arguments'),
    'restart': (JOBTYPE_RESTART,
                'Restarts qpy-master. No arguments'),
    'ctrlQueue': (JOBTYPE_CTRLQUEUE,
                  'Fine control over the queue. Arguments: see tutorial'),
    'notes': (JOBTYPE_NOTE,
              'Adds and reads notes. Arguments: ID and the note')
}

MULTIUSER_KEYWORDS = {
    'nodes': (MULTIUSER_NODES,
              'Realoads nodes file. No arguments'),
    'distribute': (MULTIUSER_DISTRIBUTE,
                   'Distributes cores: No arguments'),
    'status': (MULTIUSER_STATUS,
               'Shows status. No arguments'),
    'variables': (MULTIUSER_SHOW_VARIABLES,
                  'Shows variables. No arguments'),
    'start': (MULTIUSER_START,
              'Starts multiuser execution. No arguments'),
    'finish': (MULTIUSER_FINISH,
               'Finishes the multiuser execution. No arguments'),
    'saveMessages': (MULTIUSER_SAVE_MESSAGES,
                     'Saves messages for debugging. ' +
                     'Arguments: true or false'),
    'tutorial': (MULTIUSER_TUTORIAL,
                 'Opens the qpy administrator tutorial. ' +
                 'Arguments: optional: a pattern'),
    '__user': (MULTIUSER_USER,
               'Adds user. Arguments: user_name'),
    '__req_core': (MULTIUSER_REQ_CORE,
                   'Requires a core: ' +
                   'Arguments: user_name, jobID, n_cores, mem, queue_size'),
    '__remove_job': (MULTIUSER_REMOVE_JOB,
                     'Removes a job: ' +
                     'Arguments: user_name, job_ID, queue_size'),
}

PORT_MIN_MULTI  = 10000
PORT_MAX_MULTI  = 20000
PORT_MIN_MASTER = 20001
PORT_MAX_MASTER = 60000
