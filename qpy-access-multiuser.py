# qpy - user interface to the qpy-multiuser
#
# 31 Dec 2015 - Pradipta and Yuri
from multiprocessing.connection import Client
from time import sleep
import os
import sys
import subprocess
import re
import threading

from qpy_general_variables import *

# Important variables
multiuser_address = 'localhost'
multiuser_key = 'zxcvb'
multiuser_port = 9999

qpy_source_dir = os.path.dirname( os.path.abspath( __file__)) + '/'
qpy_multiuser_command = ['python', qpy_source_dir+'qpy-multiuser.py', '>', '/dev/null', '2>', '/dev/null']

keywords={'nodes':        (MULTIUSER_NODES,          'Reaload nodes file. No arguments'),
          'distribute':   (MULTIUSER_DISTRIBUTE,     'Distribute cores: No arguments'),
          'status' :      (MULTIUSER_STATUS,         'Show status. No arguments'),
          'variables' :   (MULTIUSER_SHOW_VARIABLES, 'Show variables. No arguments'),
          'start':        (MULTIUSER_START,          'Start multiuser execution. No arguments'),
          'finish':       (MULTIUSER_FINISH,         'Finish the multiuser execution. No arguments'),
          '__user':       (MULTIUSER_USER,           'Add user. Arguments: user_name'),
          '__req_core':   (MULTIUSER_REQ_CORE,       'Require a core: Arguments: user_name, jobID, n_cores, mem, queue_size'),
          '__remove_job': (MULTIUSER_REMOVE_JOB,     'Remove a job: Arguments: user_name, job_ID, queue_size'),
          }

try:
    option = keywords[sys.argv[1]][0]
except:
    str_len = 0
    for opt in keywords:
        if (keywords[opt][0] < 0):
            continue
        if (str_len < len( opt)):
            str_len = len( opt)
    format_spc = '{0:' + str( str_len+1) + 's}'
    usage_msg =  'Usage: ' + sys.argv[0] +  ' <option> [<arguments>].\n'
    usage_msg += 'Options:'
    for opt in keywords:
        if (keywords[opt][0] < 0):
            continue
        usage_msg += '\n  ' + format_spc.format( opt+':') + ' ' + keywords[opt][1]
    sys.exit( usage_msg)


# Get arguments, according to the option
arguments = ()


# Add user
if (option == MULTIUSER_USER):
    try:
        arguments = (sys.argv[2], [])
    except:
        usage_msg =  'Usage: ' + sys.argv[0] +  ' __user <user_name>.'
        sys.exit( usage_msg)

# Request a core
if (option == MULTIUSER_REQ_CORE):
    try:
        arguments = (sys.argv[2], int(sys.argv[3]), int(sys.argv[4]), float(sys.argv[5]), int(sys.argv[6]))
    except:
        usage_msg = 'Usage: ' + sys.argv[0] +  ' __req_core <user_name> <jobID> <n_cores> <mem> <queue_size>.'
        sys.exit( usage_msg)

# remove a job
if (option == MULTIUSER_REMOVE_JOB):
    try:
        arguments = [sys.argv[2], int( sys.argv[3]), int(sys.argv[4])]
    except:
        usage_msg = 'Usage: ' + sys.argv[0] +  ' __remove_job <user_name> <job_ID> <queue_size>.'
        sys.exit( usage_msg)


# Add user
if (option == MULTIUSER_START):
    sys.stdout.write( "Starting qpy-multiuser driver.\n")
    subprocess.Popen( qpy_multiuser_command, shell = False)
    exit()


# Try to connect in a thread, to test the connection
class try_connection( threading.Thread):
    def __init__( self):
        threading.Thread.__init__( self)
        self.conn = None
    def run( self):
        self.conn = Client( (multiuser_address, multiuser_port), authkey=multiuser_key)

M = try_connection()
M.daemon = True
M.start()
n = 0
waiting = 60
while (M.is_alive()):
    n += 1
    if (n == waiting):
        sys.exit( 'Time for connection exceeded. Are you sure that qpy-master is running?')
    sleep( 0.05)
conn = M.conn
conn.send( (option, arguments))
message_back = conn.recv()
sys.stdout.write( str(message_back[0]) + ": " + message_back[1] + '\n')
conn.close()
