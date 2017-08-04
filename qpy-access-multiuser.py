# qpy - user interface to the qpy-multiuser
#
# 31 Dec 2015 - Pradipta and Yuri
from time import sleep
import os
import sys
import subprocess
import re
import threading
from qpyCommon import *

multiuser_address = 'ares4'
multiuser_key = 'zxcvb'
if (TEST_RUN):
    multiuser_port = 9998
else:
    multiuser_port = 9999

qpy_multiuser_command = [ 'python', QPY_SOURCE_DIR + 'qpy-multiuser.py', '>', '/dev/null']#, '2>', '/dev/null']


try:
    option = MULTIUSER_KEYWORDS[sys.argv[1]][0]
except:
    str_len = 0
    for opt in MULTIUSER_KEYWORDS:
        if (MULTIUSER_KEYWORDS[opt][0] < 0):
            continue
        if (str_len < len( opt)):
            str_len = len( opt)
    format_spc = '{0:' + str( str_len+1) + 's}'
    usage_msg =  'Usage: ' + sys.argv[0] +  ' <option> [<arguments>].\n'
    usage_msg += 'Options:'
    for opt in MULTIUSER_KEYWORDS:
        if (MULTIUSER_KEYWORDS[opt][0] < 0):
            continue
        usage_msg += '\n  ' + format_spc.format( opt+':') + ' ' + MULTIUSER_KEYWORDS[opt][1]
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


# remove a job
if (option == MULTIUSER_SAVE_MESSAGES):
    try:
        arguments = [True if (sys.argv[2] == 'true') else False]
    except:
        usage_msg = 'Usage: ' + sys.argv[0] +  ' [true,false].'
        sys.exit( usage_msg)


# Start
if (option == MULTIUSER_START):
    sys.stdout.write( "Starting qpy-multiuser driver.\n")
    node_exec('ares4', qpy_multiuser_command, get_outerr = False, mode='popen')

    exit()


try:
    msg_back = message_transfer((option, arguments),
                                multiuser_address, multiuser_port, multiuser_key,
                                timeout = 3.0)
except Exception as ex:
    sys.exit('Time for connection exceeded. Are you sure that qpy-multiuser is running?')
else:
    sys.stdout.write('status: ' + str(msg_back[0]) + '\n' + msg_back[1] + '\n')
