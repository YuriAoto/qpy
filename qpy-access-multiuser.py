""" qpy - user interface to the qpy-multiuser

History:
    31 Dec 2015 - Creation, Pradipta and Yuri
"""
from time import sleep
import os
import sys
import subprocess
import re
import threading

import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_communication as qpycomm


## Put somewhere else?
(multiuser_address,
 multiuser_port,
 multiuser_key) = qpycomm.read_conn_files(qpysys.multiuser_conn_file)
qpy_multiuser_command = ['python',
                         qpysys.source_dir + 'qpy-multiuser.py',
                         '>', '/dev/null',
                         '2>', '/dev/null']

try:
    option = qpyconst.MULTIUSER_KEYWORDS[sys.argv[1]][0]
except:
    str_len = 0
    for opt in qpyconst.MULTIUSER_KEYWORDS:
        if (qpyconst.MULTIUSER_KEYWORDS[opt][0] < 0):
            continue
        if (str_len < len( opt)):
            str_len = len( opt)
    format_spc = '{0:' + str( str_len+1) + 's}'
    usage_msg =  'Usage: ' + sys.argv[0] +  ' <option> [<arguments>].\n'
    usage_msg += 'Options:'
    for opt in qpyconst.MULTIUSER_KEYWORDS:
        if (qpyconst.MULTIUSER_KEYWORDS[opt][0] < 0):
            continue
        usage_msg += ('\n  ' + format_spc.format( opt+':')
                      + ' ' + qpyconst.MULTIUSER_KEYWORDS[opt][1])
    sys.exit( usage_msg)

arguments = ()
if (option == qpyconst.MULTIUSER_USER):
    try:
        arguments = (sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    except:
        usage_msg = ('Usage: ' + sys.argv[0]
                     + ' __user <user_name> <address> <port> <conn_key>.')
        sys.exit(usage_msg)

if (option == qpyconst.MULTIUSER_REQ_CORE):
    try:
        arguments = (sys.argv[2],
                     int(sys.argv[3]),
                     int(sys.argv[4]),
                     float(sys.argv[5]),
                     int(sys.argv[6]),
                     [] if len(sys.argv) == 7 else sys.argv[7:])
    except:
        usage_msg = 'Usage: ' + sys.argv[0] + ' __req_core <user_name> <jobID> <n_cores> <mem> <queue_size> [<node_attr>].'
        sys.exit(usage_msg)

if (option == qpyconst.MULTIUSER_REMOVE_JOB):
    try:
        arguments = [sys.argv[2], int( sys.argv[3]), int(sys.argv[4])]
    except:
        usage_msg = 'Usage: ' + sys.argv[0] + ' __remove_job <user_name> <job_ID> <queue_size>.'
        sys.exit(usage_msg)

if (option == qpyconst.MULTIUSER_SAVE_MESSAGES):
    try:
        arguments = [True if (sys.argv[2] == 'true') else False]
    except:
        usage_msg = 'Usage: ' + sys.argv[0] +  ' [true,false].'
        sys.exit( usage_msg)

if (option == qpyconst.MULTIUSER_START):
    sys.stdout.write( "Starting qpy-multiuser driver.\n")
    qpycomm.node_exec(multiuser_address,
                      qpy_multiuser_command,
                      get_outerr=False,
                      mode='popen')
    exit()

elif (option == qpyconst.MULTIUSER_TUTORIAL):    
    pattern = ''
    for i in sys.argv[2:3]:
        pattern += i
    for i in sys.argv[3:]:
        pattern += ' ' + i

    if (pattern in qpyconst.KEYWORDS):
        pattern = '--pattern "# ' + pattern + '"'
    elif (pattern):
        pattern = '--pattern "' + pattern + '"'

    command = 'less '  + pattern + ' ' + qpysys.tutorial_file
    try:
        subprocess.call( command, shell = True)
    except:
        sys.exit( 'Error when loading the tutorial.')
    exit()

try:
    msg_back = qpycomm.message_transfer((option, arguments),
                                        multiuser_address,
                                        multiuser_port,
                                        multiuser_key,
                                        timeout = 3.0)
except Exception as ex:
    print ex
    sys.exit('Time for connection exceeded. Are you sure that qpy-multiuser is running?')
else:
    sys.stdout.write('status: ' + str(msg_back[0]) + '\n' + msg_back[1] + '\n')
