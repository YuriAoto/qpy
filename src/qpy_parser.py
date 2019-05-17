""" qpy - Parsers for commands

"""
import os
import sys
import subprocess

import qpy_system as qpysys
import qpy_useful_cosmetics as qpyutil
import qpy_constants as qpyconst
from qpy_exceptions import *

def parse_qpy_cmd_line():
    try:
        option = qpyconst.KEYWORDS[sys.argv[1]][0]
    except:
        str_len = 0
        for opt in qpyconst.KEYWORDS:
            if (str_len < len( opt)):
                str_len = len( opt)
        format_spc = '{0:' + str(str_len+1) + 's}'
        usage_msg = 'Usage: ' + sys.argv[0] + ' <option> [<arguments>].\n'
        usage_msg += 'Options:'
        for opt in qpyconst.KEYWORDS:
            usage_msg += ('\n  ' + format_spc.format(opt + ':')
                          + ' ' + qpyconst.KEYWORDS[opt][1])
        sys.exit(usage_msg)

    start_qpy_master = option == qpyconst.JOBTYPE_RESTART
    if start_qpy_master:
        option = qpyconst.JOBTYPE_FINISH

    arguments = ()
    if option == qpyconst.JOBTYPE_SUB:
        job = ''
        next_is_attr = False
        for i in sys.argv[2:]:
            if next_is_attr:
                i = i.replace(' ', '##')
            job += ' ' + i
            next_is_attr = i == '-a' or i == '--node_attr' or i == '--attribute'
        arguments = [job, os.getcwd()]

    if option == qpyconst.JOBTYPE_CHECK:
        arguments = {}
        for x in sys.argv[2:]:
            if x in JOB_STATUS:
                if ('status' in arguments):
                    arguments['status'].append(x)
                else:
                    arguments['status'] = [x]
            else:
                if (os.path.isdir(x)):
                    if 'dir' in arguments:
                        arguments['dir'].append(os.path.abspath(x))
                    else:
                        arguments['dir'] = [os.path.abspath(x)]
                else:
                    try:
                        new_jobids = qpyutil.string_to_int_list(x)
                    except:
                        sys.exit( 'Unknown pattern for checking jobs: ' + x)
                    if ('job_id' in arguments):
                        arguments['job_id'].extend( new_jobids)
                    else:
                        arguments['job_id'] = new_jobids

    elif option == qpyconst.JOBTYPE_CONFIG:
        if (len(sys.argv) == 4):
            arguments = (sys.argv[2], sys.argv[3])
        elif (len( sys.argv) > 4):
            arguments = (sys.argv[2], sys.argv[3:])
        elif (len( sys.argv) == 3):
            arguments = (sys.argv[2], ())

    elif (option == qpyconst.JOBTYPE_KILL
          or option == qpyconst.JOBTYPE_CLEAN):
        if option == qpyconst.JOBTYPE_KILL:
            status_bound = (0,2)
        else:
            status_bound = (2,5)
        arguments = []
        for x in sys.argv[2:]:
            try:
                new_range = qpyutil.string_to_int_list(x)
                arguments.extend( new_range)
            except:
                if (x == 'all'
                    or x in qpyconst.JOB_STATUS[status_bound[0]:status_bound[1]]):
                    arguments.append( x)
                elif (os.path.isdir(x)):
                    arguments.append(os.path.abspath(x))
                else:
                    sys.exit('Range with wrong format or invalid status: ' + x)
        arguments = list(set(arguments))

    elif option == qpyconst.JOBTYPE_CTRLQUEUE:
        if len( sys.argv) < 3:
            sys.exit('Give the queue control type.')
        else:
            if sys.argv[2] == 'jump':
                new_range = []
                for x in sys.argv[3:-1]:
                    try:
                        new_range.extend(qpyutil.string_to_int_list(x))
                    except:
                        sys.exit('Range with wrong format: ' + x)
                if not new_range:
                        sys.exit('Give the jobs to change position.')
                try:
                    pos = int(sys.argv[-1])
                except:
                    if sys.argv[-1] == 'begin':
                        pos = 0
                    elif sys.argv[-1] == 'end':
                        pos = -1
                    else:
                        sys.exit('Invalid jump position: ' + sys.argv[-1])
                arguments = ('jump', new_range, pos)
            else:
                arguments = sys.argv[2:]

    elif option == qpyconst.JOBTYPE_NOTE:
        if len(sys.argv) < 3:
            arguments = ()
        elif len(sys.argv) == 3:
            arguments = [sys.argv[2]]
        else:
            arguments = [sys.argv[2], ' '.join(sys.argv[3:])]

    elif option == qpyconst.JOBTYPE_TUTORIAL:
        pattern = ''
        for i in sys.argv[2:3]:
            pattern += i
        for i in sys.argv[3:]:
            pattern += ' ' + i
        if pattern in qpyconst.KEYWORDS:
            pattern = '--pattern "# ' + pattern + '"'
        elif pattern:
            pattern = '--pattern "' + pattern + '"'
        command = 'less '  + pattern + ' ' + qpysys.tutorial_file
        try:
            subprocess.call(command, shell = True)
        except:
            sys.exit('Error when loading the tutorial.')
        exit()

    return option, arguments, start_qpy_master


def parse_qpy_multiuser_cmd_line():
    start_qpy_multiuser = False
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
            usage_msg = ('Usage: ' + sys.argv[0]
                         + ' __req_core <user_name> <jobID>'
                         + ' <n_cores> <mem> <queue_size> [<node_attr>].')
            sys.exit(usage_msg)

    if (option == qpyconst.MULTIUSER_REMOVE_JOB):
        try:
            arguments = [sys.argv[2], int( sys.argv[3]), int(sys.argv[4])]
        except:
            usage_msg = ('Usage: ' + sys.argv[0]
                         + ' __remove_job <user_name> <job_ID> <queue_size>.')
            sys.exit(usage_msg)

    if (option == qpyconst.MULTIUSER_SAVE_MESSAGES):
        try:
            arguments = [True if (sys.argv[2] == 'true') else False]
        except:
            usage_msg = 'Usage: ' + sys.argv[0] +  ' [true,false].'
            sys.exit( usage_msg)

    if (option == qpyconst.MULTIUSER_START):
        start_qpy_multiuser = True

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

        command = 'less ' + pattern + ' ' + qpysys.tutorial_file
        try:
            subprocess.call(command, shell = True)
        except:
            sys.exit( 'Error when loading the tutorial.')
        exit()

    return option, arguments, start_qpy_multiuser


def parse_node_info(L):
    """A parser for the line in nodes_file"""
    lspl = L.split()
    name = lspl.pop(0)
    address = name
    n_cores = int(lspl.pop(0))
    if 'M' in lspl:
        multicore = True
        lspl.remove('M')
    else:
        multicore = False
    for i in lspl:
        if 'address=' in i:
            address = i.split('=')[1]
            lspl.remove(i)
            break
    attributes = lspl

    return name, n_cores, address, multicore, attributes
