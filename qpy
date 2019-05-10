#!/usr/bin/env python
"""
qpy - The Queue Management System in Python

History:
    29 May 2015 - started by Pradipta and Yuri

"""
__version__ = '1.0'
__author__ = 'Yuri Alexandre Aoto'

import os
import sys
import re

import qpy_system as qpysys
import qpy_constants as qpyconst
try:
    import qpy_communication as qpycomm
except AssertionError as e:
    sys.exit(str(e) + "\nContact the system administrator.")
import qpy_useful_cosmetics as qpyutil
from qpy_parser import parse_qpy_cmd_line

option, arguments, start_qpy_master = parse_qpy_cmd_line()

address = qpycomm.read_address_file(qpysys.master_conn_file)
try:
    port, conn_key = qpycomm.read_conn_files(qpysys.master_conn_file)
except IOError:
    if start_qpy_master:
        qpyutil.start_master_driver(qpysys.sys_user,
                                    address,
                                    qpysys.qpy_master_command)
    else:
        sys.exit('Problem to get the connection information.'
                 + ' Are you sure that qpy-master is running?')
try:
    master_msg = qpycomm.message_transfer((option, arguments),
                                          address,
                                          port,
                                          conn_key,
                                          timeout = 3.0)
except:
    if start_qpy_master:
        os.remove(qpysys.master_conn_file + '_port')
        os.remove(qpysys.master_conn_file + '_conn_key')
        start_master_driver(qpysys.sys_user,
                            address,
                            qpysys.qpy_master_command)
    else:
        sys.exit('Time for connection exceeded.'
                 + ' Are you sure that qpy-master is running?')
if not sys.stdout.isatty():
    ansi_escape = re.compile(r'\x1b[^m]*m')
    master_msg = ansi_escape.sub('', master_msg)
sys.stdout.write(master_msg)
if start_qpy_master:
    start_master_driver(qpysys.sys_user, address, qpysys.qpy_master_command)
