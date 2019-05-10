""" qpy - user interface to the qpy-multiuser

History:
    31 Dec 2015 - Creation, Pradipta and Yuri
"""

from time import sleep
import sys

import qpy_system as qpysys
import qpy_communication as qpycomm
from qpy_parser import parse_qpy_multiuser_cmd_line

option, arguments, start_qpy_multiuser = parse_qpy_multiuser_cmd_line()
if start_qpy_multiuser:
    sys.stdout.write("Starting qpy-multiuser driver.\n")
    qpycomm.node_exec(qpycomm.multiuser_address,
                      qpysys.qpy_multiuser_command,
                      get_outerr=False,
                      mode='popen')
    exit()
try:
    msg_back = qpycomm.message_transfer((option, arguments),
                                        qpycomm.multiuser_address,
                                        qpycomm.multiuser_port,
                                        qpycomm.multiuser_key,
                                        timeout=3.0)
except Exception as ex:
    print ex
    sys.exit('Time for connection exceeded.'
             + ' Are you sure that qpy-multiuser is running?')
else:
    sys.stdout.write('status: ' + str(msg_back[0]) + '\n'
                     + msg_back[1] + '\n')
