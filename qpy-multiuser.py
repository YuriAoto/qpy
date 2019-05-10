"""
qpy - control the nodes distribution for several users

History:
  26 December 2015 - Pradipta and Yuri
  06 January 2016 - Pradipta and Yuri
  2017 - Arne - Several code improvements
  10 May 2019 - Distributing classes and functions over
                other files and avoiding global variables
"""
import os

import qpy_system as qpysys
import qpy_logging as qpylog
import qpy_nodes_management as qpynodes
import qpy_users_management as qpyusers
from qpy_multiuser_interaction import handle_client

if (not(os.path.isdir(qpysys.qpy_multiuser_dir))):
    os.makedirs(qpysys.qpy_multiuserdir)
os.chmod(qpysys.qpy_multiuser_dir, 0700)
qpynodes.load_nodes()
qpyusers.load_users()
check_nodes = qpynodes.CheckNodes()
check_nodes.start()
try:
    handle_client()
except:
    qpylog.logging.exception("Exception at handle_client")
qpylog.logger.info('Finishing main thread of qpy-multiuser')
check_nodes.finish.set()
