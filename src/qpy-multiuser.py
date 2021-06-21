""" qpy - Control the core distribution among several users

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
from qpy_exceptions import qpyConnectionError
from qpy_multiuser_interaction import handle_client

if (not(os.path.isdir(qpysys.qpy_multiuser_dir))):
    os.makedirs(qpysys.qpy_multiuser_dir)
os.chmod(qpysys.qpy_multiuser_dir, 0o700)

try:
    logger = qpylog.configure_logger(qpysys.multiuser_log_file,
                                     qpylog.logging.DEBUG)
    logger.info('Starting main thread of qpy-multiuser')
    nodes = qpynodes.NodesCollection()
    nodes.load_from_file(qpysys.nodes_file)
    logger.info('Nodes loaded')
    users = qpyusers.UsersCollection()
    users.load_users(nodes)
    logger.info('Users loaded')
    check_nodes = qpynodes.CheckNodes(nodes)
    check_nodes.start()
except:
    qpylog.logging.exception('Exception before handle_client')
else:
    try:
        handle_client(users, nodes, logger)
    except qpyConnectionError:
        logger.exception("Error when establishing connection. "
                         + "Is there already a qpy-multiuser instance?")
    except BaseException:
        qpylog.logging.exception('Exception at handle_client')
logger.info('Finishing main thread of qpy-multiuser')
check_nodes.finish.set()
