"""qpy-master - set the main driver for qpy

History:
    29 May 2015 - Pradipta and Yuri
    2017 - Arne - Several code improvements
    10 May 2019 - Distributing classes and functions over
                other files and avoiding global variables
"""
import os
import sys
import threading

import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_communication as qpycomm
import qpy_control_jobs as qpyctrl
from qpy_configurations import Configurations
from qpy_job import JobCollection
from qpy_master_interaction import MultiuserHandler, handle_qpy

if (not(os.path.isdir(qpysys.qpy_dir))):
    os.makedirs(qpysys.qpy_dir)
os.chmod(qpysys.qpy_dir, 0700)

if (not(os.path.isdir(qpysys.scripts_dir))):
    os.makedirs(qpysys.scripts_dir)

if (not(os.path.isdir(qpysys.notes_dir))):
    os.makedirs(qpysys.notes_dir)

if (os.path.isfile(qpysys.master_conn_file + '_port')):
    sys.exit('A connection file was found. '
             + 'Is there a qpy-master instance running?')

if (os.path.isfile(qpysys.master_conn_file+'_conn_key')):
    sys.exit('A connection file was found. '
             + 'Is there a qpy-master instance running?')

config = Configurations(qpysys.config_file)
jobs = JobCollection(config)

multiuser_alive = threading.Event()
multiuser_alive.clear()

check_run = qpyctrl.CheckRun(jobs, multiuser_alive, config)
check_run.start()

jobs_killer = qpyctrl.JobsKiller(jobs, multiuser_alive, config)
jobs_killer.start()

multiuser_handler = MultiuserHandler(jobs, multiuser_alive, config)
multiuser_handler.start()

sub_ctrl = qpyctrl.Submission(jobs, multiuser_handler, config)
sub_ctrl.start()

try:
    handle_qpy(jobs, sub_ctrl, jobs_killer, config, multiuser_alive)
except:
    config.logger.error('Exception at handle_qpy', exc_info=True)

config.logger.info("Finishing qpy-master")
sub_ctrl.finish.set()
check_run.finish.set()
jobs_killer.to_kill.put('kill')
qpycomm.message_transfer((qpyconst.FROM_MULTI_FINISH, ()),
                         multiuser_handler.address,
                         multiuser_handler.port,
                         multiuser_handler.conn_key)
os.remove(qpysys.master_conn_file + '_port')
os.remove(qpysys.master_conn_file + '_conn_key')
