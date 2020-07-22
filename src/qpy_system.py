""" qpy - System's and user's information

"""
import os
import sys

qpy_instance = sys.argv[0].split('/')[-1]
source_dir = os.path.dirname(os.path.abspath(__file__)) + '/'
test_run = os.path.isfile(source_dir + 'test_dir')
home_dir = os.environ['HOME']
sys_user = os.environ['USER']

if qpy_instance == 'qpy' or qpy_instance == 'qpy-master.py':
    if test_run:
        try:
            user = os.environ['QPY_TEST_USER']
        except KeyError:
            raise Exception(
                'Please, set the environment variable QPY_TEST_USER.')
        qpy_dir = os.path.expanduser('~/.qpy-test_' + user + '/')
    else:
        user = os.environ['USER']
        qpy_dir = os.path.expanduser('~/.qpy/')
    scripts_dir = qpy_dir + '/scripts/'
    notes_dir = qpy_dir + '/notes/'
    jobID_file = qpy_dir + '/next_jobID'
    all_jobs_file = qpy_dir + '/all_jobs'
    config_file = qpy_dir + '/config'
    multiuser_conn_file = qpy_dir + 'multiuser_connection'
    master_conn_file = qpy_dir + 'master_connection'
    master_log_file = qpy_dir + 'master.log'
    tutorial_file = source_dir + '../doc/tutorial'
    qpy_master_command = 'python3 ' + source_dir + 'qpy-master.py'

else:
    if test_run:
        qpy_multiuser_dir = os.path.expanduser('~/.qpy-multiuser-test/')
    else:
        qpy_multiuser_dir = os.path.expanduser('~/.qpy-multiuser/')
    nodes_file = qpy_multiuser_dir + 'nodes'
    allowed_users_file = qpy_multiuser_dir + 'allowed_users'
    cores_distribution_file = qpy_multiuser_dir + 'distribution_rules'
    user_conn_file = qpy_multiuser_dir + 'connection_'
    multiuser_conn_file = qpy_multiuser_dir + 'multiuser_connection'
    multiuser_log_file = qpy_multiuser_dir + 'multiuser.log'
    tutorial_file = source_dir + '../doc/adm_tutorial'
    qpy_multiuser_command = ['python3',
                             source_dir + 'qpy-multiuser.py',
                             '>', '/dev/null',
                             '2>', '/dev/null']
