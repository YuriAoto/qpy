""" qpy - Kills jobs. This is to be called from qpy_control_job.JobKiller

USAGE: qpy_job_killer.py <process_id 1> <process_id 2> ...
"""
import sys
import subprocess
import re

import qpy_system as qpysys
import qpy_useful_cosmetics as qpyutil

for job_id in sys.argv:
    command = 'ps -fu ' + qpysys.sys_user
    ps = subprocess.Popen(command,
                          shell = True,
                          stdout = subprocess.PIPE,
                          stderr = subprocess.PIPE)
    ps_out = ps.stdout.readlines()
    grand_PID = ''
    parents = {}
    for l in ps_out:
        new_pid = l.split()
        parents[new_pid[1]] = new_pid[2]
        re_res = re.search('export QPY_JOB_ID=' + job_id + ';', l)
        if (re_res):
            grand_PID = new_pid[1]
    pid_kill = qpyutil.get_all_children(grand_PID, parents)
    pid_kill.reverse()
    for i in pid_kill:
        kill_job = subprocess.call(['kill', '-9', i])
        sys.stdout.write(i + ' ')
