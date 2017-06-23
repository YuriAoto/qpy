# module with common tasks/variables for qpy
#
#
import paramiko

MULTIUSER_NODES          = 1
MULTIUSER_DISTRIBUTE     = 2
MULTIUSER_STATUS         = 3
MULTIUSER_SHOW_VARIABLES = 4
MULTIUSER_FINISH         = 5
MULTIUSER_START          = 6
MULTIUSER_USER           = -1
MULTIUSER_REQ_CORE       = -2
MULTIUSER_REMOVE_JOB     = -3

JOBTYPE_SUB       = 1
JOBTYPE_CHECK     = 2
JOBTYPE_KILL      = 3
JOBTYPE_FINISH    = 4
JOBTYPE_NODES     = 5 # Obsolete
JOBTYPE_MAXJOBS   = 6 # Obsolete
JOBTYPE_CONFIG    = 7
JOBTYPE_CLEAN     = 8
JOBTYPE_TUTORIAL  = 9
JOBTYPE_STATUS    = 10
JOBTYPE_RESTART   = 11
JOBTYPE_CTRLQUEUE = 12
JOBTYPE_NOTE      = 13



# execute command by ssh
def node_exec( node, command, get_outerr = True):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect( node)
    except BadHostKeyException:
        raise Exception( "SSH error: server\'s host key could not be verified")
    except AuthenticationException:
        raise Exception( "SSH error: authentication failed")
    except:
        raise Exception( "SSH error: Connection error")

    if (get_outerr):
        (stdin, stdout, stderr) = ssh.exec_command( command)
        out = stdout.read()
        err = stderr.read()
        stdin.flush()
        stdout.close()
        stderr.close()
        ssh.close()
        return ( out, err)

    else:
        ssh.exec_command( command)
        ssh.close()
        return
