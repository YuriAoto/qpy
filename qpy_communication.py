"""Communication between threads, program instances and nodes

This module is central in qpy, as it deals with message
transfer between threads, and execution of programs in a node

"""
__version__ = '0.0'
__author__ = 'Yuri Alexandre Aoto'

from multiprocessing import connection
import os
import sys
import time
import random
import subprocess
try:
    import paramiko
    is_paramiko = True
    # Check version??
except:
    is_paramiko = False

def write_conn_files(f_name,
                     address,
                     port,
                     conn_key):
    """Write the connection information to files."""
    f = open(f_name + '_address', 'w')
    f.write(address)
    f.close()
    f = open(f_name + '_port', 'w')
    f.write(str(port))
    f.close()
    f = open(f_name + '_conn_key', 'w')
    f.write(conn_key)
    f.close()

def read_address_file(f_name):
    """Read the connection address (hostname) from file."""
    try:
        f = open(f_name + '_address', 'r')
        address = f.read().strip()
        f.close()
    except:
        address = 'localhost'
    return address

def read_conn_files(f_name):
    """Read the connection information from files."""
    address = read_address_file(f_name)
    try:
        f = open(f_name + '_port', 'r')
        port = int(f.read())
        f.close()
    except:
        port = None
    try:
        f = open(f_name + '_conn_key', 'r')
        conn_key = f.read()
        f.close()
    except:
        conn_key = None
    return address, port, conn_key

def establish_Listener_connection(address,
                                  port_min,
                                  port_max,
                                  port=None,
                                  conn_key=None):
    """Create a Listener connection

    Arguments:
    address (str)      The address of the connection
    port_min (int)     The minimum value that a randomly generated
                       port can have
    port_max (int)     The maximum value that a randomly generated
                       port can have
    port (int)         (optional, default = None) The port of the
                       connection. If None, randomly generates one
    conn_key (str)     (optional, default = None) The key of the
                       connection. If None, randomly generates one
    
    Behaviour:
    If port is passed as argument, it uses this port for the connection.
    Otherwise generates a random port, in the interval (port_min, port_max).
    Similar behaviour for conn_key, but without a max/min.
    
    Return:
    The tuple (List_master, port, key)
    where List_master is the Listener object
    (see multiprocessing.connection)
    port is the port for the connection.
    key is the key to the connection.
    
    See also:
    multiprocessing.connect
    """
    if conn_key == None:
        random.seed()
        conn_key = os.urandom(30)
    if port == None:
        while True:
            port = random.randint(port_min, port_max)
            try:
                List_master = connection.Listener((address, port),
                                                  authkey = conn_key)
                break
            except: #TODO handle better this
                pass
    else:
        try:
            List_master = connection.Listener((address, port),
                                              authkey = conn_key)
        except:
            List_master = None
    return (List_master, port, conn_key)

def message_transfer(msg,
                     address,
                     port,
                     key,
                     timeout=5.0):
    """Send and receive a message.
    
    Arguments:
    msg (str)          The message to be transfer
    address (str)      The address of the Listener
    port (int)         The port of the connection
    key (str)          The key for the connection
    wait (int, float)  (optional, default = 5.0).
                       The waiting time in seconds until it
                       gives up the attempt to connect
    
    Behaviour:
    This function sends a message to a Listener at (address, port),
    wait for a message back from the Listener and returns it.
    If the Listener takes too long to accept the connection, 
    an exception is raised.
    
    Returns:
    The message back from the connection.
    
    Raise:
    An Exception if the connection is not established.
    
    See also:
    multiprocessing.connect
    """
    def my_init_timeout():
        return time.time() + timeout
    connection._init_timeout = my_init_timeout
    try:
        conn = connection.Client((address, port), authkey=key)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        raise Exception("Connection not successful: " + str(exc_value))    
    conn.send(msg)
    back_msg = conn.recv()
    conn.close()
    return back_msg

def node_exec(node,
              command,
              get_outerr=True,
              mode="paramiko",
              pKey_file=None,
              localhost_popen_shell=False):
    """ Execute a command by ssh
    
    Arguments:
    node (str)         Where to execute
    command (str)      The command to be executed
    get_outerr (bool)  (optional, default = True).
                       If True, returns a tuple with the
                       stdout and stderr of the command.
    mode (str)         (optional, default = "paramiko")
                       The mode for the connection.
                       Possible modes:
                       "paramiko"
                       "popen"
    pKey_file          (optional, default = None)
    localhost_popen_shell (bool)
                       (optional, default = False)
                       Value for the argument shell of
                       subprocess.Popen used when node is
                       localhost.
    
    Return:
    The tuple (stdout, stderr), if the optional argument get_outerr
    is set to True
    
    Raise:
    SSH Exceptions if there is a problem in the SSH connection
    """
    if node == 'localhost':
        if isinstance(command, str) and not localhost_popen_shell: # Just to make it work with localhost as node...
            command = command.split()
        if (get_outerr):
            ssh = subprocess.Popen(command, shell = localhost_popen_shell,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            std_outerr = ssh.communicate()
            return(std_outerr)
        else:
            ssh = subprocess.Popen(command, shell = localhost_popen_shell)
            return
    elif (mode == "paramiko" and is_paramiko):
        if isinstance(command, list):
            command = ' '.join(command)
        if pKey_file is not None:
            k = paramiko.RSAKey.from_private_key_file(pKey_file)
        else:
            k = None
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(node, pkey = k)
        except paramiko.BadHostKeyException:
            raise Exception( "SSH error: server\'s host key could not be verified")
        except paramiko.AuthenticationException:
            raise Exception( "SSH error: authentication failed")
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            raise Exception( "SSH error: Connection error" + str(exc_type) + "  " + str(exc_value))
        if (get_outerr):
            (stdin, stdout, stderr) = ssh.exec_command( command)
            out = stdout.read()
            err = stderr.read()
            stdin.flush()
            stdout.close()
            stderr.close()
            ssh.close()
            return (out, err)
        else:
            ssh.exec_command(command)
            sleep(1.)
            ssh.close()
            return
    elif (mode == "popen"):
        if isinstance(command, str):
            command = command.split()
        if (get_outerr):
            ssh = subprocess.Popen(['ssh', node] + command, shell = False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
            std_outerr = ssh.communicate()
            return(std_outerr)
        else:
            ssh = subprocess.Popen(['ssh', node] + command, shell = False)
            return
    else:
        raise Exception("Unknown mode for node_exec")
