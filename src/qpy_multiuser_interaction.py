"""

"""
import qpy_system as qpysys
import qpy_logging as qpylog
import qpy_constants as qpyconst
import qpy_communication as qpycomm
import qpy_nodes_management as qpynodes
import qpy_users_management as qpyusers

## TODO: use exceptions instead of status codes
def _format_general_variables():
    variables = [
        ('N_cores', qpynodes.N_cores),
        ('N_min_cores', qpynodes.N_min_cores),
        ('N_used_cores', qpynodes.N_used_cores),
        ('N_used_min_cores', qpynodes.N_used_min_cores),
        ('N_outsiders', qpynodes.N_outsiders),
        ]
    format_spec = '{0: <16} = {1}'
    return '\n'.join(format_spec.format(*pair) for pair in variables)

def _format_jobs(jobs):
    format_spec='          {0}'
    return '\n'.join(format_spec.format(job)for job in jobs)

def _format_messages(messages):
    return '  Last messages:\n' + str(messages) + '----------'

def _format_user(user,info):
    fields = [
        ('min_cores', info.min_cores),
        ('extra_cores', info.extra_cores),
        ('max_cores', info.max_cores),
        ('n_used_cores', info.n_used_cores),
        ('n_queue', info.n_queue),
    ]
    format_spec = '  {0: <12} = {1}'
    infos = '\n'.join(format_spec.format(*pair) for pair in fields)
    current_jobs = _format_jobs(info.cur_jobs)
    messages = ('\n' + _format_messages(info.messages)
                if len(info.messages) > 0 else
                '')
    return '\n'.join([user,
                     infos,
                     current_jobs]) + messages

def _format_users(users):
    return "\n".join(_format_user(user,info)
                     for user, info in users.iteritems())

def _format_node(node, info):
    fields = [
        ('max_cores', info.max_cores),
        ('n_used_cores', info.n_used_cores),
        ('n_outsiders', info.n_outsiders),
        ('total_mem', info.total_mem),
        ('req_mem', info.req_mem),
        ('attributes', info.attributes),
        ('free_mem_real', info.free_mem_real),
        ('pref_multicores', info.pref_multicores),
        ('is_up', info.is_up),
    ]
    format_spec = '  {0: <15} = {1}'
    infos = '\n'.join(format_spec.format(*pair)
                      for pair in fields)
    messages = ("\n" + _format_messages(info.messages)
                if len(info.messages) > 0 else
                '')
    return infos + messages

def _format_nodes(nodes):
    return "\n".join(_format_node(node,info)
                     for node,info in nodes.iteritems())

def _handle_reload_nodes(args):
    """Handle a request to reload the nodes.
    
    args: ()
    """
    assert len(()) == 0
    status = qpynodes.load_nodes()
    return status, {
        0  : 'Nodes loaded.',
        -1 : ('Nodes loading failed. '\
              'Problem when openning {0}.'\
              .format(qpysys.nodes_file)),
        -2 : ('Nodes loading failed. '\
              'Check {0}.'.format(qpysys.nodes_file)),
    }.get(status, 'Nodes loading failed.')

def _handle_redistribute_cores(args):
    """Handle a request to redistribute cores.
    
    args: ()
    """
    assert len(()) ==0
    status = qpyusers.distribute_cores()
    return status,{
        0 : 'Cores distributed.',
        -1: ('Cores distribution failed. '\
             'Problem when openning {0}.'\
             .format(qpysys.cores_distribution_file)),
        -2: ('Cores distribution failed. '\
             'Check {0}.'\
             .format(qpysys.cores_distribution_file)),
        -3: ('Cores distribution failed. '\
             'Not enough cores.'),
    }.get(status, 'Cores distribution failed.')

def _handle_show_variables(args):
    """Handle a request to show the current variables
    
    args: ()
    """
    assert len(()) == 0
    with qpynodes.nodes_check_lock:
        return 0, "{general}\n{theusers}\n{thenodes}\n".format(
            general=_format_general_variables(),
            theusers = _format_users(qpyusers.users),
            thenodes = _format_nodes(qpynodes.nodes)
        )

def _handle_show_status(args):
    """Handle a request for the general multiuser status
    
    args : () or (user_name)?
    """
    sep1 = '-'*70 + '\n'
    sep2 = '='*70 + '\n'
    headerN =  '                                 cores              memory (GB)\n'
    headerN += 'node                          used  total      used     req   total\n'
    headerU =  'user                          using cores        queue size\n' + sep1
    msgU = ''
    format_spec = '{0:32s} {1:<5d}' + ' '*13 + '{2:<5d}\n'
    for user in sorted(qpyusers.users):
        msgU += format_spec.format(user,
                                   qpyusers.users[user].n_used_cores,
                                   qpyusers.users[user].n_queue)
    msgU = headerU + msgU + sep2 if msgU else 'No users.\n'
    msgN = ''
    format_spec = '{0:30s} {1:<5d} {2:<5d}' + ' '*2 + '{3:>7.1f} {4:>7.1f} {5:>7.1f}\n'
    with qpynodes.nodes_check_lock:
        for node in qpynodes.nodes:
            down=' (down)' if not(qpynodes.nodes[node].is_up) else ''
            len_node_row = (len(down) + len(node)
                            + sum(map(len,qpynodes.nodes[node].attributes))
                            + len(qpynodes.nodes[node].attributes) + 2)
            if len_node_row > 28 or not(qpynodes.nodes[node].attributes):
                attr = ''
            else:
                attr = ' [' + ','.join(qpynodes.nodes[node].attributes) + ']'
            msgN += format_spec.format(node + attr + down,
                                       qpynodes.nodes[node].n_used_cores
                                       + qpynodes.nodes[node].n_outsiders,
                                       qpynodes.nodes[node].max_cores,
                                       qpynodes.nodes[node].total_mem
                                       - qpynodes.nodes[node].free_mem_real,
                                       qpynodes.nodes[node].req_mem,
                                       qpynodes.nodes[node].total_mem)
            if len_node_row > 28 and qpynodes.nodes[node].attributes:
                msgN += '    [' + ','.join(qpynodes.nodes[node].attributes) + ']\n'
    msgN = headerN + sep1 + msgN + sep2 if msgN else 'No nodes.\n'
    status = 0
    msg_used_cores = 'There are {0} out of a total of {1} cores being used.\n'.format(
        qpynodes.N_used_cores + qpynodes.N_outsiders,
        qpynodes.N_cores)
    return status, msgU + msgN + msg_used_cores
        
def _handle_save_messages(args):
    """Handle a request to start saving messages
    
    args: (save_messages)
    """
    for user in qpyusers.users:
        qpyusers.users[user].messages.save= args[0]
    for node in qpynodes.nodes:
        qpynodes.nodes[node].messages.save= args[0]
    status = 0
    return status, 'Save messages set to {0}.\n'.format(args[0])
    
def _handle_sync_user_info(args):
    """Handle a request to synchronize user info
    
    args: user_name, address,port, conn_key, cur_jobs
    """
    user, address, port, conn_key, new_cur_jobs = args
    try:
        if user in qpyusers.users:
            qpyusers.users[user].address = address
            qpyusers.users[user].port = port
            qpyusers.users[user].conn_key = conn_key
            same_list = (len(new_cur_jobs) == len(qpyusers.users[user].cur_jobs)
                         and all(new_job == old_job
                                 for new_job, old_job in zip(new_cur_jobs,
                                                             qpyusers.users[user].cur_jobs)))
            return ((0,'User exists')
                    if same_list else
                    (1, 'User exists but with a different job list.'))
        else:
            try:
                with open(qpysys.allowed_users_file,'r') as f:
                    allowed_users =  list(line.strip() for line in f)
            except:
                allowed_users = []
            if user in allowed_users:
                new_user = qpyusers.User(user, address, port, conn_key)
                for job in new_cur_jobs:
                    new_user.add_job(job)
                qpyusers.users[user] = new_user
                return ((0,'User added')
                        if qpyusers.distribute_cores() == 0 else
                        (0, 'User added. Cores distribution failed.'))
            else:
                return 2,'Not allowed user'
    finally:
        for user in qpyusers.users:
            qpycomm.write_conn_files(qpysys.user_conn_file + user,
                                     qpyusers.users[user].address,
                                     qpyusers.users[user].port,
                                     qpyusers.users[user].conn_key)

def _handle_add_job(args):
    """Handle request to add a job
    
    args: (user_name, jobID, n_cores, mem, queue_size, node_attr)
    """
    if len(args) == 5: # old style. Can be removed when all masters are updated
        user, jobID, n_cores, mem, queue_size = args
        node_attr = []
    else:
        user, jobID, n_cores, mem, queue_size, node_attr = args
    assert isinstance(user, str)
    assert isinstance(jobID, int)
    assert isinstance(n_cores, int)
    assert isinstance(mem, float) or isinstance(mem,int)
    assert isinstance(queue_size, int)
    assert isinstance(node_attr, list)
    try:
        status = qpyusers.users[user].request_node(jobID,n_cores,mem,node_attr)
        qpylog.logger.debug('I am here: ' + str(status))
        if isinstance(status,str):
            qpyusers.users[user].n_queue = queue_size -1
            return 0,status
        else:
            qpyusers.users[user].n_queue = queue_size
            return (1,'No node with this requirement.') if status == 1 \
                else (2,'No free cores.')
    except KeyError:
        return -1, 'User does not exists.'
    except Exception as ex:
        return -2, ('WARNING: An exception of type {0} occured - add a job.\n'
                    + 'Arguments:\n{1!r}\n'
                    + 'Contact the qpy-team.').format(type(ex).__name__,
                                                      ex.args)

def _handle_remove_job(args):
    """Handle  a request to remove a job
    
    args : (user_name, jobID, queue_size)
    """
    user, jobID, queue_size = args
    assert isinstance(user, str)
    assert isinstance(jobID, int)
    assert isinstance( queue_size, int)
    try:
        status = qpyusers.users[user].remove_job(jobID)
        qpyusers.users[user].n_queue = queue_size
        return status,{0:'Job removed.',
                       1:'Job not found'}[status]
    except KeyError:
        return -1,'User does not exists.'
    except Exception as ex:
        return -2, ('WARNING: An exception of type {0} occured - remove a job.\n'
                    + 'Arguments:\n{1!r}\n'
                    + 'Contact the qpy-team.').format(type(ex).__name__,
                                                      ex.args)

def handle_client():
    """Handle the user messages sent from the client
    
    Behaviour:
    It opens a new connection using the multiuser connection
    parameters and waits for messages.
    When a message is received, it analyzes it, does whatever
    is needed and returns a message back.
    
    The message from qpy must be a tuple (action_type, arguments)
    where action_type is one of qpyconst.MULTIUSER_<something>
    
    To terminate, send a qpyconst.MULTIUSER_FINISH
    """
    try:
        (conn,
         multiuser_port,
         multiuser_key) = qpycomm.establish_Listener_connection(
             qpycomm.multiuser_address,
             qpyconst.PORT_MIN_MULTI,
             qpyconst.PORT_MAX_MULTI,
             port = qpycomm.multiuser_port,
             conn_key = qpycomm.multiuser_key
         )
        qpycomm.write_conn_files(qpysys.multiuser_conn_file,
                                 qpycomm.multiuser_address,
                                 multiuser_port,
                                 multiuser_key)
    except:
        qpylog.logger.exception("Error when establishing connection. Is there already a qpy-multiuser instance?")
        return
    if conn is None:
        return
    while True:
        qpylog.logger.info("Starting main loop.")
        try:
            client = conn.accept()
            (action_type, arguments) = client.recv()
        except:
            qpylog.logger.exception("Connection failed")
        else:
            qpylog.logger.info("Received request: %s arguments:%s",str(action_type), str(arguments))
        try:
            if (action_type == qpyconst.MULTIUSER_NODES):
                status,msg = _handle_reload_nodes(arguments)

            elif (action_type == qpyconst.MULTIUSER_DISTRIBUTE):
                status,msg = _handle_redistribute_cores(arguments)

            elif (action_type == qpyconst.MULTIUSER_SHOW_VARIABLES):
                status,msg = _handle_show_variables(arguments)

            elif (action_type == qpyconst.MULTIUSER_STATUS):
                status,msg = _handle_show_status(arguments)

            elif (action_type == qpyconst.MULTIUSER_SAVE_MESSAGES):
                status,msg = _handle_save_messages(arguments)

            elif (action_type == qpyconst.MULTIUSER_FINISH):
                client.send( (0, 'Finishing qpy-multiuser.'))
                client.close()
                break

            elif (action_type == qpyconst.MULTIUSER_USER):
                status, msg = _handle_sync_user_info(arguments)

            elif (action_type == qpyconst.MULTIUSER_REQ_CORE):
                status, msg = _handle_add_job(arguments)

            elif (action_type == qpyconst.MULTIUSER_REMOVE_JOB):
                status, msg = _handle_remove_job(arguments)

            else:
                status, msg =  -1, 'Unknown option: ' + str( action_type)
        except Exception as ex:
            qpylog.logger.exception("An error occured")
            template = ('WARNING: an exception of type {0} occured.\n'
                        + 'Arguments:\n{1!r}'
                        + '\nContact the qpy-team.')
            try:
                client.send((-10,template.format(type(ex).__name__, ex.args) ))
            except Exception:
                qpylog.logger.exception("An error occured while returning a message.")
                pass
        except BaseException as ex:
            qpylog.logger.exception("An error occured")
            template = ('WARNING: an exception of type {0} occured.\n'
                        + 'Arguments:\n{1!r}\n'
                        + 'Contact the qpy-team. qpy-multiuser is shutting down.')
            try:
                client.send((-10,template.format(type(ex).__name__, ex.args)))
            except Exception:
                qpylog.logger.exception("An error occured while returning a message.")
                pass
            finally:
                break
        else:
            try:
                client.send((status,msg))
            except:
                qpylog.logger.exception("An error occured while returning a message.")
                continue
