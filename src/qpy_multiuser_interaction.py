""" qpy - Funtions for interaction with qpy-multiuser

"""
import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_communication as qpycomm
import qpy_users_management as qpyusers
from qpy_exceptions import *

## TODO: use exceptions instead of status codes
def _format_general_variables(nodes):
    variables = [
        ('N_cores', nodes.N_cores),
        ('N_min_cores', nodes.N_min_cores),
        ('N_used_cores', nodes.N_used_cores),
        ('N_used_min_cores', nodes.N_used_min_cores),
        ('N_outsiders', nodes.N_outsiders),
        ]
    format_spec = '{0: <16} = {1}'
    return '\n'.join(format_spec.format(*pair) for pair in variables)

def _format_jobs(jobs):
    format_spec='          {0}'
    return '\n'.join(format_spec.format(job)for job in jobs)

def _format_messages(messages):
    return '  Last messages:\n' + str(messages) + '----------'

def _format_user(user, info):
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
    return "\n".join(_format_user(user, info)
                     for user, info in users.all_.items())

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
    return "\n".join(_format_node(node, info)
                     for node, info in nodes.items())

def _handle_reload_nodes(args, nodes):
    """Handle a request to reload the nodes.
    
    args: ()
    """
    assert len(()) == 0
    status = nodes.load_nodes()
    return status, {
        0: 'Nodes loaded.',
        -1: ('Nodes loading failed. '\
              'Problem when openning {0}.'\
              .format(qpysys.nodes_file)),
        -2: ('Nodes loading failed. '\
              'Check {0}.'.format(qpysys.nodes_file)),
    }.get(status, 'Nodes loading failed.')

def _handle_redistribute_cores(args, users, nodes):
    """Handle a request to redistribute cores.
    
    args: ()
    """
    assert len(()) ==0
    status = users.distribute_cores(nodes)
    return status, {
        0: 'Cores distributed.',
        -1: ('Cores distribution failed. '\
             'Problem when openning {0}.'\
             .format(qpysys.cores_distribution_file)),
        -2: ('Cores distribution failed. '\
             'Check {0}.'\
             .format(qpysys.cores_distribution_file)),
        -3: ('Cores distribution failed. '\
             'Not enough cores.'),
    }.get(status, 'Cores distribution failed.')

def _handle_show_variables(args, users, nodes):
    """Handle a request to show the current variables
    
    args: ()
    """
    assert len(()) == 0
    with nodes.check_lock:
        return 0, "{general}\n{theusers}\n{thenodes}\n".format(
            general=_format_general_variables(nodes),
            theusers = _format_users(users),
            thenodes = _format_nodes(nodes.all_)
        )

def _handle_show_status(args, users, nodes):
    """Handle a request for the general multiuser status
    
    args : () or (user_name)?
    """
    sep1 = '-'*88 + '\n'
    sep2 = '='*88 + '\n'
    headerN =  '                                   cores                    memory (GB)       disk (GB)\n'
    headerN += 'node                            used  total   load     used     req   total  used total\n'
    headerU =  'user                          using cores        queue size\n' + sep1
    msgU = ''
    format_spec = '{0:32s} {1:<5d}' + ' '*13 + '{2:<5d}\n'
    for user in sorted(users.all_):
        msgU += format_spec.format(user,
                                   users.all_[user].n_used_cores,
                                   users.all_[user].n_queue)
    msgU = headerU + msgU + sep2 if msgU else 'No users.\n'
    msgN = ''
    format_spec = '{0:30s} {1:>5d} {2:>5d} {3:>7.1f}' + ' '*2 + '{4:>7.1f} {5:>7.1f} {6:>7.1f} {7:5.0f} {8:5.0f}\n'
    with nodes.check_lock:
        for node in nodes.all_:
            down=' (down)' if not(nodes.all_[node].is_up) else ''
            len_node_row = (len(down) + len(node)
                            + sum(map(len, nodes.all_[node].attributes))
                            + len(nodes.all_[node].attributes) + 2)
            if len_node_row > 28 or not(nodes.all_[node].attributes):
                attr = ''
            else:
                attr = ' [' + ','.join(nodes.all_[node].attributes) + ']'
            msgN += format_spec.format(node + attr + down,
                                       nodes.all_[node].n_used_cores
                                       + nodes.all_[node].n_outsiders,
                                       nodes.all_[node].max_cores,
                                       nodes.all_[node].load,
                                       nodes.all_[node].total_mem
                                       - nodes.all_[node].free_mem_real,
                                       nodes.all_[node].req_mem,
                                       nodes.all_[node].total_mem,
                                       nodes.all_[node].total_disk
                                       - nodes.all_[node].free_disk,
                                       nodes.all_[node].total_disk)

            if len_node_row > 28 and nodes.all_[node].attributes:
                msgN += '    [' + ','.join(nodes.all_[node].attributes) + ']\n'
    msgN = headerN + sep1 + msgN + sep2 if msgN else 'No nodes.\n'
    status = 0
    msg_used_cores = 'There are {0} out of a total of {1} cores being used.\n'.format(
        nodes.N_used_cores + nodes.N_outsiders,
        nodes.N_cores)
    return status, msgU + msgN + msg_used_cores
        
def _handle_save_messages(args, users, nodes):
    """Handle a request to start saving messages
    
    args: (save_messages)
    """
    for user in users.all_:
        users.all_[user].messages.save= args[0]
    for node in nodes.all_:
        nodes.all_[node].messages.save= args[0]
    status = 0
    return status, 'Save messages set to {0}.\n'.format(args[0])
    
def _handle_sync_user_info(args, users, nodes):
    """Handle a request to synchronize user info
    
    args: user_name, address,port, conn_key, cur_jobs
    """
    user, address, port, conn_key, new_cur_jobs = args
    try:
        if user in users.all_:
            users.all_[user].address = address
            users.all_[user].port = port
            users.all_[user].conn_key = conn_key
            same_list = (len(new_cur_jobs) == len(users.all_[user].cur_jobs)
                         and all(new_job == old_job
                                 for new_job, old_job in zip(new_cur_jobs,
                                                             users.all_[user].cur_jobs)))
            return ((0, 'User exists')
                    if same_list else
                    (1, 'User exists but with a different job list.'))
        else:
            try:
                with open(qpysys.allowed_users_file, 'r') as f:
                    allowed_users =  list(line.strip() for line in f)
            except:
                allowed_users = []
            if user in allowed_users:
                new_user = qpyusers.User(user, address, port, conn_key)
                for job in new_cur_jobs:
                    new_user.add_job(job, nodes)
                users.all_[user] = new_user
                return ((0, 'User added')
                        if users.distribute_cores(nodes) == 0 else
                        (0, 'User added. Cores distribution failed.'))
            else:
                return 2, 'Not allowed user'
    finally:
        for user in users.all_:
            qpycomm.write_conn_files(qpysys.user_conn_file + user,
                                     users.all_[user].address,
                                     users.all_[user].port,
                                     users.all_[user].conn_key)

def _handle_add_job(args, users, nodes):
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
    assert isinstance(mem, float) or isinstance(mem, int)
    assert isinstance(queue_size, int)
    assert isinstance(node_attr, list)
    try:
        status = users.all_[user].request_node(jobID,
                                               n_cores,
                                               mem,
                                               node_attr,
                                               users,
                                               nodes)
        users.logger.debug('I am here: ' + str(status))
        if isinstance(status, str):
            users.all_[user].n_queue = queue_size -1
            return 0, status
        else:
            users.all_[user].n_queue = queue_size
            return (1, 'No node with this requirement.') if status == 1 \
                else (2, 'No free cores.')
    except KeyError:
        return -1, 'User does not exists.'
    except Exception as ex:
        return -2, ('WARNING: An exception of type {0} occured - add a job.\n'
                    + 'Arguments:\n{1!r}\n'
                    + 'Contact the qpy-team.').format(type(ex).__name__,
                                                      ex.args)

def _handle_remove_job(args, users, nodes):
    """Handle  a request to remove a job
    
    args : (user_name, jobID, queue_size)
    """
    user, jobID, queue_size = args
    assert isinstance(user, str)
    assert isinstance(jobID, int)
    assert isinstance( queue_size, int)
    try:
        status = users.all_[user].remove_job(jobID, nodes)
        users.all_[user].n_queue = queue_size
        return status, {0:'Job removed.',
                       1:'Job not found'}[status]
    except KeyError:
        return -1, 'User does not exists.'
    except Exception as ex:
        return -2, ('WARNING: An exception of type {0} occured - remove a job.\n'
                    + 'Arguments:\n{1!r}\n'
                    + 'Contact the qpy-team.').format(type(ex).__name__,
                                                      ex.args)

def handle_client(users, nodes, logger):
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
    except qpyConnectionError:
        logger.exception("Error when establishing connection. "
                         + "Is there already a qpy-multiuser instance?")
        return
    qpycomm.write_conn_files(qpysys.multiuser_conn_file,
                             qpycomm.multiuser_address,
                             multiuser_port,
                             multiuser_key)
    while True:
        logger.info("Starting main loop.")
        try:
            client = conn.accept()
            (action_type, arguments) = client.recv()
        except:
            logger.exception("Connection failed")
        else:
            logger.info("Received request: %s arguments:%s", str(action_type), str(arguments))
        try:
            if (action_type == qpyconst.MULTIUSER_NODES):
                status, msg = _handle_reload_nodes(arguments, nodes)

            elif (action_type == qpyconst.MULTIUSER_DISTRIBUTE):
                status, msg = _handle_redistribute_cores(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_SHOW_VARIABLES):
                status, msg = _handle_show_variables(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_STATUS):
                status, msg = _handle_show_status(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_SAVE_MESSAGES):
                status, msg = _handle_save_messages(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_FINISH):
                client.send( (0, 'Finishing qpy-multiuser.'))
                client.close()
                break

            elif (action_type == qpyconst.MULTIUSER_USER):
                status, msg = _handle_sync_user_info(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_REQ_CORE):
                status, msg = _handle_add_job(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_REMOVE_JOB):
                status, msg = _handle_remove_job(arguments, users, nodes)

            else:
                status, msg =  -1, 'Unknown option: ' + str( action_type)
        except Exception as ex:
            logger.exception("An error occured")
            template = ('WARNING: an exception of type {0} occured.\n'
                        + 'Arguments:\n{1!r}'
                        + '\nContact the qpy-team.')
            try:
                client.send((-10, template.format(type(ex).__name__, ex.args) ))
            except Exception:
                logger.exception("An error occured while returning a message.")
                pass
        except BaseException as ex:
            logger.exception("An error occured")
            template = ('WARNING: an exception of type {0} occured.\n'
                        + 'Arguments:\n{1!r}\n'
                        + 'Contact the qpy-team. qpy-multiuser is shutting down.')
            try:
                client.send((-10, template.format(type(ex).__name__, ex.args)))
            except Exception:
                logger.exception("An error occured while returning a message.")
                pass
            finally:
                break
        else:
            try:
                client.send((status, msg))
            except:
                logger.exception("An error occured while returning a message.")
                continue
