""" qpy - Funtions for interaction with qpy-multiuser

"""
import qpy_system as qpysys
import qpy_constants as qpyconst
import qpy_communication as qpycomm
import qpy_users_management as qpyusers
from qpy_parser import ParseError


def _handle_reload_nodes(args, nodes):
    """Handle a request to reload the nodes.
    
    args: ()
    """
    assert len(args) == 0
    try:
        nodes.load_from_file(qpysys.nodes_file)
    except OSError as exc:
        return -1, 'Nodes loading failed: {0}.'.format(str(exc))
    except ParseError as exc:
        return -2, ('Nodes loading failed: {0}. Check {1}.'
                    .format(str(exc), qpysys.nodes_file))
    return 0, 'Nodes loaded.'


def _handle_redistribute_cores(args, users, nodes):
    """Handle a request to redistribute cores.
    
    args: ()
    """
    assert len(args) == 0
    try:
        users.distribute_cores(nodes)
    except OSError as exc:
        return -1, 'Cores distribution failed: {0}.'.format(str(exc))
    except ParseError as exc:
        return -2, ('Cores distribution failed: {0}. Check {1}.'
                    .format(str(exc), qpysys.cores_distribution_file))
    except qpyusers.NoNodeAvailableError as exc:
        return -3, 'Cores distribution failed. {0}.'.format(str(exc))
    return 0, 'Cores distributed.'


def _handle_show_variables(args, users, nodes):
    """Handle a request to show the current variables
    
    args: ()
    """
    assert len(args) == 0
    with nodes.check_lock:
        return 0, f"{nodes!r}\n\n{users!r}\n"


def _handle_show_status(args, users, nodes):
    """Handle a request for the general multiuser status
    
    args: ()
    
    TODO: Can args be (user_name)?
    """
    return 0, f"{users}\n{nodes}\n"


def _handle_save_messages(args, users, nodes):
    """Handle a request to start saving messages
    
    args: (save_messages)
    """
    assert len(args) == 1
    for user in users:
        user.messages.save = args[0]
    for node in nodes:
        node.messages.save = args[0]
    return 0, 'Save messages set to {0}.\n'.format(args[0])


def _handle_sync_user_info(args, users, nodes):
    """Handle a request to synchronize user info
    
    args: user_name, address, port, conn_key, cur_jobs
    """
    username, address, port, conn_key, new_cur_jobs = args
    try:
        if username in users:
            user = users[username]
            user.address = address
            user.port = port
            user.conn_key = conn_key
            same_list = (len(new_cur_jobs) == len(user.cur_jobs)
                         and all(new_job == old_job
                                 for new_job, old_job in zip(
                                         new_cur_jobs,
                                         user.cur_jobs)))
            return ((0, 'User exists')
                    if same_list else
                    (1, 'User exists but with a different job list.'))
        else:
            if user in qpyusers.get_allowed_users():
                users.add_user(user, nodes,
                               address, port, conn_key, new_cur_jobs)
                return ((0, 'User added')
                        if users.distribute_cores(nodes) == 0 else
                        (0, 'User added. Cores distribution failed.'))
            else:
                return 2, 'Not allowed user'
    finally:
        for user in users:
            qpycomm.write_conn_files(qpysys.user_conn_file + user.name,
                                     user.address,
                                     user.port,
                                     user.conn_key)


def _handle_add_job(args, users, nodes):
    """Handle request to add a job
    
    args: (user_name, jobID, n_cores, mem, queue_size, node_attr)
    """
    # old style. Can be removed when all masters are updated
    if len(args) == 5:
        user, jobID, n_cores, mem, queue_size = args
        node_attr = []
    else:
        user, jobID, n_cores, mem, queue_size, node_attr = args
    assert isinstance(user, str)
    assert isinstance(jobID, int)
    assert isinstance(n_cores, int)
    assert isinstance(mem, (float, int))
    assert isinstance(queue_size, int)
    assert isinstance(node_attr, list)
    try:
        allocated_node = users[user].request_node(
            jobID,
            n_cores,
            mem,
            node_attr,
            users,
            nodes)
    except qpyusers.NoNodeAvailableError as exc:
        users[user].n_queue = queue_size
        return 1, str(exc)
    except KeyError:
        return -1, 'User does not exists.'
    except Exception as ex:
        return -2, ('WARNING: An exception of type {0} occured at add a job.\n'
                    + 'Arguments:\n{1!r}\n'
                    + 'Contact the qpy-team.').format(type(ex).__name__,
                                                      ex.args)
    else:
        users[user].n_queue = queue_size - 1
        return 0, allocated_node


def _handle_remove_job(args, users, nodes):
    """Handle  a request to remove a job
    
    args : (user_name, jobID, queue_size)
    """
    user, jobID, queue_size = args
    assert isinstance(user, str)
    assert isinstance(jobID, int)
    assert isinstance(queue_size, int)
    try:
        users[user].remove_job(jobID, nodes)
        users[user].n_queue = queue_size
        return 0, 'Job removed.'
    except ValueError:
        return 1, 'Job not found'
    except KeyError:
        return -1, 'User does not exists.'
    except Exception as ex:
        return -2, (
            'WARNING: An exception of type {0} occured - remove a job.\n'
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
    (conn,
     multiuser_port,
     multiuser_key) = qpycomm.establish_Listener_connection(
         qpycomm.multiuser_address,
         qpyconst.PORT_MIN_MULTI,
         qpyconst.PORT_MAX_MULTI,
         port=qpycomm.multiuser_port,
         conn_key=qpycomm.multiuser_key)
    qpycomm.write_conn_files(qpysys.multiuser_conn_file,
                             qpycomm.multiuser_address,
                             multiuser_port,
                             multiuser_key)
    while True:
        logger.info("Waiting for a message.")
        try:
            client = conn.accept()
            (action_type, arguments) = client.recv()
        except:
            logger.exception("Connection failed")
        else:
            logger.info('Received request:\n'
                        '%s, internal code %s.\n'
                        'Arguments:\n'
                        '  %s\n',
                        qpyconst.MULTIUSER_REQUEST_NAMES[action_type],
                        action_type,
                        arguments
                        if action_type != qpyconst.MULTIUSER_USER else
                        '(´･_･`) users connection are not logged!')
        try:
            if (action_type == qpyconst.MULTIUSER_NODES):
                status, msg = _handle_reload_nodes(
                    arguments, nodes)

            elif (action_type == qpyconst.MULTIUSER_DISTRIBUTE):
                status, msg = _handle_redistribute_cores(
                    arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_SHOW_VARIABLES):
                status, msg = _handle_show_variables(
                    arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_STATUS):
                status, msg = _handle_show_status(
                    arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_SAVE_MESSAGES):
                status, msg = _handle_save_messages(
                    arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_FINISH):
                client.send((0, 'Finishing qpy-multiuser.'))
                conn.close()
                break

            elif (action_type == qpyconst.MULTIUSER_USER):
                status, msg = _handle_sync_user_info(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_REQ_CORE):
                status, msg = _handle_add_job(arguments, users, nodes)

            elif (action_type == qpyconst.MULTIUSER_REMOVE_JOB):
                status, msg = _handle_remove_job(arguments, users, nodes)

            else:
                status, msg = -1, 'Unknown option: ' + str(action_type)
        except Exception as ex:
            logger.exception("An error occured")
            template = ('WARNING: an exception of type {0} occured.\n'
                        + 'Arguments:\n{1!r}'
                        + '\nContact the qpy-team.')
            try:
                client.send((-10, template.format(type(ex).__name__, ex.args)))
            except Exception:
                logger.exception("An error occured while returning a message.")
                pass
        except BaseException as ex:
            logger.exception("An error occured")
            template = ('WARNING: an exception of type {0} occured.\n'
                        + 'Arguments:\n{1!r}\n'
                        + 'Contact the qpy-team.'
                        + ' qpy-multiuser is shutting down.')
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
