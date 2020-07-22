""" qpy - Cosmetic/useful functions

"""
import sys
from time import sleep
import re

import qpy_communication as qpycomm
from qpy_exceptions import qpyValueError


def kill_master_instances(user, address, qpy_master_command):
    """Kill all qpy-master instances from this user.
    
    Behaviour:
    It does it only from the same source directory.
    """
    killed_instances = []
    ps_stdout = qpycomm.node_exec(address,
                                  ["ps", "-fu", user],
                                  get_outerr=True,
                                  mode='popen')
    ps_stdout = ps_stdout[0].split('\n')
    for l in ps_stdout:
        if re.search(qpy_master_command + '$', l) is not None:
            pid = l.split()[1]
            qpycomm.node_exec(address,
                              "kill " + pid,
                              get_outerr=False,
                              mode='popen')
            sys.stdout.write('Killing older qpy-master instance: '
                             + pid + '\n')
            killed_instances.append(pid)
    return ' '.join(killed_instances)


def start_master_driver(user, address, qpy_master_command):
    """Start qpy-master.
    
    TODO:
    Only main programs should exit.
    should we indeed write to stdout? and wait?
    """
    sys.stdout.write("Starting qpy-master driver..."
                     + "It takes a few seconds, be patient.\n")
    sleep(5.)
    kill_master_instances(user, address, qpy_master_command)
    qpycomm.node_exec(address,
                      qpy_master_command + ' > /dev/null 2> /dev/null',
                      get_outerr=False,
                      mode='popen')
    exit()


def get_all_children(x, parent_of):
    """Get all children and further generations
    
    Arguments:
    x (str)           The element whose children we are looking for
    parent_of (dict)  Gives the parents of each element
    
    Behaviour:
    The dictionary parent_of should heve strings as values,
    that represent the parent of the key.
    This is a recursive function.
    
    Return:
    A list with all chidren and further generations of
    x, according to the family tree defined by parent_of
    """
    cur_child = []
    for p in parent_of:
        if (parent_of[p] == x):
            cur_child.append(p)
    all_children = []
    for c in cur_child:
        all_children = get_all_children(c, parent_of)
    all_children.extend(cur_child)
    return all_children


def string_to_int_list(x):
    """Parse a string to a list of int
    
    Arguments:
    x (str)    A string to be parsed to integers
    
    Behaviour:
    '1,2,3'    -> [1,2,3]
    '1-3'      -> [1,2,3]
    '1-3,6-10' -> [1,2,3,6,7,8,9,10]
    
    Return:
    a list of integers.
    
    Raise:
    IndexError
    ValueError  if string is not correctly formatted.
    
    TODO:
    Raise specific Exception?
    """
    res = []
    for entry in x.split(','):
        # raises ValueError on 3-
        range_ = [int(num) for num in entry.split('-')]
        if len(range_) not in [1, 2]:
            raise IndexError("No multivalue ranges")
        res.extend(list(range(range_[0], range_[-1] + 1)))
    return res


def true_or_false(v):
    """Return True or False, depending on the string v."""
    if (v.lower() == 'true'):
        return True
    elif (v.lower() == 'false'):
        return False
    else:
        raise qpyValueError('Neither true nor false.')


def get_plural(word_s, stuff):
    """Get the plural
    
    Arguments:
    word_s (tuple, list)      Contains (singular_case, plural_case)
    stuff (list, strings,     The stuff to be checked to be multiple
           positive int)      or single
    
    Behaviour:
    Analyse 'stuff' and return the correct thing, in plural
    or singular. Most a cosmetic function to have gramatically
    correct output.
    
    Return:
    A tuple (correct_case, Predicate or listing)
    
    Raise:
    An Exception if we cannot deal with stuff
    
    Examples:
    get_plural(("job","jobs"),0) => ("jobs", "No")
    get_plural(("job","jobs"),
               ["queued", "running", "killed"])
                    => ("jobs", "queued, running and killed")
    """
    if (isinstance(stuff, list)):
        if (len(stuff) == 0):
            return (word_s[1], 'No')
        elif len(stuff) == 1:
            return (word_s[0], str(stuff[0]))
        elif (len(stuff) > 1):
            ret = ", ".join(stuff[:-1]) + " and " + stuff[-1]
            return (word_s[1], ret)
        else:
            raise Exception("get_plural: negative list length? "
                            + str(word_s) + str(stuff)
                            + "\n Contact the qpy-team.")
    elif (isinstance(stuff, int)):
        if stuff == 0:
            return word_s[1], 'No'
        elif stuff == 1:
            return word_s[0], str(stuff)
        elif stuff > 1:
            return word_s[1], str(stuff)
        else:
            raise Exception("get_plural: negative amount? "
                            + str(word_s) + str(stuff)
                            + "\n Contact the qpy-team.")
    else:
        raise Exception("get_plural:stuff neither int nor list? "
                        + str(type(stuff)) + "\n Contact the qpy-team.")
