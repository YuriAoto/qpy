""" qpy - Parsers for commands

"""
import os
import sys
import subprocess
from collections import namedtuple
from optparse import OptionParser

import qpy_system as qpysys
import qpy_useful_cosmetics as qpyutil
import qpy_constants as qpyconst
from qpy_exceptions import *

def parse_qpy_cmd_line():
    """Parse the command line of qpy."""
    try:
        option = qpyconst.KEYWORDS[sys.argv[1]][0]
    except:
        str_len = 0
        for opt in qpyconst.KEYWORDS:
            if (str_len < len( opt)):
                str_len = len( opt)
        format_spc = '{0:' + str(str_len+1) + 's}'
        usage_msg = 'Usage: ' + sys.argv[0] + ' <option> [<arguments>].\n'
        usage_msg += 'Options:'
        for opt in qpyconst.KEYWORDS:
            usage_msg += ('\n  ' + format_spc.format(opt + ':')
                          + ' ' + qpyconst.KEYWORDS[opt][1])
        sys.exit(usage_msg)

    start_qpy_master = option == qpyconst.JOBTYPE_RESTART
    if start_qpy_master:
        option = qpyconst.JOBTYPE_FINISH

    arguments = ()
    if option == qpyconst.JOBTYPE_SUB:
        job = ''
        next_is_attr = False
        for i in sys.argv[2:]:
            if next_is_attr:
                i = i.replace(' ', '##')
            job += ' ' + i
            next_is_attr = i == '-a' or i == '--node_attr' or i == '--attribute'
        arguments = [job, os.getcwd()]

    if option == qpyconst.JOBTYPE_CHECK:
        arguments = {}
        for x in sys.argv[2:]:
            if x in qpyconst.JOB_STATUS:
                if ('status' in arguments):
                    arguments['status'].append(x)
                else:
                    arguments['status'] = [x]
            else:
                if (os.path.isdir(x)):
                    if 'dir' in arguments:
                        arguments['dir'].append(os.path.abspath(x))
                    else:
                        arguments['dir'] = [os.path.abspath(x)]
                else:
                    try:
                        new_jobids = qpyutil.string_to_int_list(x)
                    except:
                        sys.exit( 'Unknown pattern for checking jobs: ' + x)
                    if ('job_id' in arguments):
                        arguments['job_id'].extend( new_jobids)
                    else:
                        arguments['job_id'] = new_jobids

    elif option == qpyconst.JOBTYPE_CONFIG:
        if (len(sys.argv) == 4):
            arguments = (sys.argv[2], sys.argv[3])
        elif (len( sys.argv) > 4):
            arguments = (sys.argv[2], sys.argv[3:])
        elif (len( sys.argv) == 3):
            arguments = (sys.argv[2], ())

    elif (option == qpyconst.JOBTYPE_KILL
          or option == qpyconst.JOBTYPE_CLEAN):
        if option == qpyconst.JOBTYPE_KILL:
            status_bound = (0, 2)
        else:
            status_bound = (2, 5)
        arguments = []
        for x in sys.argv[2:]:
            try:
                new_range = qpyutil.string_to_int_list(x)
                arguments.extend( new_range)
            except:
                if (x == 'all'
                    or x in qpyconst.JOB_STATUS[status_bound[0]:status_bound[1]]):
                    arguments.append( x)
                elif (os.path.isdir(x)):
                    arguments.append(os.path.abspath(x))
                else:
                    sys.exit('Range with wrong format or invalid status: ' + x)
        arguments = list(set(arguments))

    elif option == qpyconst.JOBTYPE_CTRLQUEUE:
        if len( sys.argv) < 3:
            sys.exit('Give the queue control type.')
        else:
            if sys.argv[2] == 'jump':
                new_range = []
                for x in sys.argv[3:-1]:
                    try:
                        new_range.extend(qpyutil.string_to_int_list(x))
                    except:
                        sys.exit('Range with wrong format: ' + x)
                if not new_range:
                        sys.exit('Give the jobs to change position.')
                try:
                    pos = int(sys.argv[-1])
                except:
                    if sys.argv[-1] == 'begin':
                        pos = 0
                    elif sys.argv[-1] == 'end':
                        pos = -1
                    else:
                        sys.exit('Invalid jump position: ' + sys.argv[-1])
                arguments = ('jump', new_range, pos)
            else:
                arguments = sys.argv[2:]

    elif option == qpyconst.JOBTYPE_NOTE:
        if len(sys.argv) < 3:
            arguments = ()
        elif len(sys.argv) == 3:
            arguments = [sys.argv[2]]
        else:
            arguments = [sys.argv[2], ' '.join(sys.argv[3:])]

    elif option == qpyconst.JOBTYPE_TUTORIAL:
        pattern = ''
        for i in sys.argv[2:3]:
            pattern += i
        for i in sys.argv[3:]:
            pattern += ' ' + i
        if pattern in qpyconst.KEYWORDS:
            pattern = '--pattern "# ' + pattern + '"'
        elif pattern:
            pattern = '--pattern "' + pattern + '"'
        command = 'less '  + pattern + ' ' + qpysys.tutorial_file
        try:
            subprocess.call(command, shell = True)
        except:
            sys.exit('Error when loading the tutorial.')
        exit()

    return option, arguments, start_qpy_master


def parse_qpy_multiuser_cmd_line():
    """Parse the command line of qpy-access-multiuser.py"""
    start_qpy_multiuser = False
    try:
        option = qpyconst.MULTIUSER_KEYWORDS[sys.argv[1]][0]
    except:
        str_len = 0
        for opt in qpyconst.MULTIUSER_KEYWORDS:
            if (qpyconst.MULTIUSER_KEYWORDS[opt][0] < 0):
                continue
            if (str_len < len( opt)):
                str_len = len( opt)
        format_spc = '{0:' + str( str_len+1) + 's}'
        usage_msg =  'Usage: ' + sys.argv[0] +  ' <option> [<arguments>].\n'
        usage_msg += 'Options:'
        for opt in qpyconst.MULTIUSER_KEYWORDS:
            if (qpyconst.MULTIUSER_KEYWORDS[opt][0] < 0):
                continue
            usage_msg += ('\n  ' + format_spc.format( opt+':')
                          + ' ' + qpyconst.MULTIUSER_KEYWORDS[opt][1])
        sys.exit( usage_msg)

    arguments = ()
    if (option == qpyconst.MULTIUSER_USER):
        try:
            arguments = (sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        except:
            usage_msg = ('Usage: ' + sys.argv[0]
                         + ' __user <user_name> <address> <port> <conn_key>.')
            sys.exit(usage_msg)

    if (option == qpyconst.MULTIUSER_REQ_CORE):
        try:
            arguments = (sys.argv[2],
                         int(sys.argv[3]),
                         int(sys.argv[4]),
                         float(sys.argv[5]),
                         int(sys.argv[6]),
                         [] if len(sys.argv) == 7 else sys.argv[7:])
        except:
            usage_msg = ('Usage: ' + sys.argv[0]
                         + ' __req_core <user_name> <jobID>'
                         + ' <n_cores> <mem> <queue_size> [<node_attr>].')
            sys.exit(usage_msg)

    if (option == qpyconst.MULTIUSER_REMOVE_JOB):
        try:
            arguments = [sys.argv[2], int( sys.argv[3]), int(sys.argv[4])]
        except:
            usage_msg = ('Usage: ' + sys.argv[0]
                         + ' __remove_job <user_name> <job_ID> <queue_size>.')
            sys.exit(usage_msg)

    if (option == qpyconst.MULTIUSER_SAVE_MESSAGES):
        try:
            arguments = [True if (sys.argv[2] == 'true') else False]
        except:
            usage_msg = 'Usage: ' + sys.argv[0] +  ' [true,false].'
            sys.exit( usage_msg)

    if (option == qpyconst.MULTIUSER_START):
        start_qpy_multiuser = True

    elif (option == qpyconst.MULTIUSER_TUTORIAL):    
        pattern = ''
        for i in sys.argv[2:3]:
            pattern += i
        for i in sys.argv[3:]:
            pattern += ' ' + i

        if (pattern in qpyconst.KEYWORDS):
            pattern = '--pattern "# ' + pattern + '"'
        elif (pattern):
            pattern = '--pattern "' + pattern + '"'

        command = 'less ' + pattern + ' ' + qpysys.tutorial_file
        try:
            subprocess.call(command, shell = True)
        except:
            sys.exit( 'Error when loading the tutorial.')
        exit()

    return option, arguments, start_qpy_multiuser


def parse_node_info(L):
    """A parser for the line in nodes_file"""
    lspl = L.split()
    name = lspl.pop(0)
    address = name
    n_cores = int(lspl.pop(0))
    if 'M' in lspl:
        multicore = True
        lspl.remove('M')
    else:
        multicore = False
    for i in lspl:
        if 'address=' in i:
            address = i.split('=')[1]
            lspl.remove(i)
            break
    attributes = lspl

    return name, n_cores, address, multicore, attributes

class JobOptParser(OptionParser):
    """A parser for the job options
    
    Behaviour:
    
    A Job in qpy can be submitted with several options, 
    and the purpose of this class is to handle these options.
    These options can be passed by command line:
    
    $ qpy sub -m 0.1 date
    
    where the options are flagged with the -<letter> and --<word>
    conventions. If the command to be submitted is a script (with its
    full path), options can be given inside the script, for example:
    
    $ cat ./script.sh
    #QPY mem=0.1
    sleep
    $ qpy sub ./script.sh
    
    The options inside the script should follow the convention:

    #QPY <key>=<value>; <key>  = <value>
    #QPY <key> = <value>

    That is: each line has several pairs <key> <value>,
    separated by semicolon: ";".
    The pairs <key> <value> should be separated
    by the equal sign: "=".
    The number of spaces around the equal and semicolon signs
    is arbitrary.
    There are, in general, several possible keys with the same effect
    
    The possible options are:
    
    ## Number of cores (integer) requested for this job:
    
    command line flags:  -n, --cores
    keys for script:     n_cores, number of cores
    
    ## Memory (float) in GB requested for this job:
    
    command line flags:  -m, --mem, --memory
    keys for script:     mem, memory
    
    ## Attributes of the nodes for this job. It can be any logical expression
    of Python, that can have node attributes that will be replaced by a boolean
    (indicating whether the node has that attribute). If passed by command
    line, this logical expression can not have spaces.

    command line flags:  -a, --node_attr, --attributes
    keys for script:     node_attr, node attributes

    ## If flagged (in command line) or set to true in the script,
    copies the script, such that the original version is submitted

    command line flags: -c, --copyScript
    keys for script:    copy script, cpScript, cp_script
    
    NOTE:
    Optparse is deprecated.
    The overwritten
    functions are somewhat mentioned in the documentation.
    
    TODO:
    replace it by argparse
    """
    def exit(self, prog='', message=''):
        raise qpyParseError(message)
    def error(self, message):
        raise qpyParseError(message)
    def print_usage(self):
        pass
    def print_version(self):
        pass
    def print_help(self):
        raise qpyHelpException(self.format_help())

    @classmethod
    def set_parser(cls):
        """Instantiate a parser and set flags for command line."""
        parser = cls()
        parser.add_option("-n", "--cores", dest="cores",
                          help="set the number of cores", default="1")
        parser.add_option("-m", "--mem", "--memory", dest="memory",
                          help="set the memory in GB", default="5")
        parser.add_option("-a", "--node_attr", "--attributes", dest="node_attr",
                          help="set the attributes for node", default='')
        parser.add_option("-c", "--copyScript", dest="cpScript",
                          help="script should be copied",
                          action='store_false')
        parser.add_option("-o", "--originalScript", dest="orScript",
                          help="use original script",
                          action='store_false')
        parser.disable_interspersed_args()
        return parser

    def _scanline(self, line, options):
        """Parse the line for qpy options.
        
        Arguments:
        line (str)       The line to be parsed
        options (dict)   A dictionary with the options
        
        Behaviour:
        If the line sets some qpy option, put it in options
        
        Raise:
        qpyParseError   In case there is an error in the syntax
        """
        if line[0:5] == '#QPY ':
            line_split = line[5:].split(';')
            for kv in line_split:
                if kv:
                    try:
                        k, v = [x.strip() for x in kv.split('=')]
                    except ValueError:
                        raise qpyParseError('Invalid syntax for options inside script: ' + kv)
                    try:
                        if k in ['number of cores',
                                 'n_cores']:
                            options['n_cores'] = int(v)
                        elif k in ['memory',
                                   'mem']:
                            options['mem'] = float(v)
                        elif k in ['node attributes',
                                   'node_attr']:
                            options['node_attr'] = v.split()
                        elif k in ['copy script',
                                   'cpScript',
                                   'cp_script']:
                            options['use_script_copy'] = true_or_false(v)
                        else:
                            raise qpyParseError('Unknown script option: ' + k)
                    except ValueError:
                        raise qpyParseError(
                            "Invalid value for {0}: {1}.".format(k, v))

    def parse_file(self, file_name, options):
        """Parse a submission script file for options set in the script.
        
        Arguments::
        file_name (str)     The file name
        options (dict)      A dictionary with the options that can be set
        
        Raise:
        qpyParseError   In case there is an error in the syntax
        
        See also:
        _scanline
        """
        try:
            with open(file_name, 'r') as f:
                for line in f:
                    self._scanline(line, options)
        except IOError:
            pass

    def parse_cmd_line(self, command, options):
        """Parse the command for options.
        
        Arguments:
        command (str)     The command to be parsed
        options (dict)    The options to be set
        
        Behaviour:        
        Set the options found in the command and return the
        command free of these options.
        
        Return:
        The command, without the parsed options.
        
        Raise:
        qpyParseError    if the parse was not successful
        """
        try:
            parsed_opt, command = self.parse_args(command.split())
            options['n_cores'] = int(parsed_opt.cores)
            options['mem'] = float(parsed_opt.memory)
            if parsed_opt.node_attr:
                options['node_attr'] = parsed_opt.node_attr.split('##')
            else:
                options['node_attr'] = []
            if parsed_opt.cpScript == None and parsed_opt.orScript == None:
                pass
            elif parsed_opt.cpScript != None and parsed_opt.orScript != None:
                raise qpyParseError("Please, do not supply both cpScript and orScript")
            elif parsed_opt.cpScript != None:
                options['use_script_copy'] = True
            else:
                options['use_script_copy'] = False
        except ValueError:
            raise qpyParseError("Please supply only numbers for memory or"
                                + " number of cores, true or false for cpScript")
        return ' '.join(command)
