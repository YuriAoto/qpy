""" qpy - Users configurations in qpy

"""
import os

import termcolor.termcolor as termcolour

import qpy_system as qpysys
import qpy_logging as qpylog
import qpy_constants as qpyconst
import qpy_useful_cosmetics as qpyutil
from qpy_exceptions import *

class Configurations(object):
    """The current configuration of qpy.
    
    Attributes:
    config_file (str)        The file with configurations
    messages (Messages)      Messages to the user
    job_fmt_pattern (str)    A pattern for the check
    use_colour (bool)        (default = True) If True, check is coloured
                             (for terminal outputs)
    colour_scheme (list)     Colours to the check command
    use_script_copy (bool)   (default = True) If True,
                             save scripts to execute original versions
    sub_paused (True)        If True, job submission is paused
    default_attr (list)      A list with the default node attributes
    and_attr (list)          A list with attributes to be added with "and"
    or_attr (list)           A list with attributes to be added with "or"
    sleep_time_sub_ctrl (int)      (default = 1) Time that sub_ctrl is
                                   paused between each check
    sleep_time_check_run (int)     (default = 10) Time that check_run is
                                   paused between each check
    source_these_files (list)      (default = ['~/.bash_profile'])
                                   files to be sourced in every job run
    ssh_p_key_file                 (defaut = None)
    logger_level (string, int)     (default = 'warning')
    logger                         A logger to print messages to file
                                   (global) master_log_file
    
    Behaviour:
    Contains the messages
    and the several variables that can be customized
    by the user.
    
    See also:
    Messages
    """
    __slots__ = (
        'config_file',
        'messages',
        'job_fmt_pattern',
        'use_colour',
        'colour_scheme',
        'use_script_copy',
        'sub_paused',
        'default_attr',
        'and_attr',
        'or_attr',
        'sleep_time_sub_ctrl',
        'sleep_time_check_run',
        'source_these_files',
        'ssh_p_key_file',
        'logger_level',
        'logger')
    
    def __init__(self, config_file):
        """Initialise a new set of configurations
        
        Arguments:
        config_file (str)    The file with configurations
        
        Behaviour:
        Read configurations from the file config_file or initiate them
        with some appropriate default values and then write on the file
        """
        self.config_file = config_file
        self.messages = qpylog.Messages()
        self.job_fmt_pattern = qpyconst.JOB_FMT_PATTERN_DEF
        self.use_colour = True
        self.colour_scheme = qpyconst.POSSIBLE_COLOURS[:5]
        self.use_script_copy = False
        self.sub_paused = False
        self.default_attr = []
        self.and_attr = []
        self.or_attr = []
        self.sleep_time_sub_ctrl = 1
        self.sleep_time_check_run = 10
        self.source_these_files = []
        self.ssh_p_key_file = None
        self.logger_level = 'debug' #'warning'
        self.logger = qpylog.configure_logger(qpysys.master_log_file,
                                              qpylog.logging.DEBUG)

        if os.path.isfile(self.config_file):
            f = open(self.config_file, 'r')
            for l in f:
                l_spl = l.split()
                if not l_spl:
                    continue
                key = l_spl[0]
                if key == 'checkFMT':
                    val = l.strip()[10:-1]
                elif key == 'job_fmt_pattern':
                    val = l.strip()[17:-1]
                elif len(l_spl) == 1:
                    val = ()
                elif len(l_spl) == 2:
                    val = l_spl[1]
                else:
                    val = l_spl[1:]
                try:
                    msg = self.set_key(key, val)
                except (qpyKeyError, qpyValueError) as e:
                    self.messages.add('Reading config file: ' + str(e))
            f.close()
        else:
            self.write_on_file()

    def set_key(self, k, v):
        """Set a new configuration value
        
        Arguments:
        k   key
        v   value
        
        Behaviour:
        Set the value given by v to the configuration
        key k.
        
        Return:
        msg (str)    An informative message.

        Raise:
        qpyKeyError
        qpyValueError
        """
        if k == 'checkFMT' or k == 'job_fmt_pattern': # job_fmt_pattern: obsolete
            if isinstance(v, list):
                v = ' '.join(v)
            if v == 'default':
                self.job_fmt_pattern = JOB_FMT_PATTERN_DEF
                msg = ('Check pattern restored to the default value: '
                       + repr(self.job_fmt_pattern) + '.')
            else:
                self.job_fmt_pattern = v.decode('string_escape')
                msg = ('Check pattern modified to '
                       + repr(self.job_fmt_pattern) + '.')

        elif k == 'paused_jobs':
            try:
                self.sub_paused = qpyutil.true_or_false(v)
            except:
                raise qpyValueError("Value for paused_jobs must be true or false.")
            else:
                msg = "paused_jobs set to " + str(self.sub_paused) + '.'

        elif k == 'defaultAttr':
            try:
                self.default_attr = [v] if isinstance(v, str) else v
            except:
                raise qpyValueError("Value for " + k + " must be a string.")
            else:
                if v:
                    msg = k + " set to " + ' '.join(self.default_attr) + '.'
                else:
                    msg = k + " unset."

        elif k == 'andAttr':
            try:
                self.and_attr = [v] if isinstance(v, str) else v
            except:
                raise qpyValueError("Value for " + k + " must be a string.")
            else:
                if v:
                    msg = k + " set to " + ' '.join(self.and_attr) + '.'
                else:
                    msg = k + " unset."

        elif k == 'orAttr':
            try:
                self.or_attr = [v] if isinstance(v, str) else v
            except:
                raise qpyValueError("Value for " + k + " must be a string.")
            else:
                if v:
                    msg = k + " set to " + ' '.join(self.or_attr) + '.'
                else:
                    msg = k + " unset."

        elif k == 'copyScripts' or k == 'use_script_copy': # use_script_copy: obsolete
            try:
                self.use_script_copy = qpyutil.true_or_false(v)
            except:
                raise qpyValueError("Value for copyScripts must be true or false.")
            else:
                msg = "copyScripts set to " + str(self.use_script_copy)

        elif k == 'saveMessages' or k == 'save_messages':
            try:
                self.messages.save = qpyutil.true_or_false(v)
            except:
                raise qpyValueError("Value for saveMessages must be true or false.")
            else:
                msg = "saveMessages set to " + str(self.messages.save)

        elif k == 'maxMessages':
            try:
                self.messages.max_len = int(v)
            except:
                raise qpyValueError("Value for maxMessages must be an integer.")
            else:
                msg = "maxMessages set to " + str(self.messages.max_len)

        elif k == 'ssh_p_key_file' or k == 'ssh_pKey':
            if v == 'None':
                self.ssh_p_key_file = None
            else:
                self.ssh_p_key_file = v
            msg = "ssh_pKey set to " + str(self.ssh_p_key_file)

        elif k == 'cleanMessages':
            self.messages.clean()
            msg = "Messages were cleand."

        elif k == 'loggerLevel':
            if v in ['debug', 'DEBUG']:
                vnew = qpylog.logging.DEBUG
            elif v in ['info', 'INFO']:
                vnew = qpylog.logging.INFO
            elif v in ['warning', 'WARNING']:
                vnew = qpylog.logging.WARNING
            elif v in ['error', 'ERROR']:
                vnew = qpylog.logging.ERROR
            elif v in ['critical', 'CRITICAL']:
                vnew = qpylog.logging.CRITICAL
            else:
                try:
                    vnew = int(v)
                except:
                    vnew = None
            if vnew is not None:
                self.logger_level = v
                self.logger.setLevel(vnew)
                msg = 'Logger level set to ' + v
            else:
                raise qpyValueError('Unknown logging level: ' + v)

        elif k == 'colour' or k == 'use_colour':
            try:
                self.use_colour = qpyutil.true_or_false(v)
            except:
                raise qpyValueError("Value for colour must be true or false.")
            else:
                msg = "colour set to " + str(self.use_colour)

        elif k == 'coloursScheme':
            for i in v:
                if not i in qpyconst.POSSIBLE_COLOURS:
                    raise qpyValueError('Unknown colour: ' + i)
            if len(v) != 5:
                raise qpyValueError('Give five colours for coloursScheme.')
            self.colour_scheme = list(v)
            msg = 'Colours scheme changed.\n'

        elif k == 'sleepTimeSubCtrl':
            try:
                self.sleep_time_sub_ctrl = float(v)
            except:
                raise qpyValueError("Value for sleepTimeSubCtrl must be a float number.")
            else:
                msg = ("sleepTimeSubCtrl set to "
                       + str(self.sleep_time_sub_ctrl) + '.')

        elif k == 'sleepTimeCheckRun':
            try:
                self.sleep_time_check_run = float(v)
            except:
                raise qpyValueError("Value for sleepTimeCheckRun must be a float number.")
            else:
                msg = ("sleepTimeCheckRun set to " +
                       str(self.sleep_time_check_run) + '.')

        elif k == 'sourceTheseFiles':
            if isinstance(v, list):
                self.source_these_files = v
                msg = ("sourceTheseFiles set to "
                       + str(self.source_these_files) + '.')
            elif isinstance(v, str):
                self.source_these_files = [v]
                msg = ("sourceTheseFiles set to "
                       + str(self.source_these_files) + '.')
            else:
                raise qpyValueError("sourceTheseFiles should receive a "
                                       + "file name or a list of files.")

        else:
            raise qpyKeyError('Unknown key: ' + k)

        return msg

    def write_on_file(self):
        """Write the current configurations in the file."""
        f = open(self.config_file, 'w')
        f.write('paused_jobs '  + str(self.sub_paused)      + '\n')
        f.write('saveMessages ' + str(self.messages.save)   + '\n')
        f.write('maxMessages '  + str(self.messages.max_len)+ '\n')
        f.write('loggerLevel '  + str(self.logger_level)    + '\n')
        f.write('defaultAttr '  + ' '.join(self.default_attr) + '\n')
        f.write('orAttr '       + ' '.join(self.or_attr)    + '\n')
        f.write('andAttr '      + ' '.join(self.and_attr)   + '\n')
        f.write('checkFMT '     +repr(self.job_fmt_pattern) + '\n')
        f.write('ssh_pKey '     + str(self.ssh_p_key_file)  + '\n')
        f.write('copyScripts '  + str(self.use_script_copy) + '\n')
        f.write('colour '       + str(self.use_colour)      + '\n')
        f.write('coloursScheme '
                 + str(self.colour_scheme[0]) + ' '
                 + str(self.colour_scheme[1]) + ' '
                 + str(self.colour_scheme[2]) + ' '
                 + str(self.colour_scheme[3]) + ' '
                 + str(self.colour_scheme[4]) + '\n')
        f.write('sleepTimeSubCtrl '  + str(self.sleep_time_sub_ctrl)  + '\n')
        f.write('sleepTimeCheckRun ' + str(self.sleep_time_check_run) + '\n')
        f.write('sourceTheseFiles ')
        for i in self.source_these_files:
            f.write(i + ' ')
        f.write('\n')
        f.close()

    def __str__(self):
        """A formatted version of the configurations.
        
        TODO: + on str is not very good. Use .join?
        """
        msg = 'Check pattern: ' + repr(self.job_fmt_pattern) + '\n'
        msg += ('Using a copied version of run script: '
                + str(self.use_script_copy) + '\n')
        msg += 'Using coloured check: ' + str(self.use_colour) + '\n'
        msg += ('Sleeping time in submission control: '
                + str(self.sleep_time_sub_ctrl) + '\n')
        msg += ('Sleeping time in check run: '
                + str(self.sleep_time_check_run) + '\n')
        if self.default_attr:
            msg += ('Default node attributes: '
                    + ' '.join(self.default_attr) + '\n')
        if self.and_attr:
            msg += ('"and" node attributes: '
                    + ' '.join(self.and_attr) + '\n')
        if self.or_attr:
            msg += '"or" node attributes: ' + ' '.join(self.or_attr) + '\n'
        if self.ssh_p_key_file is not None:
            msg += 'Using ssh private key from ' + self.ssh_p_key_file + '\n'
        if (len(self.source_these_files)):
            msg += 'These files are sourced when running a job:\n'
            for i in self.source_these_files:
                msg += '  ' + i + '\n'
        if (self.use_colour):
            msg += 'Colours:\n'
            for i in range(len(qpyconst.JOB_STATUS)):
                msg += '  - ' + termcolour.colored(qpyconst.JOB_STATUS[i],
                                                   self.colour_scheme[i])
                msg += ' (' + self.colour_scheme[i] + ')\n'
        if (self.sub_paused):
            msg += 'Job submission is paused\n'
        if (self.messages.save):
            msg += ('A maximum of ' + str(self.messages.max_len)
                    + ' messages are being saved\n')
        if (len(self.messages) > 0):
            msg += 'Last messages:\n' + str(self.messages) + '\n'
        msg += 'Logger level: ' + str(self.logger_level) + '\n'
        return msg
