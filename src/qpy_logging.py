""" qpy - Logging and messages

"""
import os
import sys
import traceback
import logging
import logging.handlers


def configure_logger(base_file,
                     level=logging.WARNING,
                     logger_name=None
):
    """Set up a logger
    
    Arguments:
    base_file (str)     basename of file name used for log
    level (int, str)    log level in the initialisation
    
    Behaviour:
    note that the TimedRotatingFileHandler adds time info to
    the filenames e.g.:
    mylog.log-> mylog.log.2017-12-...
    change every day at midnight; one week should be enough
    
    Return:
    A logging.Logger
    
    See also:
    logging
    """
    the_logger = logging.getLogger(logger_name)
    the_logger.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s: %(message)s')
    ch = logging.handlers.TimedRotatingFileHandler(
        filename=str(base_file),
        when='midnight',
        interval=1,
        backupCount=7,
        delay=False
    )
    ch.setFormatter(formatter)
    the_logger.addHandler(ch)
    the_logger.propagate = False
    # ch2 = logging.StreamHandler(sys.stdout)
    # ch2.setFormatter(formatter)
    # the_ogger.addHandler(ch2)
    return the_logger


def traceback_exception(msg):
    """Return a string with 'msg' and the exception traceback."""
    exc_type, exc_value, exc_traceback = sys.exc_info()
    tb_msg = msg + ':\n'
    tb_msg += "  Value: " + str(exc_value) + "\n"
    tb_msg += "  Type:  " + str(exc_type) + "\n"
    tb_msg += "  Traceback:\n"
    for tb in traceback.extract_tb(exc_traceback):
        tb_msg + "    in {0}, line {1:d}: {2}, {3}\n".format(tb[0],
                                                             tb[1],
                                                             str(tb[2]),
                                                             tb[3])
    return tb_msg


class Messages(object):
    """The messages for debbuging, generated at run time.
    
    Attributes:
    save (bool)       (default = False) If True, messages are saved
    messages (list)   A list with the messages, as [M, n_times], where
                      M is a string (the message itself) and n_times is
                      the number of time the messages was sent.
    max_len (int)     (default = 100) Maximum number of
                      messages (not counting repetitions)
    
    Behaviour:
    This class handles and stores the messages obtained
    from exceptions and others.
    Repeated messages sent one after another are not doubly
    stored, just a counter is raised.
    If number of messages gets larger than max_len, older messages
    are removed.
    
    TODO:
    replace clean to __del__?
    """
    __slots__ = (
        'save',
        'messages',
        'max_len')

    def __init__(self):
        """Initialise the class."""
        self.save = False
        self.messages = []
        self.max_len = 100

    def __len__(self):
        """Returns the number of messages."""
        return len(self.messages)

    def __repr__(self):
        """Retruns important informations about the messages."""
        return ("Messages<save=" + str(self.save)
                + ";max_len=" + str(self.max_len)
                + ";len=" + len(self.messages)
                + ">")

    def __str__(self):
        """Retruns a formatted version of the messages."""
        x = ''
        for m in self.messages:
            x += m[0]
            if (m[1] > 1):
                x += ' (' + str(m[1]) + 'x)'
            x += '\n'
        return x

    def clean(self):
        """Delete all messages"""
        self.messages = []

    def add(self, M):
        """Add a new message M"""
        if (self.save):
            if (len(self.messages) == 0 or self.messages[-1][0] != M):
                self.messages.append([M, 1])
            else:
                self.messages[-1][1] += 1
            if (len(self) > self.max_len):
                self.messages.pop(0)
