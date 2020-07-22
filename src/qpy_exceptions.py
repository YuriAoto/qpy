""" qpy - Exceptions

"""


class qpyError(Exception):
    """Main qpy Exception.

    Inherit always from this class.
    """
    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return str(self.message)


class qpyParseError(qpyError):
    pass


class qpyHelpException(qpyError):
    pass


class qpyKeyError(qpyError):
    pass


class qpyValueError(qpyError, ValueError):
    pass


class qpyConnectionError(qpyError):
    """Exceptions from SSH and multuprocessing connections."""
    pass


class qpyUnknownError(qpyError):
    def __init__(self, msg, exc_info):
        qpyError.__init__(self, msg)
        (self.exc_type,
         self.exc_value,
         self.exc_traceback) = exc_info

    def __str__(self):
        return '\n'.join(map(str, [self.message,
                                   self.exc_type,
                                   self.exc_value,
                                   self.exc_traceback]))
