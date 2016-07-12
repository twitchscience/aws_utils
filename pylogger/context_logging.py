"""This library makes it convenient to output log messages as formatted JSON.

1. A JSON formatter is added to the root logger.
2. We provide a ContextAdapter class, which when applied to a logger automatically adds background information
   to log messages:  environment, PID, hostname, and line where the logger is called.
3. For convenience, static functions are provided which route to the root logger through a ContextAdapter.

Any additional fields may be provided as key-value pairs in the extra kwarg.  Also exc_info works as in default
Python logging.
"""

import datetime
import inspect
import logging
import os
import socket

from logging import LoggerAdapter
from pythonjsonlogger import jsonlogger


# Basically the JSON formatter provided by python-json-logger, but allows the level of the log message to be printed
# as part of the message.
class _JsonLevelFormatter(jsonlogger.JsonFormatter):
    def __init__(self, *args, **kwargs):
        super(_JsonLevelFormatter, self).__init__(*args, **kwargs)
        del self._skip_fields['levelname']

logHandler = logging.StreamHandler()
logHandler.setFormatter(_JsonLevelFormatter())
logging.getLogger().addHandler(logHandler)

__process_environment__ = dict(env=os.getenv('CLOUD_ENVIRONMENT', 'local'),
                               pid=os.getpid(),
                               host=socket.gethostname())


class Adapter(LoggerAdapter):
    """Adds context information to log messages."""
    def __init__(self, logger, stack_depth=0):
        """
        :param logger: the underlying logger instance.
        :param stack_depth: the number of steps up the stack to take before recording the filename and line number.
        Defaults to 0, i.e., the line where this object's logging methods are called.
        """
        super(Adapter, self).__init__(logger, __process_environment__)
        self.__stack_depth = stack_depth + 2

    def process(self, msg, kwargs):
        (msg, kwargs) = LoggerAdapter.process(self, msg, kwargs)
        kwargs['extra']['timestamp'] = datetime.datetime.now()

        stack = inspect.stack()
        if len(stack) > self.__stack_depth:
            log_line = stack[self.__stack_depth]
            kwargs['extra']['caller'] = os.path.basename(log_line[1]) + ":" + str(log_line[2])

        return msg, kwargs


__root_logger__ = Adapter(logging.getLogger(), 1)


def debug(msg, *args, **kwargs):
    """Log a message to the root logger at DEBUG level."""
    __root_logger__.debug(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
    """Log a message to the root logger at INFO level."""
    __root_logger__.info(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
    """Log a message to the root logger at WARNING level."""
    __root_logger__.warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
    """Log a message to the root logger at ERROR level."""
    __root_logger__.error(msg, *args, **kwargs)


def critical(msg, *args, **kwargs):
    """Log a message to the root logger at CRITICAL level."""
    __root_logger__.critical(msg, *args, **kwargs)


def log(lvl, msg, *args, **kwargs):
    """Log a message to the root logger at specified level."""
    __root_logger__.log(lvl, msg, *args, **kwargs)


def exception(msg, *args, **kwargs):
    """Log a message to the root logger at ERROR level.  Automatically include exception information."""
    __root_logger__.exception(msg, *args, **kwargs)
