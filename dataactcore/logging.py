from datetime import datetime
import json
import logging
import os.path
import traceback

from dataactcore.config import CONFIG_LOGGING
from dataactcore.utils.responseException import ResponseException


def deep_merge(left, right):
    """Deep merge dictionaries, appending iterables, replacing values from
    right"""
    if isinstance(left, dict) and isinstance(right, dict):
        result = left.copy()
        for key in right:
            if key in left:
                result[key] = deep_merge(left[key], right[key])
            else:
                result[key] = right[key]
        return result
    elif isinstance(left, (tuple, list)) and isinstance(right, (tuple, list)):
        return tuple(left) + tuple(right)
    else:
        return right


# Reasonable defaults to avoid clutter in our config files
DEFAULT_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'default': {
            'format': "%(asctime)s %(levelname)s:%(name)s:%(message)s"
        },
        'deprecated.exception': {
            '()': 'dataactcore.logging.DeprecatedJSONFormatter',
            'format': "%(asctime)s %(levelname)s:%(name)s:%(message)s",
        },
    },
    'handlers': {
        'default': {
            'formatter': 'default',
            'class': 'logging.StreamHandler'
        },
        'deprecated.debug': {
            'formatter': 'default',
            'class': 'logging.FileHandler',
            'filename': os.path.join(CONFIG_LOGGING['log_files'], 'debug.log')
        },
        'deprecated.exception': {
            'formatter': 'deprecated.exception',
            'class': 'logging.FileHandler',
            'filename': os.path.join(CONFIG_LOGGING['log_files'], 'error.log')
        },
        'deprecated.info': {
            'formatter': 'default',
            'class': 'logging.FileHandler',
            'filename': os.path.join(CONFIG_LOGGING['log_files'], 'info.log')
        },
        'deprecated.smx': {
            'formatter': 'default',
            'class': 'logging.FileHandler',
            'filename': os.path.join(CONFIG_LOGGING['log_files'], 'smx.log')
        }
    },
    'loggers': {
        # i.e. "all modules"
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        },
        'deprecated.debug': {
            'handlers': ['deprecated.debug'],
            'level': 'DEBUG',
            'propagate': False
        },
        'deprecated.exception': {
            'handlers': ['deprecated.exception'],
            'level': 'ERROR',
            'propagate': False
        },
        'deprecated.info': {
            'handlers': ['deprecated.info'],
            'level': 'DEBUG',
            'propagate': False
        },
        'deprecated.smx': {
            'handlers': ['deprecated.smx'],
            'level': 'DEBUG',
            'propagate': False
        },
    }
}


def configure_logging():
    config = DEFAULT_CONFIG
    if 'python_config' in CONFIG_LOGGING:
        config = deep_merge(config, CONFIG_LOGGING['python_config'])
    logging.config.dictConfig(config)


class DeprecatedJSONFormatter(logging.Formatter):
    """Formats messages into the JSON CloudLogger used to generate."""
    def formatException(self, exc_info):
        type_, exception, tb = exc_info
        trace = traceback.extract_tb(tb, 10)
        data = {
            'error_log_type': str(type_),
            'error_log_message': str(exception),
            'error_log_wrapped_message': '',
            'error_log_wrapped_type': '',
            'error_log_trace': trace,
            'error_timestamp': str(datetime.utcnow()),
        }
        if (isinstance(exception, ResponseException) and
                exception.wrappedException):
            wrapped = exception.wrappedException
            data['error_log_wrapped_type'] = str(type(wrapped))
            data['error_log_wrapped_message'] = str(wrapped)
        return json.dumps(data)

    def format(self, record):
        """Copy-pasta of the built in `format` function, except that we don't
        add newlines in between the message and exception"""
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = self.formatMessage(record)
        if record.exc_info:
            print(record.exc_info)
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            s = s + record.exc_text
        # We also don't include the additional stack info, as it's part of the
        # exception
        # if record.stack_info:
        #     s = s + self.formatStack(record.stack_info)
        return s
