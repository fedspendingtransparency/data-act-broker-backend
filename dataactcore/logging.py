import logging
import os.path

from dataactcore.config import CONFIG_LOGGING


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
        }
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
