import logging
import os.path
from os.path import expanduser, normpath, dirname, abspath
import yaml
import re

CONFIG_BROKER = {}
CONFIG_SERVICES = {}
CONFIG_DB = {}
CONFIG_LOGGING = {}
CONFIG_CATEGORIES = {"broker": CONFIG_BROKER, "services": CONFIG_SERVICES, "db": CONFIG_DB,
                     "logging": CONFIG_LOGGING}

# set the location of the DATA Act broker config files
CONFIG_PATH = os.path.join(dirname(abspath(__file__)), 'config.yml')
if "env" in os.environ:
    env = os.environ["env"]
else:
    env = "local"

ENV_PATH = os.path.join(dirname(abspath(__file__)), '{}_config.yml'.format(env))
SECRET_PATH = os.path.join(dirname(abspath(__file__)), '{}_secrets.yml'.format(env))
path_list = [CONFIG_PATH, ENV_PATH, SECRET_PATH]

# set the location of the Alembic config file
ALEMBIC_PATH = os.path.join(dirname(abspath(__file__)), 'alembic.ini')
MIGRATION_PATH = os.path.join(dirname(abspath(__file__)), 'migrations')

for config_path in path_list:
    try:
        with open(config_path) as c:
            # Default to empty dictionary if file is empty
            CONFIG_ALL = yaml.load(c) or {}
    except IOError:
        raise IOError('Error reading a config file. Please make sure this file exists'
                      ' before starting the DATA Act broker: {}'.format(config_path))

    for category_name in CONFIG_CATEGORIES:
        CONFIG_CATEGORIES[category_name].update(CONFIG_ALL.get(category_name, {}))

# Get path to installation
CONFIG_BROKER['path'] = dirname(dirname(abspath(__file__)))

# for backward-compatibility, differentiate between local runs and AWS
if CONFIG_BROKER['use_aws'] is True or CONFIG_BROKER['use_aws'] == "true":
    CONFIG_BROKER['local'] = False
    # AWS flag is on, so make sure all needed AWS info is present
    required_aws_keys = ['aws_bucket', 'aws_role', 'aws_region', 'aws_create_temp_credentials',
                         'static_files_bucket', 'help_files_path']
    for k in required_aws_keys:
        try:
            CONFIG_BROKER[k]
        except KeyError as e:
            raise KeyError('Config error: use_aws is True, but the {} key is'
                           ' missing from the config.yml file'.format(k))
        if not CONFIG_BROKER[k]:
            raise ValueError('Config error: use_aws is True but {} value is '
                             'missing'.format(k))

    help_files_path = CONFIG_BROKER["help_files_path"]
    CONFIG_BROKER["help_files_path"] = "".join([help_files_path, "/"]) if help_files_path[-1] != "/" \
        else help_files_path
else:
    CONFIG_BROKER['local'] = True
    CONFIG_BROKER['aws_bucket'] = None
    CONFIG_BROKER['aws_role'] = None
    CONFIG_BROKER['aws_create_temp_credentials'] = None
    CONFIG_BROKER['aws_region'] = None

    # if not using AWS and no error report path specified,
    # default to `data_act_broker` in user's home dir
    error_report_path = CONFIG_SERVICES['error_report_path']
    if not error_report_path:
        error_report_path = os.path.join(expanduser('~'), 'data_act_broker')
    normpath(error_report_path)
    CONFIG_SERVICES['error_report_path'] = error_report_path

storage_path = CONFIG_BROKER['d_file_storage_path']
if storage_path[-1] != os.path.sep:
    CONFIG_BROKER['d_file_storage_path'] = "".join([storage_path, os.path.sep])

# if no broker file path specified,
# default to `data_act_broker` in user's home dir
broker_files = CONFIG_BROKER['broker_files']
if not broker_files:
    broker_files = os.path.join(expanduser('~'), 'data_act_broker')
elif len(os.path.splitext(broker_files)[1]):
    # if config's broker_files is set to a actual filename
    # just use the directory
    broker_files = os.path.split(broker_files)[0]
normpath(broker_files)
if broker_files[-1] != os.path.sep:
    broker_files += os.path.sep
CONFIG_BROKER['broker_files'] = broker_files

# normalize logging path, if given
log_path = CONFIG_LOGGING['log_files']
if log_path:
    CONFIG_LOGGING['log_files'] = normpath(log_path)

# we don't want http:// or ports in the host variables
CONFIG_SERVICES['broker_api_host'] = re.sub(
    'http://|:(.*)', '', CONFIG_SERVICES['broker_api_host'])
CONFIG_SERVICES['validator_host'] = re.sub(
    'http://|:(.*)', '', CONFIG_SERVICES['validator_host'])
# if hosts in config file are set to 0.0.0.0, override to
# 127.0.0.1 for cross-platform compatibility
if CONFIG_SERVICES['broker_api_host'] == '0.0.0.0':
    CONFIG_SERVICES['broker_api_host'] = '127.0.0.1'

if CONFIG_SERVICES["broker_api_port"] == 443:
    # Use https
    CONFIG_SERVICES["protocol"] = "https"
else:
    CONFIG_SERVICES["protocol"] = "http"

# Log some values from config
log_message = ""
if "values_to_log" in CONFIG_LOGGING:
    # If no values specified, don't do logging
    for category_yaml_name in CONFIG_LOGGING["values_to_log"]:
        category = CONFIG_CATEGORIES[category_yaml_name]
        category_message = "### {}".format(category_yaml_name)
        for key in CONFIG_LOGGING["values_to_log"][category_yaml_name]:
            value = category.get(key, "Value not provided in config")
            category_message = "{}, {}: {}".format(category_message, key, value)
        log_message = " ".join([log_message, category_message])
# Log config values along with warnings for missing files
if log_message:
    # Logging is not configured yet; create a console logger to print this
    # message
    _logger = logging.getLogger('config-printer')
    _logger.setLevel(logging.INFO)
    _logger.addHandler(logging.FileHandler(os.path.join(
        CONFIG_LOGGING['log_files'], 'info.log')))
    _logger.info(log_message)

# TODO: error-handling for db config?
# TODO: type checking and fixing for int stuff like ports?
