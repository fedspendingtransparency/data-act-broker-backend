import os.path
from os.path import expanduser, normpath, dirname, abspath
import yaml
import re

# set the location of the DATA Act broker config file
CONFIG_PATH = os.path.join(dirname(abspath(__file__)), 'config.yml')
# set the location of the Alembic config file
ALEMBIC_PATH = os.path.join(dirname(abspath(__file__)), 'alembic.ini')
MIGRATION_PATH = os.path.join(dirname(abspath(__file__)), 'migrations')

try:
    with open(CONFIG_PATH) as c:
        CONFIG_ALL = yaml.load(c)
except IOError:
    raise IOError('Error reading the config file. Please make sure this file exists'
           ' before starting the DATA Act broker: {}'.format(CONFIG_PATH))

CONFIG_BROKER = CONFIG_ALL['broker']
CONFIG_SERVICES = CONFIG_ALL['services']
CONFIG_DB = CONFIG_ALL['db']
CONFIG_LOGGING = CONFIG_ALL['logging']
CONFIG_JOB_QUEUE = CONFIG_ALL['job-queue']

# Get path to installation
CONFIG_BROKER['path'] = dirname(dirname(abspath(__file__)))

# for backward-compatibility, differentiate between local runs and AWS
if CONFIG_BROKER['use_aws'] is True or CONFIG_BROKER['use_aws'] == "true":
    CONFIG_BROKER['local'] = False
    CONFIG_BROKER['broker_files'] = None
    # AWS flag is on, so make sure all needed AWS info is present
    required_aws_keys = ['aws_bucket',
        'aws_role', 'aws_region', 'aws_create_temp_credentials']
    for k in required_aws_keys:
        try:
            CONFIG_BROKER[k]
        except KeyError as e:
            raise KeyError('Config error: use_aws is True, but the {} key is'
                ' missing from the config.yml file'.format(k))
        if not CONFIG_BROKER[k]:
            raise ValueError('Config error: use_aws is True but {} value is '
                 'missing'.format(k))
else:
    CONFIG_BROKER['local'] = True
    CONFIG_BROKER['aws_bucket'] = None
    CONFIG_BROKER['aws_role'] = None
    CONFIG_BROKER['aws_create_temp_credentials'] = None
    CONFIG_BROKER['aws_region'] = None

    # if not using AWS and no broker file path specified,
    # default to `data_act_broker` in user's home dir
    broker_files = CONFIG_BROKER['broker_files']
    if not broker_files:
        broker_files = os.path.join(expanduser('~'), 'data_act_broker')
    elif len(os.path.splitext(broker_files)[1]):
        # if config's broker_files is set to a actual filename
        # just use the directory
        broker_files = os.path.split(broker_files)[0]
    normpath(broker_files)
    CONFIG_BROKER['broker_files'] = broker_files

    # if not using AWS and no error report path specified,
    # default to `data_act_broker` in user's home dir
    error_report_path = CONFIG_SERVICES['error_report_path']
    if not error_report_path:
        error_report_path = os.path.join(expanduser('~'), 'data_act_broker')
    normpath(error_report_path)
    CONFIG_SERVICES['error_report_path'] = error_report_path

    # if not using AWS and no DynamoDB settings specified, default
    # to localhost and port 8000
    if not CONFIG_DB['dynamo_host']:
        # TODO: dynamo host/port don't do anything; code assumes localhost/8000
        CONFIG_DB['dynamo_host'] = '127.0.0.1'
    if not CONFIG_DB['dynamo_port']:
        CONFIG_DB['dynamo_port'] = 8000
    # TODO: can we test that local dynamo is up and running? if not, route calls hang

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
if CONFIG_SERVICES['validator_host'] == '0.0.0.0':
    CONFIG_SERVICES['validator_host'] = '127.0.0.1'
if CONFIG_DB['dynamo_host'] == '0.0.0.0':
    CONFIG_DB['dynamo_host'] = '127.0.0.1'

# TODO: error-handling for db config?
# TODO: type checking and fixing for int stuff like ports?
