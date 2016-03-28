import os.path
from os.path import expanduser, normpath, dirname
import yaml
import re

# set the location of the DATA Act broker config file
CONFIG_PATH = os.path.join(dirname(__file__), 'config.yml')

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

# for backward-compatibility, differentiate between local runs and AWS
if CONFIG_BROKER['use_aws'] or CONFIG_BROKER['use_aws'] == "true":
    CONFIG_BROKER['local'] = False
    CONFIG_BROKER['broker_files'] = None
else:
    CONFIG_BROKER['local'] = True
    CONFIG_BROKER['aws_bucket'] = None
    CONFIG_BROKER['aws_role'] = None
    CONFIG_BROKER['aws_create_temp_credentials'] = None

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
        CONFIG_DB['dynamo_host'] = '0.0.0.0'
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

# TODO: error-handling for db config?
# TODO: type checking and fixing for int stuff like ports?
