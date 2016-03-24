from os.path import dirname, join
import yaml

# set the location of the DATA Act broker config file
CONFIG_PATH = join(dirname(__file__), 'config.yml')

with open(CONFIG_PATH) as c:
    CONFIG_ALL = yaml.load(c)
CONFIG_BROKER = CONFIG_ALL['broker']
CONFIG_SERVICES = CONFIG_ALL['services']
CONFIG_DB = CONFIG_ALL['db']
CONFIG_LOGGING = CONFIG_ALL['logging']

# for backward-compatibility
if not CONFIG_BROKER['use_aws']:
    CONFIG_BROKER['local'] = False
else:
    CONFIG_BROKER['local'] = True