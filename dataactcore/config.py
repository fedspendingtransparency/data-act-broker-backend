from os.path import dirname, join
import yaml

# set the location of the DATA Act broker config file
CONFIG = join(dirname(__file__), 'config.yml')

with open(CONFIG) as c:
    configs = yaml.load(c)
CONFIG_BROKER = configs['broker']
CONFIG_SERVICES = configs['services']
CONFIG_UPLOADS = configs['uploads']
CONFIG_DB = configs['db']
CONFIG_LOGGING = configs['logging']