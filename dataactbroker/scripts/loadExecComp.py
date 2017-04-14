import logging
import sys

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactcore.config import CONFIG_BROKER, CONFIG_SERVICES
import paramiko
import socket


logger = logging.getLogger(__name__)

# Paramiko client configuration
UseGSSAPI = True             # enable GSS-API / SSPI authentication
DoGSSAPIKeyExchange = True

# sftp -i SAMDATAProd -P 22 samdataprod04@66.77.18.174


def get_config():
    sam_config = CONFIG_BROKER.get('sam')

    if sam_config:
        return sam_config.get('private_key'), sam_config.get('username'), sam_config.get('password'), \
               sam_config.get('host'), sam_config.get('port')

    return None, None, None, None, None

if __name__ == '__main__':
    configure_logging()

    with create_app().app_context():
        sess = GlobalDB.db().session
        private_key, username, password, host, port = get_config()

        if None in (private_key, username, password):
            logger.error("Missing config elements for connecting to SAM")
            sys.exit(1)

        transport = paramiko.Transport((host, port))
        transport.connect(hostkey=private_key, username=username, password=password, gss_host=socket.getfqdn(host),
                          gss_auth=UseGSSAPI, gss_kex=DoGSSAPIKeyExchange)

        sftp = paramiko.SFTPClient.from_transport(transport)

        # dirlist on remote host
        dirlist = sftp.listdir('.')
        print("Dirlist: %s" % dirlist)