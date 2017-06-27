import logging
import os
import sys
import pd
import paramiko
import socket
import zipfile

from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)

# Paramiko client configuration
UseGSSAPI = True             # enable GSS-API / SSPI authentication
DoGSSAPIKeyExchange = True

# sftp -i SAMDATAProd -P 22 samdataprod04@66.77.18.174


def parse_sam_file(file, sess):
    logger.info("starting file " + str(file.name))

    csv_file = os.path.splitext(os.path.basename(file.name))[0]
    zfile = zipfile.ZipFile(file.name)
    data = pd.read_csv(zfile.open(csv_file), dtype=str, usecols=[1, 11, 49, 90])

    # parse out column 90 (pandas dataframe)
    # each value will be -> things^more things^other things~things2^more things2^other things2~ etc etc


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
        dirlist = sftp.listdir('/current/SAM/6_EXECCOMP')
        print("Dirlist: %s" % dirlist)

        for item in dirlist:
            file = sftp.file(item)
            # if running historical loader
            #     delete exec comp data
            #     for each monthly file
            #         pull all data
            #         save values as executive_compensation models in the DB
            # if running daily loader
            #     pull today's file
            #     save values as executive_compensation models in the DB
            parse_sam_file(file, sess)
