import logging
import argparse
import re

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging
from dataactcore.utils.duns import get_client, REMOTE_SAM_EXEC_COMP_DIR
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def main():
    logger.info("Starting historical transaction executive compensation backfill.")

    parser = argparse.ArgumentParser(description='Backfill historical executive compensation data for transactions.')
    parser.add_argument('-k', '--ssh_key', help='private key used to access the API remotely', required=True)
    args = parser.parse_args()

    root_dir = CONFIG_BROKER['d_file_storage_path']
    client = get_client(ssh_key=args.ssh_key)
    sftp = client.open_sftp()
    # dirlist on remote host
    dirlist = sftp.listdir(REMOTE_SAM_EXEC_COMP_DIR)

    sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist if re.match('.*MONTHLY_\d+',
                                                                                             monthly_file)])

    logger.info(sorted_monthly_file_names)

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
