import argparse
import datetime
import logging
import os
import re
import sys
import paramiko

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import DUNS
from dataactcore.utils.parentDuns import sams_config_is_valid, get_duns_batches, update_missing_parent_names
from dataactcore.utils.duns import get_config, parse_sam_file, process_from_dir
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

REMOTE_SAM_DIR = '/current/SAM/2_FOUO/UTF-8/'


def get_client():
    username, password, host, port = get_config()

    if None in (username, password):
        logger.error("Missing config elements for connecting to SAM")
        sys.exit(1)

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        username=username,
        password=password
    )

    return client


def get_parser():
    duns_parser = argparse.ArgumentParser(description='Get the latest data from SAM and update '
                                                      'duns table. By default, it loads the latest daily file.')
    duns_parser.add_argument("--historic", "-i", action="store_true", help='load the oldest monthly zip and all the '
                                                                           'daily files afterwards from the directory.')
    duns_parser.add_argument("--local", "-l", type=str, default=None, help='work from a local directory')
    duns_parser.add_argument("--monthly", "-m", type=str, default=None, help='load a local monthly file')
    duns_parser.add_argument("--daily", "-d", type=str, default=None, help='load a local daily file')
    duns_parser.add_argument("--benchmarks", "-b", action="store_true",
                             help='log times of operations for testing')
    duns_parser.add_argument("--update", "-u", action="store_true",
                             help='Run all daily files since latest last_sam_mod_date in table')
    return duns_parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    historic = args.historic
    local = args.local
    monthly = args.monthly
    daily = args.daily
    benchmarks = args.benchmarks
    update = args.update

    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session
        sftp = None

        wdsl_client = sams_config_is_valid()
        updated_date = datetime.date.today()

        if monthly and daily:
            logger.error("For loading a single local file, you must provide either monthly or daily.")
            sys.exit(1)
        if historic and update:
            logger.error("For multiple file loads you must choose either historic or update.")
            sys.exit(1)
        elif (monthly or daily) and local:
            logger.error("Local directory specified with a local file.")
            sys.exit(1)
        elif monthly:
            parse_sam_file(monthly, sess=sess, monthly=True, benchmarks=benchmarks)
        elif daily:
            parse_sam_file(daily, sess=sess, benchmarks=benchmarks)
        else:
            # dealing with a local or remote directory
            if not local:
                root_dir = CONFIG_BROKER["d_file_storage_path"]

                ssh_client = get_client()
                sftp = ssh_client.open_sftp()
                # dirlist on remote host
                dirlist = sftp.listdir(REMOTE_SAM_DIR)
            else:
                root_dir = local
                dirlist = os.listdir(local)

            # generate chronological list of daily and monthly files
            sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist
                                                if re.match(".*MONTHLY_\d+\.ZIP", monthly_file.upper())])
            sorted_daily_file_names = sorted([daily_file for daily_file in dirlist
                                              if re.match(".*DAILY_\d+\.ZIP", daily_file.upper())])

            if historic or update:
                if historic:
                    if sorted_monthly_file_names:
                        process_from_dir(root_dir, sorted_monthly_file_names[0],
                                         sess, local, sftp, monthly=True, benchmarks=benchmarks)

                        get_duns_batches(wdsl_client, sess, updated_date=updated_date)
                        update_missing_parent_names(sess, updated_date=updated_date)
                    else:
                        logger.info("No monthly file found.")

                if sorted_daily_file_names:
                    if historic:
                        if sorted_monthly_file_names:
                            earliest_daily_file = sorted_monthly_file_names[0].replace("MONTHLY", "DAILY")
                    else:
                        # Insert item into sorted file list with date of last sam mod
                        last_update = sess.query(DUNS.last_sam_mod_date).\
                                order_by(DUNS.last_sam_mod_date.desc()). \
                                filter(DUNS.last_sam_mod_date.isnot(None)). \
                                first()[0].strftime("%Y%m%d")
                        earliest_daily_file = re.sub("_DAILY_[0-9]{8}\.ZIP", "_DAILY_" +
                                                     last_update + ".ZIP", sorted_daily_file_names[0])
                    if earliest_daily_file:
                        sorted_full_list = sorted(sorted_daily_file_names + [earliest_daily_file])
                        daily_files_after = sorted_full_list[sorted_full_list.index(earliest_daily_file) + 1:]
                    else:
                        daily_files_after = sorted_daily_file_names

                if daily_files_after:
                    for daily_file in daily_files_after:
                        process_from_dir(root_dir, daily_file, sess, local, sftp, benchmarks=benchmarks)

                    get_duns_batches(wdsl_client, sess, updated_date=updated_date)
                    update_missing_parent_names(sess, updated_date=updated_date)
                else:
                    logger.info("No daily file found.")
            else:
                if sorted_daily_file_names:

                    if sftp.sock.closed:
                        # Reconnect if channel is closed
                        ssh_client = get_client()
                        sftp = ssh_client.open_sftp()

                    process_from_dir(root_dir, sorted_daily_file_names[-1], sess, local, sftp, benchmarks=benchmarks)

                    get_duns_batches(wdsl_client, sess, updated_date=updated_date)
                    update_missing_parent_names(sess, updated_date=updated_date)
                else:
                    logger.info("No daily file found.")
        sess.close()
