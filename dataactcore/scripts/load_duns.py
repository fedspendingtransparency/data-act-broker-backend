import argparse
import datetime
import logging
import os
import re
import sys
import json

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import DUNS
from dataactcore.utils.parentDuns import sam_config_is_valid, update_missing_parent_names
from dataactcore.utils.duns import get_client, parse_duns_file, REMOTE_SAM_DUNS_DIR
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def process_from_dir(root_dir, file_name, sess, local, sftp=None, monthly=False, benchmarks=False, metrics=None):
    """ Process the SAM file found locally or remotely

        Args:
            root_dir: the folder containing the SAM file
            file_name: the name of the SAM file
            sess: the database connection
            local: whether it's local or not
            sftp: the sftp client to pull the CSV from
            monthly: whether it's a monthly file
            benchmarks: whether to log times
            metrics: dictionary representing metrics data for the load
    """
    if not metrics:
        metrics = {}

    file_path = os.path.join(root_dir, file_name)
    if not local:
        if sftp.sock.closed:
            # Reconnect if channel is closed
            ssh_client = get_client()
            sftp = ssh_client.open_sftp()
        logger.info("Pulling {}".format(file_name))
        with open(file_path, "wb") as zip_file:
            sftp.getfo(''.join([REMOTE_SAM_DUNS_DIR, '/', file_name]), zip_file)
    parse_duns_file(file_path, sess, monthly=monthly, benchmarks=benchmarks, metrics=metrics)
    if not local:
        os.remove(file_path)


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
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
    now = datetime.datetime.now()

    parser = get_parser()
    args = parser.parse_args()

    historic = args.historic
    local = args.local
    monthly = args.monthly
    daily = args.daily
    benchmarks = args.benchmarks
    update = args.update

    metrics = {
        'script_name': 'load_duns.py',
        'start_time': str(now),
        'files_processed': [],
        'records_received': 0,
        'adds_received': 0,
        'updates_received': 0,
        'deletes_received': 0,
        'records_ignored': 0,
        'added_duns': [],
        'updated_duns': [],
        'records_added': 0,
        'records_updated': 0,
        'parent_rows_updated': 0,
        'parent_update_date': None
    }

    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session
        sftp = None

        wdsl_client = sam_config_is_valid()
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
            parse_duns_file(monthly, sess=sess, monthly=True, benchmarks=benchmarks, metrics=metrics)
        elif daily:
            parse_duns_file(daily, sess=sess, benchmarks=benchmarks, metrics=metrics)
        else:
            # dealing with a local or remote directory
            if not local:
                root_dir = CONFIG_BROKER["d_file_storage_path"]

                ssh_client = get_client()
                sftp = ssh_client.open_sftp()
                # dirlist on remote host
                dirlist = sftp.listdir(REMOTE_SAM_DUNS_DIR)
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
                        process_from_dir(root_dir, sorted_monthly_file_names[0], sess, local, sftp, monthly=True,
                                         benchmarks=benchmarks, metrics=metrics)
                        metrics['parent_rows_updated'] = update_missing_parent_names(sess, updated_date=updated_date)
                        metrics['parent_update_date'] = str(updated_date)
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
                        process_from_dir(root_dir, daily_file, sess, local, sftp, benchmarks=benchmarks,
                                         metrics=metrics)

                    metrics['parent_rows_updated'] = update_missing_parent_names(sess, updated_date=updated_date)
                    metrics['parent_update_date'] = str(updated_date)
                else:
                    logger.info("No daily file found.")
            else:
                if sorted_daily_file_names:
                    process_from_dir(root_dir, sorted_daily_file_names[-1], sess, local, sftp, benchmarks=benchmarks,
                                     metrics=metrics)

                    metrics['parent_rows_updated'] = update_missing_parent_names(sess, updated_date=updated_date)
                    metrics['parent_update_date'] = str(updated_date)
                else:
                    logger.info("No daily file found.")
        sess.close()

    metrics['records_added'] = len(set(metrics['added_duns']))
    metrics['records_updated'] = len(set(metrics['updated_duns']) - set(metrics['added_duns']))
    del metrics['added_duns']
    del metrics['updated_duns']

    metrics['duration'] = str(datetime.datetime.now() - now)
    with open('load_duns_metrics.json', 'w+') as metrics_file:
        json.dump(metrics, metrics_file)
