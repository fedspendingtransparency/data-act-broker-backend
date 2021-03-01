import argparse
import datetime
import logging
import os
import re
import json
import tempfile
import requests

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import DUNS
from dataactcore.utils.parentDuns import update_missing_parent_names
from dataactcore.utils.duns import parse_duns_file, update_duns
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

API_URL = CONFIG_BROKER['sam']['duns']['api_url'].format(CONFIG_BROKER['sam']['api_key'])
MONTHLY_DUNS_FORMAT = 'SAM_FOUO_UTF-8_MONTHLY_V2_%Y%m%d.ZIP'
DAILY_DUNS_FORMAT = 'SAM_FOUO_UTF-8_DAILY_V2_%Y%m%d.ZIP'
FIRST_MONTHLY = datetime.date(year=2021, month=2, day=1)


def load_duns(sess, historic, local=None, benchmarks=None, metrics=None, force_reload=None):
    """ Process the script arguments to figure out which files to process in which order

        Args:
            sess: the database connection
            historic: whether to load in monthly file and daily files after, or just the latest daily files
            local: path to local directory to process, if None, it will go though the remote SAM service
            benchmarks: whether to log times
            metrics: dictionary representing metrics data for the load
            force_reload: specific date to force reload from
    """
    if not metrics:
        metrics = {}

    updated_date = datetime.date.today()

    # Figure out what files we have available based on our local or remote setup
    if local:
        local_files = os.listdir(local)
        monthly_files = sorted([monthly_file for monthly_file in local_files
                                if re.match(".*MONTHLY_V2_\d+\.ZIP", monthly_file.upper())])
        daily_files = sorted([daily_file for daily_file in local_files
                              if re.match(".*DAILY_V2_\d+\.ZIP", daily_file.upper())])
    else:
        # TODO: SAM currently doesn't have an easy way to detect which files are available or when it starts,
        #       so for now we're trying all options. Rework this part if/when SAM provides a list of available files.
        monthly_files = [FIRST_MONTHLY.strftime(MONTHLY_DUNS_FORMAT)]
        days_to_load = [FIRST_MONTHLY + datetime.timedelta(days=i) for i in
                        range((datetime.date.today() - FIRST_MONTHLY).days + 1)]
        daily_files = [day.strftime(DAILY_DUNS_FORMAT) for day in days_to_load]

    # load in earliest monthly file for historic
    if historic:
        process_duns_file(monthly_files[0], sess, local=local, monthly=True, benchmarks=benchmarks, metrics=metrics)

    # determine which daily files to load in by setting the start load date
    if historic:
        load_date = datetime.datetime.strptime(monthly_files[0], MONTHLY_DUNS_FORMAT)
    elif force_reload:
        # a bit redundant but also date validation
        load_date = datetime.datetime.strptime(force_reload, '%Y-%m-%d')
    else:
        load_date = sess.query(DUNS.last_sam_mod_date). \
            order_by(DUNS.last_sam_mod_date.desc()). \
            filter(DUNS.last_sam_mod_date.isnot(None)). \
            first()
        load_date = load_date[0]
        if not load_date:
            raise Exception('No last sam mod date found in DUNS table. Please run historic loader first.')

    # load daily files starting from the load_date
    for daily_file in filter(lambda daily_file: daily_file >= load_date.strftime(DAILY_DUNS_FORMAT), daily_files):
        try:
            process_duns_file(daily_file, sess, local=local, benchmarks=benchmarks, metrics=metrics)
        except FileNotFoundError:
            logger.warning('No file found for {}, continuing'.format(daily_file))
            continue

    metrics['parent_rows_updated'] = update_missing_parent_names(sess, updated_date=updated_date)
    metrics['parent_update_date'] = str(updated_date)


def download_duns(root_dir, file_name):
    """ Downloads the requested DUNS file to root_dir

        Args:
            root_dir: the folder containing the DUNS file
            file_name: the name of the SAM file

        Raises:
            FileNotFoundError if the SAM HTTP API doesnt have the file requested
    """
    logger.info('Pulling {}'.format(file_name))
    url_with_params = '{}&fileName={}'.format(API_URL, file_name)
    r = requests.get(url_with_params)
    if r.status_code == 200:
        duns_file = os.path.join(root_dir, file_name)
        open(duns_file, 'wb').write(r.content)
    elif r.status_code == 400:
        raise FileNotFoundError('File not found on SAM HTTP API.')


def process_duns_file(file_name, sess, local=None, monthly=False, benchmarks=False, metrics=None):
    """ Process the SAM file found locally or remotely

        Args:
            file_name: the name of the SAM file
            sess: the database connection
            local: path to local directory to process, if None, it will go though the remote SAM service
            monthly: whether it's a monthly file
            benchmarks: whether to log times
            metrics: dictionary representing metrics data for the load

        Raises:
            FileNotFoundError if the SAM HTTP API doesnt have the file requested
    """
    if not metrics:
        metrics = {}

    root_dir = local if local else tempfile.gettempdir()
    if not local:
        try:
            download_duns(root_dir, file_name)
        except FileNotFoundError as e:
            raise e

    file_path = os.path.join(root_dir, file_name)

    add_update_data, delete_data = parse_duns_file(file_path, sess, monthly=monthly, benchmarks=benchmarks,
                                                   metrics=metrics)
    if add_update_data is not None:
        update_duns(sess, add_update_data, metrics=metrics)
    if delete_data is not None:
        update_duns(sess, delete_data, metrics=metrics, deletes=True)
    if not local:
        os.remove(file_path)


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
    parser = argparse.ArgumentParser(description='Get data from SAM and update duns table')
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("-a", "--historic", action="store_true", help='Reload from the first monthly file on')
    scope.add_argument("-u", "--update", action="store_true", help='Load daily files since latest last_sam_mod_date')
    environ = parser.add_mutually_exclusive_group(required=True)
    environ.add_argument("-l", "--local", type=str, default=None, help='Local directory to work from')
    environ.add_argument("-r", "--remote", action="store_true", help='Work from a remote directory (SAM)')
    parser.add_argument("-f", "--force_reload", type=str, default=None, help='Force update from a specific date'
                                                                             ' (YYYY-MM-DD)')
    parser.add_argument("-b", "--benchmarks", action="store_true", help='log times of operations for testing')
    return parser


if __name__ == '__main__':
    now = datetime.datetime.now()

    configure_logging()
    parser = get_parser()
    args = parser.parse_args()

    historic = args.historic
    update = args.update
    local = args.local
    remote = args.remote
    benchmarks = args.benchmarks
    force_reload = args.force_reload

    metrics = {
        'script_name': 'load_duns.py',
        'start_time': str(now),
        'files_processed': [],
        'records_received': 0,
        'records_processed': 0,
        'adds_received': 0,
        'updates_received': 0,
        'deletes_received': 0,
        'added_duns': [],
        'updated_duns': [],
        'records_added': 0,
        'records_updated': 0,
        'parent_rows_updated': 0,
        'parent_update_date': None
    }

    with create_app().app_context():
        sess = GlobalDB.db().session
        load_duns(sess, historic, local, benchmarks=benchmarks, metrics=metrics, force_reload=force_reload)
        sess.close()

    metrics['records_added'] = len(set(metrics['added_duns']))
    metrics['records_updated'] = len(set(metrics['updated_duns']) - set(metrics['added_duns']))
    del metrics['added_duns']
    del metrics['updated_duns']

    logger.info('Added {} records and updated {} records'.format(metrics['records_added'], metrics['records_updated']))

    metrics['duration'] = str(datetime.datetime.now() - now)
    with open('load_duns_metrics.json', 'w+') as metrics_file:
        json.dump(metrics, metrics_file)
