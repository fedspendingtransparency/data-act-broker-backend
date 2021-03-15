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
from dataactcore.utils.duns import parse_duns_file, update_duns, parse_exec_comp_file, update_exec_comp_duns
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

API_URL = CONFIG_BROKER['sam']['duns']['api_url'].format(CONFIG_BROKER['sam']['api_key'])
MONTHLY_FILE_FORMAT = 'SAM_{}_UTF-8_MONTHLY_V2_%Y%m%d.ZIP'
DAILY_FILE_FORMAT = 'SAM_{}_UTF-8_DAILY_V2_%Y%m%d.ZIP'
DATA_TYPE_FIELD = {'duns': 'FOUO', 'exec_comp': 'EXECCOMP'}
FIRST_MONTHLY = datetime.date(year=2021, month=2, day=1)


def load_from_sam(data_type, sess, historic, local=None, metrics=None, reload_date=None):
    """ Process the script arguments to figure out which files to process in which order

        Args:
            data_type: data type to load (DUNS or executive compensation)
            sess: the database connection
            historic: whether to load in monthly file and daily files after, or just the latest daily files
            local: path to local directory to process, if None, it will go though the remote SAM service
            metrics: dictionary representing metrics data for the load
            reload_date: specific date to force reload from
    """
    if not metrics:
        metrics = {}

    monthy_format = MONTHLY_FILE_FORMAT.format(DATA_TYPE_FIELD[data_type])
    daily_format = DAILY_FILE_FORMAT.format(DATA_TYPE_FIELD[data_type])

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
        monthly_files = [FIRST_MONTHLY.strftime(monthy_format)]
        days_to_load = [FIRST_MONTHLY + datetime.timedelta(days=i) for i in
                        range((datetime.date.today() - FIRST_MONTHLY).days + 1)]
        daily_files = [day.strftime(daily_format) for day in days_to_load]

    # load in earliest monthly file for historic
    if historic:
        process_sam_file(data_type, monthly_files[0], sess, local=local, monthly=True, metrics=metrics)

    # determine which daily files to load in by setting the start load date
    if historic:
        load_date = datetime.datetime.strptime(monthly_files[0], monthy_format)
    elif reload_date:
        # a bit redundant but also date validation
        load_date = datetime.datetime.strptime(reload_date, '%Y-%m-%d')
    else:
        sam_field = DUNS.last_sam_mod_date if data_type == 'duns' else DUNS.last_exec_comp_mod_date
        load_date = sess.query(sam_field).filter(sam_field.isnot(None)).order_by(sam_field.desc()).first()
        load_date = load_date[0]
        if not load_date:
            field = 'sam' if data_type == 'duns' else 'executive compenstation'
            raise Exception('No last {} mod date found in DUNS table. Please run historic loader first.'.format(field))

    # load daily files starting from the load_date
    for daily_file in filter(lambda daily_file: daily_file >= load_date.strftime(daily_format), daily_files):
        try:
            process_sam_file(data_type, daily_file, sess, local=local, metrics=metrics)
        except FileNotFoundError:
            logger.warning('No file found for {}, continuing'.format(daily_file))
            continue

    if data_type == 'duns':
        updated_date = datetime.date.today()
        metrics['parent_rows_updated'] = update_missing_parent_names(sess, updated_date=updated_date)
        metrics['parent_update_date'] = str(updated_date)


def download_sam_file(root_dir, file_name):
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


def process_sam_file(data_type, file_name, sess, local=None, monthly=False, metrics=None):
    """ Process the SAM file found locally or remotely

        Args:
            data_type: data type to load (DUNS or executive compensation)
            file_name: the name of the SAM file
            sess: the database connection
            local: path to local directory to process, if None, it will go though the remote SAM service
            monthly: whether it's a monthly file
            metrics: dictionary representing metrics data for the load

        Raises:
            FileNotFoundError if the SAM HTTP API doesnt have the file requested
    """
    if not metrics:
        metrics = {}

    root_dir = local if local else tempfile.gettempdir()
    if not local:
        try:
            download_sam_file(root_dir, file_name)
        except FileNotFoundError as e:
            raise e

    file_path = os.path.join(root_dir, file_name)

    if data_type == 'duns':
        add_update_data, delete_data = parse_duns_file(file_path, monthly=monthly, metrics=metrics)
        if add_update_data is not None:
            update_duns(sess, add_update_data, metrics=metrics)
        if delete_data is not None:
            update_duns(sess, delete_data, metrics=metrics, deletes=True)
    else:
        exec_comp_data = parse_exec_comp_file(file_path, monthly=monthly, metrics=metrics)
        update_exec_comp_duns(sess, exec_comp_data, metrics=metrics)
    if not local:
        os.remove(file_path)


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
    parser = argparse.ArgumentParser(description='Get data from SAM and update duns/exec comp tables')
    parser.add_argument("-t", "--data_type", choices=['duns', 'exec_comp', 'both'], help='Select data type to load')
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("-a", "--historic", action="store_true", help='Reload from the first monthly file on')
    scope.add_argument("-u", "--update", action="store_true", help='Load daily files since latest last_sam_mod_date')
    environ = parser.add_mutually_exclusive_group(required=True)
    environ.add_argument("-l", "--local", type=str, default=None, help='Local directory to work from')
    environ.add_argument("-r", "--remote", action="store_true", help='Work from a remote directory (SAM)')
    parser.add_argument("-f", "--reload_date", type=str, default=None, help='Force update from a specific date'
                                                                            ' (YYYY-MM-DD)')
    return parser


if __name__ == '__main__':
    now = datetime.datetime.now()

    configure_logging()
    parser = get_parser()
    args = parser.parse_args()

    data_type = args.data_type
    historic = args.historic
    update = args.update
    local = args.local
    reload_date = args.reload_date

    metrics = {
        'script_name': 'load_duns_exec_comp.py',
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
        if data_type in ('duns', 'both'):
            load_from_sam('duns', sess, historic, local, metrics=metrics, reload_date=reload_date)
        if data_type in ('exec_comp', 'both'):
            load_from_sam('exec_comp', sess, historic, local, metrics=metrics, reload_date=reload_date)
        sess.close()

    metrics['records_added'] = len(set(metrics['added_duns']))
    metrics['records_updated'] = len(set(metrics['updated_duns']) - set(metrics['added_duns']))
    del metrics['added_duns']
    del metrics['updated_duns']

    logger.info('Added {} records and updated {} records'.format(metrics['records_added'], metrics['records_updated']))

    metrics['duration'] = str(datetime.datetime.now() - now)
    with open('load_duns_exec_comp_metrics.json', 'w+') as metrics_file:
        json.dump(metrics, metrics_file)
