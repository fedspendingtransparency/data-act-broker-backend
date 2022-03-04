import argparse
import datetime
import logging
import os
import re
import json
import tempfile
import boto3
import requests

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import DUNS
from dataactcore.utils.duns import (parse_duns_file, update_duns, parse_exec_comp_file, update_missing_parent_names,
                                    request_sam_csv_api, is_nonexistent_file_error)
from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)

SAM_FILE_FORMAT = 'SAM_{data_type}_UTF-8_{period}{version}_%Y%m%d.ZIP'
DATA_TYPES = {
    'DUNS': 'FOUO',
    'Executive Compensation': 'EXECCOMP'
}
PERIODS = ['MONTHLY', 'DAILY']
VERSIONS = {
    'v1': '',  # V1 files simply exclude the version
    'v2': '_V2'
}
S3_ARCHIVE = CONFIG_BROKER['sam']['duns']['csv_archive_bucket']
S3_ARCHIVE_PATH = '{data_type}/{version}/{file_name}'


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

    # Figure out what files we have available based on our local or remote setup
    if local:
        local_files = os.listdir(local)
        monthly_v1_files = sorted([monthly_file for monthly_file in local_files
                                   if re.match('SAM_{}_UTF-8_MONTHLY_\d+\.ZIP'.format(DATA_TYPES[data_type]),
                                               monthly_file.upper())])
        monthly_v2_files = sorted([monthly_file for monthly_file in local_files
                                   if re.match('SAM_{}_UTF-8_MONTHLY_V2_\d+\.ZIP'.format(DATA_TYPES[data_type]),
                                               monthly_file.upper())])
        daily_v1_files = sorted([daily_file for daily_file in local_files
                                 if re.match('SAM_{}_UTF-8_DAILY_\d+\.ZIP'.format(DATA_TYPES[data_type]),
                                             daily_file.upper())])
        daily_v2_files = sorted([daily_file for daily_file in local_files
                                 if re.match('SAM_{}_UTF-8_DAILY_V2_\d+\.ZIP'.format(DATA_TYPES[data_type]),
                                             daily_file.upper())])
    else:
        # TODO: the SAM API currently doesn't list available files and doesnt include historic ones,
        #       so we're pulling files from the CSV_ARCHIVE_BUCKET bucket up and then use the API.
        #       Rework this if SAM includes these historic files in the API and list what files are available
        monthly_v1_files = list_s3_archive_files(data_type, 'MONTHLY', 'v1')
        monthly_v2_files = list_s3_archive_files(data_type, 'MONTHLY', 'v2')
        daily_v1_files = list_s3_archive_files(data_type, 'DAILY', 'v1')
        daily_v2_files = list_s3_archive_files(data_type, 'DAILY', 'v2')

    # Extracting the dates from these to figure out which files to process where
    # For both monthly and daily files, we only want to process v1 files until the equivalent v2 files are available
    monthly_v1_dates = extract_dates_from_list(monthly_v1_files, data_type, 'MONTHLY', 'v1')
    monthly_v2_dates = extract_dates_from_list(monthly_v2_files, data_type, 'MONTHLY', 'v2')
    monthly_v1_dates = [monthly_v1_date for monthly_v1_date in monthly_v1_dates
                        if monthly_v1_date not in monthly_v2_dates]
    if historic:
        earliest_date = sorted(monthly_v1_dates + monthly_v2_dates)[0]

    daily_v1_dates = extract_dates_from_list(daily_v1_files, data_type, 'DAILY', 'v1')
    daily_v2_dates = extract_dates_from_list(daily_v2_files, data_type, 'DAILY', 'v2')
    daily_v1_dates = [daily_v1_dates for daily_v1_dates in daily_v1_dates
                      if daily_v1_dates not in daily_v2_dates]
    latest_date = sorted(daily_v1_dates + daily_v2_dates)[-1]

    # For any dates after the latest date we have in the archive, use the API
    daily_v2_api_dates = [latest_date + datetime.timedelta(days=i)
                          for i in range(1, (datetime.date.today() - latest_date).days + 1)]

    # determine which daily files to load in by setting the start load date
    if historic:
        load_date = earliest_date
    elif reload_date:
        # a bit redundant but also date validation
        load_date = datetime.datetime.strptime(reload_date, '%Y-%m-%d').date()
    else:
        sam_field = DUNS.last_sam_mod_date if data_type == 'DUNS' else DUNS.last_exec_comp_mod_date
        load_date = sess.query(sam_field).filter(sam_field.isnot(None)).order_by(sam_field.desc()).first()
        if not load_date:
            field = 'sam' if data_type == 'duns' else 'executive compenstation'
            raise Exception('No last {} mod date found in DUNS table. Please run historic loader first.'.format(field))
        load_date = load_date[0]

    # only load in the daily files after the load date
    daily_v1_dates = list(filter(lambda daily_date: daily_date >= load_date, daily_v1_dates))
    daily_v2_dates = list(filter(lambda daily_date: daily_date >= load_date, daily_v2_dates))
    daily_v2_api_dates = list(filter(lambda daily_date: daily_date >= load_date, daily_v2_api_dates))

    if historic:
        # load in the earliest monthly file and all daily files after
        version = 'v1' if earliest_date in monthly_v1_dates else 'v2'
        process_sam_file(data_type, 'MONTHLY', version, earliest_date, sess, local=local, metrics=metrics)
    for daily_v1_date in daily_v1_dates:
        process_sam_file(data_type, 'DAILY', 'v1', daily_v1_date, sess, local=local, metrics=metrics)
    for daily_v2_date in daily_v2_dates:
        process_sam_file(data_type, 'DAILY', 'v2', daily_v2_date, sess, local=local, metrics=metrics)
    if not local:
        for daily_api_v2_date in daily_v2_api_dates:
            try:
                process_sam_file(data_type, 'DAILY', 'v2', daily_api_v2_date, sess, local=local, api=True,
                                 metrics=metrics)
            except requests.exceptions.HTTPError as e:
                if is_nonexistent_file_error(e):
                    logger.warning('No file found for {}, continuing'.format(daily_api_v2_date))
                    continue
                else:
                    logger.exception(e.response.content.decode('utf-8'))
                    raise e

    if data_type == 'DUNS':
        updated_date = datetime.date.today()
        metrics['parent_rows_updated'] = update_missing_parent_names(sess, updated_date=updated_date)
        metrics['parent_update_date'] = str(updated_date)

        if historic:
            logger.info('Despite the historical load being done, the UEI will most likely be out of date. '
                        'Please manually update using the UEI crosswalk file and SQL.')


def extract_dates_from_list(sam_files, data_type, period, version):
    """ Given a list of SAM files, extract the dates the files refer to

        Args:
            sam_files: list of sam file names to extract dates from
            data_type: data type to load (DUNS or executive compensation)
            period: monthly or daily
            version: v1 or v2

        Returns:
            sorted list of dates corresponding to the files
    """
    sam_filename_format = SAM_FILE_FORMAT.format(data_type=DATA_TYPES[data_type], period=period,
                                                 version=VERSIONS[version])
    return sorted([datetime.datetime.strptime(sam_file, sam_filename_format).date() for sam_file in sam_files])


def list_s3_archive_files(data_type, period, version):
    """ Given the requested fields, provide a list of available files from the remote S3 archive

        Args:
            data_type: data type to load (DUNS or executive compensation)
            period: monthly or daily
            version: v1 or v2

        Returns:
            list of available files in the S3 archive
    """
    s3_resource = boto3.resource('s3', region_name='us-gov-west-1')
    archive_bucket = s3_resource.Bucket(S3_ARCHIVE)
    file_name = SAM_FILE_FORMAT[:30].format(data_type=DATA_TYPES[data_type], period=period)
    prefix = S3_ARCHIVE_PATH.format(data_type=data_type, version=version, file_name=file_name)
    return [os.path.basename(object.key) for object in archive_bucket.objects.filter(Prefix=prefix)]


def download_sam_file(root_dir, file_name, api=False):
    """ Downloads the requested DUNS file to root_dir

        Args:
            root_dir: the folder containing the DUNS file
            file_name: the name of the SAM file
            api: whether to use the SAM CSV API or not

        Raises:
            requests.exceptions.HTTPError if the SAM HTTP API doesnt have the file requested
    """
    logger.info('Pulling {} via {}'.format(file_name, 'API' if api else 'archive'))
    if api:
        request_sam_csv_api(root_dir, file_name)
    else:
        s3_client = boto3.client('s3', region_name='us-gov-west-1')
        reverse_map = {v: k for k, v in DATA_TYPES.items()}
        data_type = reverse_map[file_name.split('_')[1]]
        version = 'v2' if 'V2' in file_name else 'v1'
        key = S3_ARCHIVE_PATH.format(data_type=data_type, version=version, file_name=file_name)
        s3_client.download_file(S3_ARCHIVE, key, os.path.join(root_dir, file_name))


def process_sam_file(data_type, period, version, date, sess, local=None, api=False, metrics=None):
    """ Process the SAM file found locally or remotely

        Args:
            data_type: data type to load (DUNS or executive compensation)
            period: monthly or daily
            version: v1 or v2
            sess: the database connection
            local: path to local directory to process, if None, it will go though the remote SAM service
            api: whether to use the SAM CSV API or not
            metrics: dictionary representing metrics data for the load

        Raises:
            requests.exceptions.HTTPError if the SAM HTTP API doesnt have the file requested
    """
    if not metrics:
        metrics = {}

    root_dir = local if local else tempfile.gettempdir()
    file_name_format = SAM_FILE_FORMAT.format(data_type=DATA_TYPES[data_type], period=period, version=VERSIONS[version])
    file_name = date.strftime(file_name_format)
    if not local:
        download_sam_file(root_dir, file_name, api=api)

    file_path = os.path.join(root_dir, file_name)
    key_cols = ['awardee_or_recipient_uniqu'] if version == 'v1' else ['awardee_or_recipient_uniqu', 'uei']
    if data_type == 'DUNS':
        add_update_data, delete_data = parse_duns_file(file_path, metrics=metrics)
        if add_update_data is not None:
            update_duns(sess, add_update_data, metrics=metrics, key_cols=key_cols)
        if delete_data is not None:
            update_duns(sess, delete_data, metrics=metrics, deletes=True, key_cols=key_cols)
    else:
        exec_comp_data = parse_exec_comp_file(file_path, metrics=metrics)
        update_duns(sess, exec_comp_data, metrics=metrics, key_cols=key_cols)
    if not local:
        os.remove(file_path)


if __name__ == '__main__':
    now = datetime.datetime.now()

    configure_logging()

    parser = argparse.ArgumentParser(description='Get data from SAM and update duns/exec comp tables')
    parser.add_argument("-t", "--data_type", choices=['duns', 'exec_comp', 'both'], default='both',
                        help='Select data type to load')
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("-a", "--historic", action="store_true", help='Reload from the first monthly file on')
    scope.add_argument("-u", "--update", action="store_true", help='Load daily files since latest last_sam_mod_date')
    environ = parser.add_mutually_exclusive_group(required=True)
    environ.add_argument("-l", "--local", type=str, default=None, help='Local directory to work from')
    environ.add_argument("-r", "--remote", action="store_true", help='Work from a remote directory (SAM)')
    parser.add_argument("-f", "--reload_date", type=str, default=None, help='Force update from a specific date'
                                                                            ' (YYYY-MM-DD)')
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
            start_time = datetime.datetime.now()
            load_from_sam('DUNS', sess, historic, local, metrics=metrics, reload_date=reload_date)
            update_external_data_load_date(start_time, datetime.datetime.now(), 'recipient')
        if data_type in ('exec_comp', 'both'):
            start_time = datetime.datetime.now()
            load_from_sam('Executive Compensation', sess, historic, local, metrics=metrics, reload_date=reload_date)
            update_external_data_load_date(start_time, datetime.datetime.now(), 'executive_compensation')
        sess.close()

    metrics['records_added'] = len(set(metrics['added_duns']))
    metrics['records_updated'] = len(set(metrics['updated_duns']) - set(metrics['added_duns']))
    del metrics['added_duns']
    del metrics['updated_duns']

    logger.info('Added {} records and updated {} records'.format(metrics['records_added'], metrics['records_updated']))

    metrics['duration'] = str(datetime.datetime.now() - now)
    with open('load_duns_exec_comp_metrics.json', 'w+') as metrics_file:
        json.dump(metrics, metrics_file)
