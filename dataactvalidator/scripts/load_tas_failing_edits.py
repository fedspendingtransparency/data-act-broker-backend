from collections import namedtuple
from datetime import datetime
import glob
import logging
import os
import re
import json
import argparse

import boto3
import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import concat_tas_dict_vectorized, concat_display_tas_dict, TASFailedEdits
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def clean_failing_tas_data(filename):
    """ Clean the data coming in from the file

        Args:
            filename: name/path of the file to be read
    """

    data = pd.read_csv(filename, dtype=str)
    data = clean_data(
        data,
        TASFailedEdits,
        {"ata": "allocation_transfer_agency",
         "aid": "agency_identifier",
         "availability_type_code": "availability_type_code",
         "bpoa": "beginning_period_of_availa",
         "epoa": "ending_period_of_availabil",
         "main_account": "main_account_code",
         "sub_account": "sub_account_code",
         "fiscal_year": "fiscal_year",
         "period": "period",
         "fr_entity": "fr_entity_type",
         "fr_entity_title": "fr_entity_description",
         "edit_number": "edit_number",
         "edit_id": "edit_id",
         "fatal/proposed": "severity",
         "atb_submission_status": "atb_submission_status",
         "approved_edit_override_exists": "approved_override_exists"},
        {"allocation_transfer_agency": {"pad_to_length": 3, "keep_null": True},
         "agency_identifier": {"pad_to_length": 3},
         "availability_type_code": {"pad_to_length": 0, "keep_null": True},
         "main_account_code": {"pad_to_length": 4},
         "sub_account_code": {"pad_to_length": 3}}
    )

    return data


def get_tas_failing_edits_file_list(failed_tas_path, aws_prefix='GTAS_FE_DA'):
    """ Get the list of SF133 files to load

        Args:
            failed_tas_path: path to where TAS failing edits files are stored
            aws_prefix: prefix to filter which files to pull from AWS

        Returns:
            A list of tuples containing information about existing TAS failing edits files
    """
    FailedTASFile = namedtuple('FailedTAS', ['full_file', 'file'])
    failed_tas_list = []
    if failed_tas_path is not None:
        logger.info('Loading local TAS failing edits files')
        failed_tas_files = glob.glob(os.path.join(failed_tas_path, 'GTAS_FE_DA*.csv'))
        failed_tas_list = [FailedTASFile(failed_tas,
                                         os.path.basename(failed_tas)) for failed_tas in failed_tas_files]
    else:
        logger.info("Loading TAS failing edits")
        if CONFIG_BROKER['use_aws']:
            # get list of TAS failing edits files in the config bucket on S3
            s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            response = s3_client.list_objects_v2(Bucket=CONFIG_BROKER['sf_133_bucket'], Prefix=aws_prefix)
            for obj in response.get('Contents', []):
                file_url = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER['sf_133_bucket'],
                                                                           'Key': obj['Key']},
                                                            ExpiresIn=600)
                failed_tas_list.append(FailedTASFile(file_url, obj['Key']))

    return failed_tas_list


def load_tas_failing_edits_file(sess, filename, fiscal_year, fiscal_period, force_load=False, metrics=None):
    """ Load TAS failing edits lookup table.

        Args:
            sess: connection to database
            filename: name/path of the file to read in
            fiscal_year: fiscal year of the file being loaded
            fiscal_period: fiscal period of the file being loaded
            force_load: boolean to indicate whether to force a reload of the data
            metrics: an object containing information for the metrics file
    """
    if not metrics:
        metrics = {
            'records_inserted': 0,
            'records_deleted': 0
        }

    existing_records = sess.query(TASFailedEdits).filter(TASFailedEdits.fiscal_year == fiscal_year,
                                                         TASFailedEdits.period == fiscal_period)
    # If we've got records from this period, delete and insert new ones only if the load is being forced
    if existing_records.count() > 0:
        if force_load:
            logger.info('Force load: deleting existing records for %s %s', fiscal_year, fiscal_period)
            delete_count = existing_records.delete()
            metrics['records_deleted'] += delete_count
            logger.info('%s records deleted', delete_count)
        else:
            logger.info('Failed TAS %s %s already in database (%s records). Skipping file.', fiscal_year, fiscal_period,
                        existing_records.count())
            return

    logger.info('Loading records for %s %s', fiscal_year, fiscal_period)
    data = clean_failing_tas_data(filename)

    data['tas'] = concat_tas_dict_vectorized(data)
    data['display_tas'] = data.apply(lambda row: concat_display_tas_dict(row), axis=1)

    # insert to db
    table_name = TASFailedEdits.__table__.name
    num = insert_dataframe(data, table_name, sess.connection())
    sess.commit()

    metrics['records_inserted'] += num
    logger.info('%s records inserted', num)


def load_all_tas_failing_edits(failed_tas_path=None, force_load=False, aws_prefix='GTAS_FE_DA'):
    """ Load any TAS failing edits files that are new since the last load or all of them for local data.

        Args:
            failed_tas_path: path to the failed TAS files
            force_load: boolean to indicate whether to force a reload of the data
            aws_prefix: prefix to filter which files to pull from AWS
    """
    now = datetime.now()
    metrics_json = {
        'script_name': 'load_tas_failing_edits.py',
        'start_time': str(now),
        'records_deleted': 0,
        'records_inserted': 0
    }

    with create_app().app_context():
        sess = GlobalDB.db().session

        failed_tas_list = get_tas_failing_edits_file_list(failed_tas_path, aws_prefix)

        tas_re = re.compile(r'GTAS_FE_DA_(?P<year>\d{4})(?P<period>\d{2})\.csv')
        for failed_tas in failed_tas_list:
            file_match = tas_re.match(failed_tas.file)
            if not file_match:
                logger.info('Skipping TAS failing edits file with invalid name: %s', failed_tas.full_file)
                continue
            logger.info('Starting %s...', failed_tas.full_file)

            load_tas_failing_edits_file(sess, failed_tas.full_file, file_match.group('year'),
                                        file_match.group('period'), force_load=force_load, metrics=metrics_json)

        update_external_data_load_date(now, datetime.now(), 'failed_tas')

    metrics_json['duration'] = str(datetime.now() - now)

    with open('load_tas_failing_edits_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info("Script complete")


if __name__ == '__main__':
    configure_logging()
    parser = argparse.ArgumentParser(description='Initialize the DATA Act Broker.')
    parser.add_argument('-r', '--remote', help='Whether to run remote or not', action='store_true')
    parser.add_argument('-p', '--local_path', help='Local path of folder to check', type=str,
                        default=os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"), required=False)
    parser.add_argument('-pre', '--aws_prefix', help='When loading via AWS, this filters which files to run', type=str,
                        default="GTAS_FE_DA")
    parser.add_argument('-f', '--force', help='Forces actions to occur in certain scripts regardless of checks',
                        action='store_true')
    args = parser.parse_args()

    if not args.remote:
        load_all_tas_failing_edits(args.local_path, args.force, None)
    else:
        load_all_tas_failing_edits(None, args.force, args.aws_prefix)
