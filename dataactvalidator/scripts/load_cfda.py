import os
from datetime import datetime, timedelta
from io import BytesIO
from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging
import logging

import boto3
from botocore.handlers import disable_signing
from botocore.exceptions import ClientError

s3 = boto3.client('s3', )
logger = logging.getLogger(__name__)

CFDA_FILE_FORMAT = os.path.join('Assistance Listings', 'usaspendinggov', '%Y', '%m-%b',
                                'AssistanceListings_USASpendingGov_PUBLIC_WEEKLY_%Y%m%d.csv')
WEEKDAY_UPLOADED = 5  # datetime.weekday()'s integer representing the day it's usually uploaded (Saturday)
DAYS_TO_SEARCH = 4 * 7  # 4 weeks
LOCAL_CFDA_FILE = os.path.join('dataactvalidator', 'config', 'cfda_program.csv')


def find_latest_file(bucket, days_to_search=DAYS_TO_SEARCH):
    # TODO: If/When the bucket is public, simply use the folder structure to find the latest file instead of guessing
    # Check for the latest Saturday upload, otherwise manually look it up
    today = datetime.today()
    if today.weekday() == WEEKDAY_UPLOADED:
        logger.info('Checking today\'s entry')
        latest_file = today.strftime(CFDA_FILE_FORMAT)
        if file_exists(bucket, latest_file):
            return latest_file

    logger.info('Checking last week\'s entry')
    last_week = today - timedelta(7 - abs(today.weekday() - WEEKDAY_UPLOADED))
    latest_file = last_week.strftime(CFDA_FILE_FORMAT)
    if file_exists(bucket, latest_file):
        return latest_file
    else:
        logger.info('Looking within the past {} days'.format(days_to_search))
        try_date = today
        while days_to_search > 0:
            latest_file = try_date.strftime(CFDA_FILE_FORMAT)
            if not file_exists(bucket, latest_file):
                try_date = try_date - timedelta(1)
                days_to_search -= 1
            else:
                break
        if days_to_search == 0:
            logger.error('Could not find cfda file within the past {} days.'.format(days_to_search))
            return None
        return latest_file


def file_exists(bucket, src):
    try:
        bucket.Object(src).load()
        return True
    except ClientError:
        return False


def load_cfda():
    gsa_connection = boto3.resource('s3', region_name='us-east-1')
    # disregard aws credentials for public file
    gsa_connection.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    gsa_bucket = gsa_connection.Bucket('falextracts')

    latest_file = find_latest_file(gsa_bucket)
    if not latest_file:
        logger.error('Could not find cfda file')
        return

    logger.info('Loading ' + os.path.basename(latest_file))

    if CONFIG_BROKER["use_aws"]:
        # download file to memory, reupload
        data = BytesIO()
        gsa_bucket.download_fileobj(latest_file, data)
        data.seek(0)
        broker_s3 = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
        broker_s3.Bucket(CONFIG_BROKER['sf_133_bucket']).put_object(Key='cfda_program.csv', Body=data)
        logger.info('Loading file to S3 completed')
    else:
        # download file locally
        gsa_bucket.download_file(latest_file, LOCAL_CFDA_FILE)
        logger.info('Loading file completed')


if __name__ == '__main__':
    configure_logging()
    load_cfda()
