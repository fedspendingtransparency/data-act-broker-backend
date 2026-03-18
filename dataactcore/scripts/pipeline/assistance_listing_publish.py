import boto3
from botocore.handlers import disable_signing
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import filecmp
import json
import argparse
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()


# Purpose of this script is to check the 'falextracts' AssistanceListing source against our current version, and update
# if necessary. The AssistanceListing source is updated on Saturdays using the ASSISTANCE_LISTING_FILE_FORMAT
# as the path (we do not have access to List anything in the bucket, so the script will break if the source file
# format changes.)

ASSISTANCE_LISTING_FILE_FORMAT = \
    'Assistance Listings/usaspendinggov/%Y/%m-%b/AssistanceListings_USASpendingGov_PUBLIC_WEEKLY_%Y%m%d.csv'


def get_parser():
    parser = argparse.ArgumentParser(description="Get NationalFedCodes file from USGS")
    parser.add_argument('--bucket', '-b', type=str, required=True, help='public bucket to download from/upload to')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    script_output = {}

    s3_reso = boto3.resource('s3', region_name='us-east-1')
    public_files_bucket_name = args.bucket
    fal_extracts_bucket_name = 'falextracts'

    # Ignore access keys...
    s3_reso.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
    falextracts_bucket = s3_reso.Bucket(fal_extracts_bucket_name)

    # Calculate object name to pull from falextracts
    today = datetime.now()

    if today.strftime("%A") == 'Saturday':
        # Check if today's file exists yet, if not, try last week's...
        al_s3_key = today.strftime(ASSISTANCE_LISTING_FILE_FORMAT)
        try:
            falextracts_bucket.Object(al_s3_key).load()
        except ClientError:
            logger.info("File at '{}'' not found. Trying last week's file...".format(al_s3_key))
            al_s3_key = (today - timedelta(days=7)).strftime(ASSISTANCE_LISTING_FILE_FORMAT)

    else:
        # Get "last Saturday" by modulo
        al_s3_key = (today - timedelta(days=((today.isoweekday() + 1) % 7))).strftime(ASSISTANCE_LISTING_FILE_FORMAT)

    logger.info("Attempting to download falextracts AssistanceListing file '{}'...".format(al_s3_key))
    falextracts_bucket.download_file(al_s3_key, 'falextracts_cfda.csv')
    script_output['source_file_path'] = al_s3_key
    script_output['source_file_date'] = str(falextracts_bucket.Object(al_s3_key).last_modified)

    logger.info("Downloading current USAspending-published AssistanceListing file...")
    usaspending_s3_reso = boto3.resource('s3', region_name='us-gov-west-1')
    usaspending_s3_reso.Bucket(public_files_bucket_name).\
        download_file('broker_reference_data/assistance_listing.csv', 'usaspending_assistance_listing.csv')

    script_output['usaspending_file_date'] = \
        str(usaspending_s3_reso.Bucket(public_files_bucket_name).Object('broker_reference_data/assistance_listing.csv').last_modified)

    if filecmp.cmp('falextracts_cfda.csv', 'usaspending_assistance_listing.csv'):
        logger.info('Current USAspending-published version matches falextracts version. No copy needed.')
        script_output['published_new_version'] = False
    else:
        logger.info('New file at {} is being uploaded to "da-public-files"...'.format(al_s3_key))
        usaspending_s3_reso.meta.client.upload_file('falextracts_cfda.csv', public_files_bucket_name,
                                                    'broker_reference_data/assistance_listing.csv')
        logger.info('AssistanceListing file uploaded successfully.')
        script_output['published_new_version'] = True

    with open("assistance_listing_publish_metrics.json", "w+") as json_out:
        logger.info('Writing to assistance_listing_publish_metrics.json')
        json.dump(script_output, json_out)
