import boto3
import logging
import argparse

from dataactcore.config import CONFIG_BROKER
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)


def move_published_agency_files(old_code, new_code):
    """ Given the provided old and new agency codes, move the published files from the old agency directory to the new

        Args:
            old_code: The old agency code to copy from
            new_code: The new agency code to move to
    """
    if not old_code.endswith('/'):
        old_code += '/'
    if not new_code.endswith('/'):
        new_code += '/'

    # Note: the submissions bucket (aws_bucket) is not being used here as that path is based on submission ids

    # DABS directory structure
    # [certified bucket]/[agency code]/[fy]/[time period]/[publish history id]/[files]
    s3 = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])
    certified_bucket = s3.Bucket(CONFIG_BROKER['certified_bucket'])
    files_in_bucket = list(certified_bucket.objects.all())

    logger.info('Moving published DABS submission files from {} to {}'.format(old_code, new_code))
    old_file_paths = [old_file_path for old_file_path in files_in_bucket if old_file_path.startswith(old_code)]
    for old_file_path in old_file_paths:
        new_file_path = old_file_path.replace(old_code, new_code, 1)
        s3.Object(CONFIG_BROKER['certified_bucket'], new_file_path).copy_from(old_file_path)
    logger.info('Moved published DABS submission files from {} to {}'.format(old_code, new_code))

    # FABS directory structure
    # [certified bucket]/FABS/[agency code]/[fy]/[time period]/[files]
    logger.info('Moving published FABS submission files from {} to {}'.format(old_code, new_code))
    old_file_paths = [old_file_path for old_file_path in files_in_bucket
                      if old_file_path.startswith('FABS/{}'.format(old_code))]
    for old_file_path in old_file_paths:
        new_file_path = old_file_path.replace(old_code, new_code, 1)
        s3.Object(CONFIG_BROKER['certified_bucket'], new_file_path).copy_from(old_file_path)
    logger.info('Moved published FABS submission files from {} to {}'.format(old_code, new_code))


def main():
    """ Move all submission files in S3 for an agency that has changed its code """
    parser = argparse.ArgumentParser(description='Initialize the DATA Act Broker.')
    parser.add_argument('-o', '--old_code', help='The old agency code to copy from', required=True)
    parser.add_argument('-n', '--new_code', help='The new agency code to move to', required=True)
    args = parser.parse_args()

    logger.info('Moving published submission files')
    move_published_agency_files(args.old_code, args.new_code)
    logger.info('Finished moving published submission files')


if __name__ == '__main__':
    configure_logging()
    with create_app().app_context():
        main()