import logging

from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer


logger = logging.getLogger(__name__)


def write_csv(file_name, upload_name, is_local, header, body):
    """Derive the relevant location and write a CSV to it.
    :return: the final file name (complete with prefix)"""
    with get_write_csv_writer(file_name, upload_name, is_local, header) as writer:
        for line in body:
            writer.write(line)
        writer.finish_batch()


def get_write_csv_writer(file_name, upload_name, is_local, header):
    """Derive the relevant location.
    :return: the writer object"""
    if is_local:
        file_name = CONFIG_BROKER['broker_files'] + file_name
        csv_writer = CsvLocalWriter(file_name, header)
        message = 'Writing file locally...'
    else:
        bucket = CONFIG_BROKER['aws_bucket']
        region = CONFIG_BROKER['aws_region']
        csv_writer = CsvS3Writer(region, bucket, upload_name, header)
        message = 'Writing file to S3...'

    logger.debug(message)

    return csv_writer
