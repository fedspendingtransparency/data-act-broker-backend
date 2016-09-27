from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.cloudLogger import CloudLogger
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer


def write_csv(file_name, upload_name, is_local, header, body):
    """Derive the relevant location and write a CSV to it.
    :return: the final file name (complete with prefix)"""
    if is_local:
        file_name = CONFIG_BROKER['broker_files'] + file_name
        csv_writer = CsvLocalWriter(file_name, header)
        message = 'DEBUG: Writing file locally...'
    else:
        bucket = CONFIG_BROKER['aws_bucket']
        region = CONFIG_BROKER['aws_region']
        csv_writer = CsvS3Writer(region, bucket, upload_name, header)
        message = 'DEBUG: Writing file to S3...'

    CloudLogger.log(message, log_type="debug", file_name='smx_request.log')

    with csv_writer as writer:
        for line in body:
            writer.write(line)
        writer.finishBatch()
