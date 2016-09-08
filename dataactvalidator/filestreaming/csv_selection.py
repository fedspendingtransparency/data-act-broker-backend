from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer


def write_csv(file_name, is_local, header, body):
    """Derive the relevant location and write a CSV to it.
    :return: the final file name (complete with prefix)"""
    if is_local:
        file_name = CONFIG_BROKER['broker_files'] + file_name
        csv_writer = CsvLocalWriter(file_name, header)
    else:
        bucket = CONFIG_BROKER['aws_bucket']
        region = CONFIG_BROKER['aws_region']
        csv_writer = CsvS3Writer(region, bucket, file_name, header)

    with csv_writer as writer:
        for line in body:
            writer.write(line)
        writer.finishBatch()
