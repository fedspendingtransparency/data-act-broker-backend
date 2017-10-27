import csv
import logging
import os
import smart_open

from dataactcore.aws.s3Handler import S3Handler
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.csvLocalWriter import CsvLocalWriter
from dataactvalidator.filestreaming.csvS3Writer import CsvS3Writer

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024
QUERY_SIZE = 10000


def write_csv(file_name, upload_name, is_local, header, body):
    """Write a CSV to the relevant location.

        Args:
            file_name - pathless file name
            upload_name - file name to be used as S3 key
            is_local - True if in local development, False otherwise
            header - value to write as the first line of the file
            body - Iterable to write as the body of the file
        
        Return:
            the final file name (complete with prefix)
    """
    with get_write_csv_writer(file_name, upload_name, is_local, header) as writer:
        for line in body:
            writer.write(line)
        writer.finish_batch()


def get_write_csv_writer(file_name, upload_name, is_local, header):
    """Derive the relevant location.

        Args:
            file_name - pathless file name
            upload_name - file name to be used as S3 key
            is_local - True if in local development, False otherwise
            header - value to write as the first line of the file
        
        Return:
            the writer object
    """
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


def write_query_to_file(local_filename, upload_name, header, file_type, is_local, query_func, query_utils,
                        is_certified=False):
    """Write file locally from a query, then stream it to S3

        Args:
            local_filename - full path for local file
            upload_name - file name to be used as S3 key
            header - value to write as the first line of the file
            file_type - Type of file (for logging purposes only)
            is_local - True if in local development, False otherwise
            query_func - function to call to query data
            query_utils - variables to pass to query function
            is_certified - True if writing to the certified bucket, False otherwise (default False)
    """
    # create file locally
    with open(local_filename, 'w', newline='') as csv_file:
        # create local file and write headers
        out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        if header:
            out_csv.writerow(header)

        page_idx = 0
        while True:
            # query QUERY_SIZE number of rows
            page_start = QUERY_SIZE * page_idx
            rows = query_func(query_utils, page_start, (QUERY_SIZE * (page_idx + 1)))

            if rows is None:
                break

            # write records to file
            logger.debug('Writing rows {}-{} to {} CSV'.format(page_start, page_start + len(rows), file_type))
            out_csv.writerows(rows)
            if len(rows) < QUERY_SIZE:
                break
            page_idx += 1

    # close file
    csv_file.close()

    if not is_local:
        # stream file to S3
        with open(local_filename, 'rb') as reader:
            write_file_to_s3(upload_name, reader, is_certified)
        # close and delete local copy
        reader.close()
        os.remove(local_filename)


def write_file_to_s3(upload_name, reader, is_certified=False):
    """Stream file to S3

        Args:
            upload_name - file name to be used as S3 key
            reader - reader object to read data from
            is_certified - True if writing to the certified bucket, False otherwise (default False)
    """
    with smart_open.smart_open(S3Handler.create_file_path(upload_name, is_certified), 'w') as writer:
        while True:
            chunk = reader.read(CHUNK_SIZE)
            if chunk:
                writer.write(chunk)
            else:
                break
