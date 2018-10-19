import csv
import logging
import os
import boto3

from dataactcore.config import CONFIG_BROKER

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024
QUERY_SIZE = 10000


def write_csv(file_name, upload_name, is_local, header, body):
    """ Write a CSV to the relevant location.

        Args:
            file_name: pathless file name
            upload_name: file name to be used as S3 key
            is_local: True if in local development, False otherwise
            header: value to write as the first line of the file
            body: Iterable to write as the body of the file
    """
    local_filename = CONFIG_BROKER['broker_files'] + file_name

    if is_local:
        logger.debug({
            'message': "Writing file locally...",
            'message_type': 'ValidatorDebug',
            'file_name': local_filename
        })

    with open(local_filename, 'w', newline='') as csv_file:
        # create local file and write headers
        out_csv = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        if header:
            out_csv.writerow(header)

        for line in body:
            out_csv.writerow(line)
    csv_file.close()

    if not is_local:
        # stream file to S3
        with open(local_filename, 'rb') as reader:
            stream_file_to_s3(upload_name, reader)
        # close and delete local copy
        reader.close()
        os.remove(local_filename)


def write_query_to_file(local_filename, upload_name, header, file_type, is_local, query_func, query_utils,
                        is_certified=False):
    """ Write file locally from a query, then stream it to S3

        Args:
            local_filename: full path for local file
            upload_name: file name to be used as S3 key
            header: value to write as the first line of the file
            file_type: Type of file (for logging purposes only)
            is_local: True if in local development, False otherwise
            query_func: function to call to query data
            query_utils: variables to pass to query function
            is_certified: True if writing to the certified bucket, False otherwise (default False)
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
            logger.debug({
                'message': 'Writing rows {}-{} to {} CSV'.format(page_start, page_start + len(rows), file_type),
                'message_type': 'ValidatorDebug',
                'file_type': file_type,
                'file_name': local_filename
            })
            out_csv.writerows(rows)
            if len(rows) < QUERY_SIZE:
                break
            page_idx += 1

    # close file
    csv_file.close()

    if not is_local:
        # stream file to S3
        with open(local_filename, 'rb') as reader:
            stream_file_to_s3(upload_name, reader, is_certified)
        # close and delete local copy
        reader.close()
        os.remove(local_filename)


def stream_file_to_s3(upload_name, reader, is_certified=False):
    """ Stream file to S3

        Args:
            upload_name: file name to be used as S3 key
            reader: reader object to read data from
            is_certified: True if writing to the certified bucket, False otherwise (default False)
    """
    path, file_name = upload_name.rsplit('/', 1)
    logger.debug({
        'message': 'Streaming file to S3',
        'message_type': 'ValidatorDebug',
        'file_name': file_name if file_name else path
    })

    s3_resource = boto3.resource('s3', region_name=CONFIG_BROKER['aws_region'])

    if is_certified:
        bucket_name = CONFIG_BROKER["certified_bucket"]
    else:
        bucket_name = CONFIG_BROKER["aws_bucket"]

    s3_resource.Object(bucket_name, upload_name).put(Body=reader)
