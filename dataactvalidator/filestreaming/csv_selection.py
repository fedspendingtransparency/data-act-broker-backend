import csv
import logging
import os
import boto3
import tempfile
import subprocess
import time

from dataactcore.config import CONFIG_BROKER
from dataactbroker.helpers.generic_helper import generate_raw_quoted_query

logger = logging.getLogger(__name__)


def write_stream_query(sess, query, local_filename, upload_name, is_local, header=None, generate_headers=False,
                       generate_string=True, is_certified=False, file_format='csv', bucket=None, set_region=True):
    """ Write file locally from a query, then stream it to S3

        Args:
            sess: the database connection
            query: the query object or string
            local_filename: full path for local file
            upload_name: file name to be used as S3 key
            is_local: True if in local development, False otherwise
            header: value to write as the first line of the file if provided (default None)
            generate_headers: whether to generate headers based on the query (default False)
            generate_string: whether to extract the raw query from the queryset (default True)
            is_certified: True if writing to the certified bucket, False otherwise (default False)
            file_format: determines if the file generated is a txt or a csv (default csv)
            bucket: A specific bucket name to overwrite the general use cases with
            set_region: Whether to set the region or not, usually required but needs no region for some scripts
                (default True)
    """
    if is_local:
        local_filename = upload_name

    logger.debug({
        'message': 'Writing query to {}'.format(file_format),
        'message_type': 'BrokerDebug',
        'upload_name': upload_name
    })

    write_query_to_file(sess, query, local_filename, header=header, generate_headers=generate_headers,
                        generate_string=generate_string, file_format=file_format)

    logger.debug({
        'message': '{} written from query'.format(file_format.upper()),
        'message_type': 'BrokerDebug',
        'upload_name': upload_name
    })

    if not is_local:
        upload_file_to_s3(upload_name, local_filename, is_certified=is_certified, bucket=bucket, set_region=set_region)
        os.remove(local_filename)


def write_query_to_file(sess, query, local_filename, header=None, generate_headers=False, generate_string=True,
                        file_format='csv'):
    """ Write file locally from a query

        Args:
            sess: database connection
            query: query to spit out data
            local_filename: full path for local file
            header: value to write as the first line of the file if provided
            generate_headers: whether to generate headers based on the query
            generate_string: whether to extract the raw query from the queryset
            file_format: determines if the file generated is a txt or a csv
    """

    delimiter = ','
    if file_format == 'txt':
        delimiter = '|'

    # write base csv with headers
    # Note: while psql's copy command supports headers, some header lengths exceed the maximum label length (63)
    if header:
        with open(local_filename, 'w', newline='') as csv_file:
            # create local file and write headers
            out_csv = csv.writer(csv_file, delimiter=delimiter, quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            out_csv.writerow(header)

    # get the raw SQL equivalent
    if generate_string:
        raw_query = generate_raw_quoted_query(query)
    else:
        raw_query = query

    # save psql command with query to a temp file
    # note: if we've been provded a header and wrote it, there's no need to add another
    temp_sql_file, temp_sql_file_path = generate_temp_query_file(raw_query, header=generate_headers,
                                                                 delimiter=delimiter)

    # run the psql command and cleanup
    database_string = str(sess.bind.url)
    execute_psql(temp_sql_file_path, local_filename, database_string)
    os.remove(temp_sql_file_path)


def upload_file_to_s3(upload_name, local_file, is_certified=False, bucket=None, set_region=True):
    """ Upload file to S3

        Args:
            upload_name: file name to be used as S3 key
            local_file: full path of local file
            is_certified: True if writing to the certified bucket, False otherwise (default False)
            bucket: A specific bucket name to overwrite the general use cases with
            set_region: Whether to set the region or not, usually required but needs no region for some scripts
                (default True)
    """
    path, file_name = upload_name.rsplit('/', 1) if '/' in upload_name else (None, upload_name)
    logger.debug({
        'message': 'Uploading file to S3',
        'message_type': 'ValidatorDebug',
        'file_name': file_name if file_name else path
    })

    if set_region:
        s3 = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
    else:
        s3 = boto3.client('s3')

    if bucket:
        bucket_name = bucket
    elif is_certified:
        bucket_name = CONFIG_BROKER["certified_bucket"]
    else:
        bucket_name = CONFIG_BROKER["aws_bucket"]

    s3.upload_file(local_file, bucket_name, upload_name)


def generate_temp_query_file(query, header=True, delimiter=','):
    """ Generates a temporary file containing the shell command to create the csv
        Reason for this being that the query can be too long for shell to process via subprocess

        Args:
            query: string query to populate the csv file
            header: include header in csv (includes column names or aliases if provided in query)
            delimiter: determines if the file generated will be comma or pipe delimited
    """
    logger.debug('Creating PSQL Query: {}'.format(query))

    # Create a unique temporary file to hold the raw query, using \copy
    (temp_sql_file, temp_sql_file_path) = tempfile.mkstemp(prefix='b_sql_', dir='/tmp')
    with open(temp_sql_file_path, 'w') as file:
        with_header = ' HEADER' if header else ''
        format_delimiter = 'delimiter \'|\' ' if delimiter == '|' else ''
        file.write('\\copy ({}) To STDOUT with {}CSV{}'.format(query.replace('\n', ''), format_delimiter, with_header))

    return temp_sql_file, temp_sql_file_path


def execute_psql(temp_sql_file_path, source_path, database_string):
    """ Executes the sql located in the temporary sql

        Args:
            temp_sql_file_path: the file path to temporarily store the copy SQL
            source_path: output path of the csv
            database_string: connection string to the database
    """
    try:
        log_time = time.time()
        # open the file to append
        source_file = open(source_path, 'a')
        # pass the command to the psql process
        cat_command = subprocess.Popen(['cat', temp_sql_file_path], stdout=subprocess.PIPE)
        # psql appends to source_path
        subprocess.call(['psql', database_string, '-v', 'ON_ERROR_STOP=1'], stdin=cat_command.stdout,
                        stderr=subprocess.STDOUT, stdout=source_file)
        source_file.close()

        logger.debug('Wrote {}, took {} seconds'.format(os.path.basename(source_path), time.time() - log_time))
    except subprocess.CalledProcessError as e:
        # Not logging the command as it can contain the database connection string
        e.cmd = '[redacted]'
        logger.error(e)
        # temp file contains '\copy ([SQL]) To STDOUT with CSV HEADER' so the SQL is 7 chars in up to the last 27 chars
        sql = subprocess.check_output(['cat', temp_sql_file_path]).decode()[7:-27]
        logger.error('Faulty SQL: {}'.format(sql))
        raise e
    except Exception as e:
        logger.error(e)
        # temp file contains '\copy ([SQL]) To STDOUT with CSV HEADER' so the SQL is 7 chars in up to the last 27 chars
        sql = subprocess.check_output(['cat', temp_sql_file_path]).decode()[7:-27]
        logger.error('Faulty SQL: {}'.format(sql))
        raise e
