import datetime
import json
import logging
import requests
import sys
import time
import xmltodict
import os
import boto3
import glob
from dateutil.relativedelta import relativedelta
from collections import namedtuple
from requests.exceptions import ConnectionError, ReadTimeout
from urllib3.exceptions import ReadTimeoutError

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import get_utc_now
from dataactcore.models.domainModels import ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT


logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def list_data(data):
    """ Make dictionaries into a list

        Args:
            data: dictionaries to turn into a list of those dictionaries

        Returns:
            a list of dictionaries or the data that was provided if it wasn't a dictionary
    """
    if isinstance(data, dict):
        # make a list so it's consistent
        data = [data, ]
    return data


def validate_load_dates(arg_start_date, arg_end_date, arg_auto, load_type, arg_date_format='%m/%d/%Y',
                        output_date_format='%m/%d/%Y'):
    """ Validate and transform command line arguments of loader dates as desired.

        Args:
            arg_start_date: the incoming argument state date
            arg_end_date: the incoming argument end date
            arg_auto: the incoming argument auto, boolean indicating whether to autogenerate dates
            load_type: the external loader data type
            arg_date_format: the incoming argument dates expected format
            output_date_format: the outgoing format of dates

        Returns:
            start and end dates in the output_date_format format

        Raises:
            ValueError if not in the right format or if arguments are logically inconsistent
    """
    start_date = None
    end_date = None

    if arg_auto and not (arg_start_date or arg_end_date):
        sess = GlobalDB.db().session
        # find yesterday and the date of the last successful generation
        yesterday = get_utc_now().date() - relativedelta(days=1)
        last_update = sess.query(ExternalDataLoadDate). \
            filter_by(external_data_type_id=EXTERNAL_DATA_TYPE_DICT[load_type]).one_or_none()
        start_date = last_update.last_load_date_start.date() if last_update else yesterday
        start_date = start_date.strftime(output_date_format)

    if arg_start_date:
        arg_date = arg_start_date[0]
        try:
            start_date = datetime.datetime.strptime(arg_date, arg_date_format)
        except ValueError as e:
            logger.error(f'Date {arg_date} not in proper format ({arg_date_format})')
            raise e
        start_date = start_date.strftime(output_date_format)

    if arg_end_date:
        arg_date = arg_end_date[0]
        try:
            end_date = datetime.datetime.strptime(arg_date, arg_date_format)
        except ValueError as e:
            logger.error(f'Date {arg_date} not in proper format ({arg_date_format})')
            raise e
        end_date = end_date.strftime(output_date_format)

    # Validate that start/end date have been provided in some way and that they are in the right order
    if not (start_date or end_date):
        logger.error('start_date, end_date, or auto setting is required.')
        raise ValueError('start_date, end_date, or auto setting is required.')

    if start_date and end_date and (datetime.datetime.strptime(start_date, output_date_format)
                                    > datetime.datetime.strptime(end_date, output_date_format)):
        logger.error('Start date cannot be later than end date.')
        raise ValueError('Start date cannot be later than end date.')

    return start_date, end_date


def get_xml_with_exception_hand(url_string, namespaces, expect_entries=True):
    """ Retrieve XML data from a feed, allow for multiple retries and timeouts

        Args:
            url_string: string path to the feed we are getting data from
            namespaces: dict of namespaces to clean up for the xml parsing
            expect_entries: boolean of whether we should check the length of the list

        Returns:
            The XML response from the url provided

        Raises:
            ConnectionResetError, ReadTimeoutError, ConnectionError, ReadTimeout:
                If there is a problem calling the url provided
            KeyError:
                If one of the expected keys doesn't exist
    """
    exception_retries = -1
    retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60

    while exception_retries < len(retry_sleep_times):
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            if expect_entries:
                # we should always expect entries, otherwise we shouldn't be calling it
                resp_dict = xmltodict.parse(resp.text, process_namespaces=True, namespaces=namespaces)
                len(list_data(resp_dict['feed']['entry']))
            break
        except (ConnectionResetError, ReadTimeoutError, ConnectionError, ReadTimeout, KeyError) as e:
            exception_retries += 1
            request_timeout += 60
            if exception_retries < len(retry_sleep_times):
                logger.info('Connection exception. Sleeping {}s and then retrying with a max wait of {}s...'
                            .format(retry_sleep_times[exception_retries], request_timeout))
                time.sleep(retry_sleep_times[exception_retries])
            else:
                logger.info('Connection to feed lost, maximum retry attempts exceeded.')
                raise e
    return resp


# TODO: Refacator to use backoff
def get_with_exception_hand(url_string):
    """ Retrieve data from API, allow for multiple retries and timeouts

        Args:
            url_string: URL to make the request to

        Returns:
            API response from the URL
    """
    exception_retries = -1
    retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60
    response_dict = None

    def handle_resp(exception_retries, request_timeout):
        exception_retries += 1
        request_timeout += 60
        if exception_retries < len(retry_sleep_times):
            logger.info('Sleeping {}s and then retrying with a max wait of {}s...'
                        .format(retry_sleep_times[exception_retries], request_timeout))
            time.sleep(retry_sleep_times[exception_retries])
            return exception_retries, request_timeout
        else:
            logger.error('Maximum retry attempts exceeded.')
            sys.exit(2)

    while exception_retries < len(retry_sleep_times):
        # Adding this to log the response if we're unable to decode it
        resp = None
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            response_dict = json.loads(resp.text)
            # We get errors back as regular JSON, need to catch them somewhere
            if response_dict.get('error'):
                err = response_dict.get('error')
                message = response_dict.get('message')
                logger.warning('Error processing response: {} {}'.format(err, message))
                exception_retries, request_timeout = handle_resp(exception_retries, request_timeout)
                continue
            break
        except (ConnectionResetError, ReadTimeoutError, requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout, json.decoder.JSONDecodeError) as e:
            if resp:
                logger.exception(resp.text)
            logger.exception(e)
            exception_retries, request_timeout = handle_resp(exception_retries, request_timeout)

    return response_dict


def trim_nested_obj(obj):
    """ A recursive version to trim all the values in a nested object

        Args:
            obj: object to recursively trim

        Returns:
            dict if object, list of values if list, trimmed if string, else obj
    """
    if isinstance(obj, dict):
        return {k: trim_nested_obj(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [trim_nested_obj(v) for v in obj]
    elif isinstance(obj, str):
        return obj.strip()
    return obj


def flatten_json(json_obj):
    """ Flatten a JSON object into a single row.
            {'a': {'b': '1', 'c': ['d', 'e']}} => {'a_b': '1', 'a_c_1': 'd', 'a_c_2': 'e'}

        Args:
            json_obj: JSON object to flatten

        Returns:
            Single row of values from the json_obj JSON
    """
    out = {}

    def _flatten(list_item, name=''):
        if type(list_item) is dict:
            for item in list_item:
                _flatten(list_item[item], name + item + '_')
        elif type(list_item) is list:
            count = 0
            for item in list_item:
                _flatten(item, name + str(count) + '_')
                count += 1
        else:
            out[name[:-1]] = list_item

    _flatten(json_obj)
    return out


def get_prefixed_file_list(file_path, aws_prefix, bucket_name='sf_133_bucket', file_extension='csv'):
    """ Get a list of files starting with the given prefix

        Args:
            file_path: path to where files are stored
            aws_prefix: prefix to filter which files to pull from AWS
            bucket_name: name of the bucket from which to pull the files
            file_extension: the extension of the files to look for

        Returns:
            A list of tuples containing information about existing
    """
    FileInfo = namedtuple('FileInfo', ['full_file', 'file'])
    if file_path is not None:
        logger.info('Loading local files')
        # get list of prefixed files in the specified local directory
        found_files = glob.glob(os.path.join(file_path, f'{aws_prefix}*.{file_extension}'))
        file_list = [FileInfo(file_info, os.path.basename(file_info)) for file_info in found_files]
    else:
        logger.info("Loading Files")
        if CONFIG_BROKER["use_aws"]:
            # get list of prefixed files in the config bucket on S3
            s3_client = boto3.client('s3', region_name=CONFIG_BROKER['aws_region'])
            response = s3_client.list_objects_v2(Bucket=CONFIG_BROKER[bucket_name], Prefix=aws_prefix)
            file_list = []
            for obj in response.get('Contents', []):
                file_url = s3_client.generate_presigned_url('get_object', {'Bucket': CONFIG_BROKER[bucket_name],
                                                                           'Key': obj['Key']}, ExpiresIn=600)
                file_list.append(FileInfo(file_url, obj['Key']))
        else:
            file_list = []

    return file_list
