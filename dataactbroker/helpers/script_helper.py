import json
import logging
import requests
import sys
import time
import xmltodict

from requests.exceptions import ConnectionError, ReadTimeout
from urllib3.exceptions import ReadTimeoutError


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
            logger.exception(e)
            exception_retries, request_timeout = handle_resp(exception_retries, request_timeout)

    return response_dict
