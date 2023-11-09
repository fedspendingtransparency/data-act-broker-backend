import requests
import xmltodict
import logging
import time

from requests.exceptions import ConnectionError, ReadTimeout
from urllib3.exceptions import ReadTimeoutError


logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)


def list_data(data):
    """ Make dictionaries into a list

        Args:
            data: dictionaries to turn into a list of those dictionaries

        Returns a list of dictionaries or the data that was provided if it wasn't a dictionary
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
