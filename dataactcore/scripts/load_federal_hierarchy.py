import asyncio
import csv
import json
import logging
import math
import pandas as pd
import requests

from pandas.io.json import json_normalize
from requests.packages.urllib3.exceptions import ReadTimeoutError

from dataactcore.config import CONFIG_BROKER
# from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

API_KEY = CONFIG_BROKER['sam']['federal_hierarchy_api_key']
API_URL = "https://api-alpha.sam.gov/prodlike/federalorganizations/v1/orgs?api_key={}".format(API_KEY)
REQUESTS_AT_ONCE = 100


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def get_with_exception_hand(url_string):
    """ Retrieve data from FPDS, allow for multiple retries and timeouts """
    exception_retries = -1
    retry_sleep_times = [5, 30, 60, 180, 300, 360, 420, 480, 540, 600]
    request_timeout = 60

    while exception_retries < len(retry_sleep_times):
        try:
            resp = requests.get(url_string, timeout=request_timeout)
            break
        except (ConnectionResetError, ReadTimeoutError, requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            exception_retries += 1
            request_timeout += 60
            if exception_retries < len(retry_sleep_times):
                logger.info('Connection exception. Sleeping {}s and then retrying with a max wait of {}s...'
                            .format(retry_sleep_times[exception_retries], request_timeout))
                time.sleep(retry_sleep_times[exception_retries])
            else:
                logger.info('Connection to FPDS feed lost, maximum retry attempts exceeded.')
                raise e
    return resp


def generate_files():
    logger.info('Starting get feed: %s', API_URL.replace(API_KEY, "[API_KEY]"))
    headers = [
        "fhorgid", "fhorgname", "fhorgtype", "description", "level", "status", "region", "categoryid",
        "effectivestartdate", "effectiveenddate", "createdby", "createddate", "updatedby", "lastupdateddate",
        "fhdeptindagencyorgid", "fhagencyorgname", "agencycode", "oldfpdsofficecode", "aacofficecode",
        "cgaclist_0_cgac", "fhorgofficetypelist_0_officetype", "fhorgofficetypelist_0_officetypestartdate",
        "fhorgofficetypelist_0_officetypeenddate", "fhorgaddresslist_0_city", "fhorgaddresslist_0_state",
        "fhorgaddresslist_0_country_code", "fhorgaddresslist_0_addresstype", "fhorgnamehistory_0_fhorgname",
        "fhorgnamehistory_0_effectivedate", "fhorgparenthistory_0_fhfullparentpathid",
        "fhorgparenthistory_0_fhfullparentpathname", "fhorgparenthistory_0_effectivedate", "links_0_href",
        "links_0_rel", "links_1_href", "links_1_rel"]

    with open('fed_hierarchy.csv', 'w') as f:
        csv_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(headers)

    # retrieve the total count of expected records for this pull
    total_expected_records = json.loads(requests.get(API_URL, timeout=60).text)['totalRecords']
    logger.info('{} record(s) expected from this feed'.format(total_expected_records))
    total_expected_records = 3678

    limit = 100
    entries_processed = 0
    while True:
        async def fed_hierarchy_async_get(entries_already_processed):
            response_list = []
            loop = asyncio.get_event_loop()
            futures = [
                loop.run_in_executor(
                    None,
                    get_with_exception_hand,
                    "{}&limit={}&offset={}".format(API_URL, str(limit),
                                                   str(entries_already_processed + (start_offset * limit)))
                )
                for start_offset in range(REQUESTS_AT_ONCE)
            ]
            for response in await asyncio.gather(*futures):
                response_list.append(response.text)
                pass
            return response_list
        # End async get requests def

        logger.info("Retrieving rows %s-%s", str(entries_processed), str(entries_processed + limit * REQUESTS_AT_ONCE))
        loop = asyncio.get_event_loop()
        full_response = loop.run_until_complete(fed_hierarchy_async_get(entries_processed))

        dataframe = pd.DataFrame()
        start = entries_processed + 1
        for next_resp in full_response:
            response_dict = json.loads(next_resp)

            for org in response_dict.get('orgList', []):
                row = json_normalize(flatten_json(org))
                dataframe = dataframe.append(row)
                entries_processed += 1

        df_length = len(dataframe.index)
        with open('fed_hierarchy.csv', 'a') as f:
            dataframe.to_csv(f, index=False, header=False, columns=headers)

        logger.info("Added rows %s-%s to file", start, entries_processed)

        if df_length < (limit * REQUESTS_AT_ONCE):
            break

    logger.info("Complete")


def main():
    # sess = GlobalDB.db().session
    generate_files()


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
