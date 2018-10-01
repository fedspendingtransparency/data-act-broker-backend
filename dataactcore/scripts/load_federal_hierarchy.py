import argparse
import asyncio
import csv
import json
import logging
import pandas as pd
import requests
import sys
import time

from datetime import datetime
from pandas.io.json import json_normalize
from requests.packages.urllib3.exceptions import ReadTimeoutError
from sqlalchemy import func

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import Office, SubTierAgency

from dataactvalidator.health_check import create_app

logger = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)

API_KEY = CONFIG_BROKER['sam']['federal_hierarchy_api_key']
API_URL = "https://api-alpha.sam.gov/prodlike/federalorganizations/v1/orgs?api_key={}".format(API_KEY)
REQUESTS_AT_ONCE = 10


def pull_offices(sess, filename, update_db, pull_all, updated_date_from):
    """ Pull Office data from the Federal Hierarchy API and update the DB, return it as a file, or both.

        Args:
            sess: Current DB session.
            filename: Name of the file to be generated with the API data. If None, no file will be created.
            update_db: Boolean; update the DB tables with the new data from the API.
            pull_all: Boolean; pull all historical data, instead of just the latest.
            updated_date_from: Date to pull data from. Defaults to the date of the most recently updated Office.
    """
    logger.info('Starting get feed: %s', API_URL.replace(API_KEY, "[API_KEY]"))
    office_levels = ["3", "4", "5", "6", "7"]

    if filename:
        # Write headers to file
        file_headers = [
            "fhorgid", "fhorgname", "fhorgtype", "description", "level", "status", "region", "categoryid",
            "effectivestartdate", "effectiveenddate", "createdby", "createddate", "updatedby", "lastupdateddate",
            "fhdeptindagencyorgid", "fhagencyorgname", "agencycode", "oldfpdsofficecode", "aacofficecode",
            "cgaclist_0_cgac", "fhorgofficetypelist_0_officetype", "fhorgofficetypelist_0_officetypestartdate",
            "fhorgofficetypelist_0_officetypeenddate", "fhorgofficetypelist_1_officetype",
            "fhorgofficetypelist_1_officetypestartdate", "fhorgofficetypelist_1_officetypeenddate",
            "fhorgofficetypelist_2_officetype", "fhorgofficetypelist_2_officetypestartdate",
            "fhorgofficetypelist_2_officetypeenddate", "fhorgaddresslist_0_city", "fhorgaddresslist_0_state",
            "fhorgaddresslist_0_country_code", "fhorgaddresslist_0_addresstype", "fhorgnamehistory_0_fhorgname",
            "fhorgnamehistory_0_effectivedate", "fhorgparenthistory_0_fhfullparentpathid",
            "fhorgparenthistory_0_fhfullparentpathname", "fhorgparenthistory_0_effectivedate", "links_0_href",
            "links_0_rel", "links_1_href", "links_1_rel", "links_2_href", "links_2_rel"]
        with open(filename, 'w+') as f:
            csv_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            csv_writer.writerow(file_headers)

    # Get or create the start date
    if not pull_all and not updated_date_from:
        last_pull_date = sess.query(func.max(Office.updated_at)).one_or_none()
        if not last_pull_date:
            logger.error("The -a or -d flag must be set when there are no Offices present in the database.")
            sys.exit(1)
        updated_date_from = last_pull_date[0].date()

    empty_pull_count = 0
    for level in office_levels:
        # Create URL with the level parameter
        url_with_params = "{}&level={}".format(API_URL, level)

        # Add updateddatefrom parameter to the URL
        if updated_date_from:
            url_with_params += "&updateddatefrom={}".format(updated_date_from)

        # Retrieve the total count of expected records for this pull
        total_expected_records = json.loads(requests.get(url_with_params, timeout=60).text)['totalRecords']
        logger.info('{} level-{} record(s) expected'.format(str(total_expected_records), str(level)))
        if total_expected_records == 0:
            empty_pull_count += 1
            continue

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
                        "{}&limit={}&offset={}".format(url_with_params, str(limit),
                                                       str(entries_already_processed + (start_offset * limit)))
                    )
                    for start_offset in range(REQUESTS_AT_ONCE)
                ]
                for response in await asyncio.gather(*futures):
                    response_list.append(response.text)
                    pass
                return response_list
            # End async get requests def

            # Retrieve limit*REQUESTS_AT_ONCE records from the API
            loop = asyncio.get_event_loop()
            full_response = loop.run_until_complete(fed_hierarchy_async_get(entries_processed))

            # Create an object with all the data from the API
            dataframe = pd.DataFrame()
            offices = {}
            start = entries_processed + 1
            for next_resp in full_response:
                response_dict = json.loads(next_resp)

                for org in response_dict.get('orgList', []):
                    entries_processed += 1

                    # Add to the file data structure
                    if filename:
                        row = json_normalize(flatten_json(org))
                        dataframe = dataframe.append(row)

                    # Add to the list of DB objects
                    if update_db:
                        agency_code = get_normalized_agency_code(org.get('cgaclist', [{'cgac': None}])[0]['cgac'],
                                                                 org.get('agencycode'))
                        if not org.get('aacofficecode') or not org.get('agencycode') or not agency_code:
                            # Item from Fed Hierarchy is missing necessary data, ignore it
                            continue

                        new_office = Office(office_code=org.get('aacofficecode'), office_name=org.get('fhorgname'),
                                            sub_tier_code=org.get('agencycode'), agency_code=agency_code,
                                            funding_office=False, contracting_office=False, grant_office=False)

                        for off_type in org.get('fhorgofficetypelist', []):
                            office_type = off_type['officetype'].lower()
                            if office_type in ['contracting', 'funding', 'grant']:
                                setattr(new_office, office_type + '_office', True)

                        offices[org.get('aacofficecode')] = new_office

            if filename and len(dataframe.index) > 0:
                # Ensure headers are handled correctly
                for header in list(dataframe.columns.values):
                    if header not in file_headers:
                        file_headers.append(header)
                        logger.info("Headers missing column: %s", header)

                # Write to file
                with open(filename, 'a') as f:
                    dataframe.to_csv(f, index=False, header=False, columns=file_headers)

            if update_db:
                office_codes = set(offices.keys())
                sess.query(Office).filter(Office.office_code.in_(office_codes)).delete(synchronize_session=False)
                sess.add_all(offices.values())

            logger.info("Processed rows %s-%s", start, entries_processed)
            if entries_processed == total_expected_records:
                # Feed has finished
                break

            if entries_processed > total_expected_records:
                # We have somehow retrieved more records than existed at the beginning of the pull
                logger.error("Total expected records: {}, Number of records retrieved: {}".format(
                    total_expected_records, entries_processed))
                sys.exit(1)

    if update_db:
        sess.commit()

    if empty_pull_count == len(office_levels):
        logger.error("No records retrieved from the Federal Hierarchy API")
        sys.exit(3)

    logger.info("Finished")


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


def get_normalized_agency_code(agency_code, subtier_code):
    """ Handle the specific set of FREC agencies by matching the Office's SubtierAgency to its FREC

        Args:
            agency_code: CGAC agency code given to us by the Fed Hierarchy API
            subtier_code: SubtierAgency code under the selected CGAC agency. May map to a FREC instead

        Returns:
            FREC or CGAC agency code, depending on whether the speicified CGAC has been replaced with FREC
    """
    sess = GlobalDB.db().session
    if agency_code in ['011', '016', '352', '537', '033']:
        st_agency = sess.query(SubTierAgency).filter(SubTierAgency.sub_tier_agency_code == subtier_code).one_or_none()
        if st_agency:
            agency_code = st_agency.frec.frec_code
        else:
            agency_code = None

    return agency_code


def get_with_exception_hand(url_string):
    """ Retrieve data from FPDS, allow for multiple retries and timeouts

        Args:
            url_string: URL to make the request to

        Returns:
            API response from the URL
    """
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
                logger.error('Connection to FPDS feed lost, maximum retry attempts exceeded.')
                sys.exit(1)
    return resp


def main():
    parser = argparse.ArgumentParser(description='Pull data from the Federal Hierarchy API.')
    parser.add_argument('-a', '--all', help='Clear out the database and get historical data', action='store_true')
    parser.add_argument('-f', '--filename', help='Generate a local CSV file from the data.', nargs=1, type=str)
    parser.add_argument('-d', '--pull_date', help='Date from which to start the pull', nargs=1, type=str)
    parser.add_argument('-i', '--ignore_db', help='Do not update the DB tables', action='store_true')
    args = parser.parse_args()

    if args.all and args.pull_date:
        logger.error("The -a and -d flags conflict, cannot use both at once.")
        sys.exit(1)

    # Handle the pull_date parameter
    updated_date_from = None
    if args.pull_date:
        try:
            updated_date_from = args.pull_date[0]
            datetime.strptime(updated_date_from, "%Y-%m-%d")
        except ValueError as e:
            logger.error("The date given to the -d flag was not parseable.")
            sys.exit(1)

    # Handle the filename parameter
    filename = args.filename[0] if args.filename else None
    if filename:
        logger.info("Creating a file ({}) with the data from this pull".format(filename))

    # Handle a complete data reload
    sess = GlobalDB.db().session
    if args.all and not args.ignore_db:
        logger.info("Emptying out the Office table for a complete reload.")
        sess.execute('''TRUNCATE TABLE office RESTART IDENTITY''')
        sess.commit()

    try:
        pull_offices(sess, filename, not args.ignore_db, args.all, updated_date_from)
    except Exception as e:
        logger.error(str(e))
        sys.exit(1)


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
