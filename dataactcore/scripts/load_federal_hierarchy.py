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
from dataactcore.models.domainModels import Office, SubTierAgency, CGAC, FREC

from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.csv_selection import write_query_to_file

logger = logging.getLogger(__name__)
logging.getLogger('requests').setLevel(logging.WARNING)

API_URL = CONFIG_BROKER['sam']['federal_hierarchy_api_url'].format(CONFIG_BROKER['sam']['federal_hierarchy_api_key'])
REQUESTS_AT_ONCE = 5


def pull_offices(sess, filename, update_db, pull_all, updated_date_from, export_office, metrics):
    """ Pull Office data from the Federal Hierarchy API and update the DB, return it as a file, or both.

        Args:
            sess: Current DB session.
            filename: Name of the file to be generated with the API data. If None, no file will be created.
            update_db: Boolean; update the DB tables with the new data from the API.
            pull_all: Boolean; pull all historical data, instead of just the latest.
            updated_date_from: Date to pull data from. Defaults to the date of the most recently updated Office.
            export_office: when provided, name of the file to export the office list to
            metrics: an object containing information for the metrics file
    """
    logger.info('Starting feed: %s', API_URL.replace(CONFIG_BROKER['sam']['federal_hierarchy_api_key'], '[API_KEY]'))
    top_sub_levels = ['1', '2']
    office_levels = ['3', '4', '5', '6', '7']
    levels = top_sub_levels + office_levels if filename else office_levels

    if filename:
        logger.info('Creating a file ({}) with the data from this pull'.format(filename))
        # Write headers to file
        file_headers = [
            'fhorgid', 'fhorgname', 'fhorgtype', 'description', 'level', 'status', 'region', 'categoryid',
            'effectivestartdate', 'effectiveenddate', 'createdby', 'createddate', 'updatedby', 'lastupdateddate',
            'fhdeptindagencyorgid', 'fhagencyorgname', 'agencycode', 'oldfpdsofficecode', 'aacofficecode',
            'cgaclist_0_cgac', 'cgaclist_1_cgac', 'cgaclist_2_cgac', 'cgaclist_3_cgac', 'cgaclist_4_cgac',
            'fhorgofficetypelist_0_officetype', 'fhorgofficetypelist_0_officetypestartdate',
            'fhorgofficetypelist_0_officetypeenddate', 'fhorgofficetypelist_1_officetype',
            'fhorgofficetypelist_1_officetypestartdate', 'fhorgofficetypelist_1_officetypeenddate',
            'fhorgofficetypelist_2_officetype', 'fhorgofficetypelist_2_officetypestartdate',
            'fhorgofficetypelist_2_officetypeenddate', 'fhorgofficetypelist_3_officetype',
            'fhorgofficetypelist_3_officetypeenddate', 'fhorgofficetypelist_3_officetypestartdate',
            'fhorgaddresslist_0_city', 'fhorgaddresslist_0_state', 'fhorgaddresslist_0_country_code',
            'fhorgaddresslist_0_addresstype', 'fhorgnamehistory_0_fhorgname', 'fhorgnamehistory_0_effectivedate',
            'fhorgparenthistory_0_fhfullparentpathid', 'fhorgparenthistory_0_fhfullparentpathname',
            'fhorgparenthistory_0_effectivedate', 'links_0_href', 'links_0_rel', 'links_1_href', 'links_1_rel',
            'links_2_href', 'links_2_rel']
        with open(filename, 'w+') as f:
            csv_writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
            csv_writer.writerow(file_headers)

    empty_pull_count = 0
    for level in levels:
        # Create URL with the level parameter
        url_with_params = '{}&level={}'.format(API_URL, level)

        # Add updateddatefrom parameter to the URL
        if not pull_all:
            url_with_params += '&updateddatefrom={}'.format(updated_date_from)

        # Retrieve the total count of expected records for this pull
        total_expected_records = json.loads(get_with_exception_hand(url_with_params).text)['totalrecords']
        metrics['level_{}_records'.format(str(level))] = total_expected_records
        logger.info('{} level-{} record(s) expected'.format(str(total_expected_records), str(level)))
        if total_expected_records == 0:
            empty_pull_count += 1
            continue

        limit = 100
        entries_processed = 0
        while True:
            async def _fed_hierarchy_async_get(entries_already_processed):
                response_list = []
                loop = asyncio.get_event_loop()
                futures = [
                    loop.run_in_executor(
                        None,
                        get_with_exception_hand,
                        '{}&limit={}&offset={}'.format(url_with_params, str(limit),
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
            full_response = loop.run_until_complete(_fed_hierarchy_async_get(entries_processed))

            # Create an object with all the data from the API
            dataframe = pd.DataFrame()
            offices = {}
            start = entries_processed + 1
            for response_dict in full_response:
                # Process the entry if it isn't an error
                for org in response_dict.get('orglist', []):
                    entries_processed += 1

                    # Add to the file data structure
                    if filename:
                        row = json_normalize(flatten_json(org))
                        dataframe = dataframe.append(row)

                    # Don't process the top_sub_levels, but store them in the fed hierarchy export
                    if level in top_sub_levels:
                        continue

                    # Add to the list of DB objects
                    if update_db:
                        agency_code = get_normalized_agency_code(org.get('cgaclist', [{'cgac': None}])[0]['cgac'],
                                                                 org.get('agencycode'))
                        # TEMPORARILY REPLACE Navy, Army, AND Air Force WITH DOD
                        if agency_code in ['017', '021', '057']:
                            agency_code = '097'
                        if not org.get('aacofficecode') or not org.get('agencycode') or not agency_code:
                            # Item from Fed Hierarchy is missing necessary data, ignore it
                            continue
                        # store all the cgacs/subtiers loaded in from this run, to be filtered later
                        metrics['missing_cgacs'].append(agency_code)
                        metrics['missing_subtier_codes'].append(org.get('agencycode'))
                        new_office = Office(office_code=org.get('aacofficecode'), office_name=org.get('fhorgname'),
                                            sub_tier_code=org.get('agencycode'), agency_code=agency_code,
                                            contract_funding_office=False, contract_awards_office=False,
                                            financial_assistance_awards_office=False,
                                            financial_assistance_funding_office=False)

                        for off_type in org.get('fhorgofficetypelist', []):
                            office_type = off_type['officetype'].lower().replace(' ', '_')
                            if office_type in ['contract_funding', 'contract_awards', 'financial_assistance_awards',
                                               'financial_assistance_funding']:
                                setattr(new_office, office_type + '_office', True)

                        offices[org.get('aacofficecode')] = new_office

            if filename and len(dataframe.index) > 0:
                # Ensure headers are handled correctly
                for header in list(dataframe.columns.values):
                    if header not in file_headers:
                        file_headers.append(header)
                        logger.info('Headers missing column: %s', header)

                # Write to file
                with open(filename, 'a') as f:
                    dataframe.to_csv(f, index=False, header=False, columns=file_headers)

            if update_db:
                office_codes = set(offices.keys())
                sess.query(Office).filter(Office.office_code.in_(office_codes)).delete(synchronize_session=False)
                sess.add_all(offices.values())

            logger.info('Processed rows %s-%s', start, entries_processed)
            if entries_processed == total_expected_records:
                # Feed has finished
                break

            if entries_processed > total_expected_records:
                # We have somehow retrieved more records than existed at the beginning of the pull
                logger.error('Total expected records: {}, Number of records retrieved: {}'.format(
                    total_expected_records, entries_processed))
                sys.exit(2)

    if update_db:
        sess.commit()

    if export_office:
        logger.info('Creating a file ({}) with the data from the database'.format(export_office))
        all_offices = sess.query(Office)
        write_query_to_file(sess, all_offices, export_office, generate_headers=True)

    if empty_pull_count == len(levels):
        logger.error('No records retrieved from the Federal Hierarchy API')
        sys.exit(3)

    logger.info('Finished')


def flatten_json(json_obj):
    """ Flatten a JSON object into a single row.

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
            response_dict = json.loads(resp)
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


def main():
    now = datetime.now()
    parser = argparse.ArgumentParser(description='Pull data from the Federal Hierarchy API.')
    parser.add_argument('-a', '--all', help='Clear out the database and get historical data', action='store_true')
    parser.add_argument('-f', '--filename', help='Generate a local CSV file from the data.', nargs=1, type=str)
    parser.add_argument('-o', '--export_office', help='Export the current office table. '
                                                      'Please provide the file name/path.', nargs=1, type=str)
    parser.add_argument('-d', '--pull_date', help='Date from which to start the pull', nargs=1, type=str)
    parser.add_argument('-i', '--ignore_db', help='Do not update the DB tables', action='store_true')
    args = parser.parse_args()

    if args.all and args.pull_date:
        logger.error('The -a and -d flags conflict, cannot use both at once.')
        sys.exit(1)

    metrics_json = {
        'script_name': 'load_federal_hierarchy.py',
        'start_time': str(now),
        'level_1_records': 0,
        'level_2_records': 0,
        'level_3_records': 0,
        'level_4_records': 0,
        'level_5_records': 0,
        'level_6_records': 0,
        'level_7_records': 0,
        'missing_cgacs': [],
        'missing_subtier_codes': []
    }

    # Handle the pull_date parameter
    updated_date_from = None
    if args.pull_date:
        try:
            updated_date_from = args.pull_date[0]
            datetime.strptime(updated_date_from, '%Y-%m-%d')
        except ValueError:
            logger.error('The date given to the -d flag was not parseable.')
            sys.exit(1)

    # Get or create the start date
    sess = GlobalDB.db().session
    if not args.all and not updated_date_from:
        last_pull_date = sess.query(func.max(Office.updated_at)).one_or_none()
        if not last_pull_date:
            logger.error('The -a or -d flag must be set when there are no Offices present in the database.')
            sys.exit(1)
        updated_date_from = last_pull_date[0].date()

    # Handle the filename parameter
    filename = args.filename[0] if args.filename else None

    # Handle the export office parameter
    export_office = args.export_office[0] if args.export_office else None

    # Handle a complete data reload
    if args.all and not args.ignore_db:
        logger.info('Emptying out the Office table for a complete reload.')
        sess.execute('''TRUNCATE TABLE office RESTART IDENTITY''')

    try:
        pull_offices(sess, filename, not args.ignore_db, args.all, updated_date_from, export_office, metrics_json)
    except Exception as e:
        logger.exception(e)
        sys.exit(1)

    # find if there were any new cgacs/subtiers added
    all_cgacs = [cgac.cgac_code for cgac in sess.query(CGAC.cgac_code)]
    all_frecs = [frec.frec_code for frec in sess.query(FREC.frec_code)]
    all_subtiers = [subtier.sub_tier_agency_code for subtier in sess.query(SubTierAgency.sub_tier_agency_code)]
    metrics_json['missing_cgacs'] = list(set(metrics_json['missing_cgacs']) - set(all_cgacs + all_frecs))
    metrics_json['missing_subtier_codes'] = list(set(metrics_json['missing_subtier_codes']) - set(all_subtiers))

    metrics_json['duration'] = str(datetime.now() - now)

    with open('load_federal_hierarchy_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info('Script complete')


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
