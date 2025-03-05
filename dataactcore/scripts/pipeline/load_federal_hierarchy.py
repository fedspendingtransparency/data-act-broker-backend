import argparse
import asyncio
import csv
import json
import logging
import pandas as pd
import sys

from datetime import datetime, timedelta

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.interfaces.function_bag import update_external_data_load_date
from dataactcore.broker_logging import configure_logging
from dataactcore.models.domainModels import Office, SubTierAgency, CGAC, FREC, ExternalDataLoadDate
from dataactcore.models.lookups import EXTERNAL_DATA_TYPE_DICT
from dataactcore.utils.loader_utils import insert_dataframe

from dataactvalidator.health_check import create_app
from dataactvalidator.filestreaming.csv_selection import write_query_to_file

from dataactbroker.helpers.script_helper import get_with_exception_hand

logger = logging.getLogger(__name__)
logging.getLogger('requests').setLevel(logging.WARNING)

API_URL = CONFIG_BROKER['sam']['federal_hierarchy']['api_url'].format(CONFIG_BROKER['sam']['api_key'])
REQUESTS_AT_ONCE = 5
LIMIT = 100

TOP_SUB_LEVELS = ['1', '2']
OFFICE_LEVELS = ['3', '4', '5', '6', '7']
OFFICE_TYPES = ['contract_funding', 'contract_awards', 'financial_assistance_awards', 'financial_assistance_funding']


def load_offices(sess, filename, update_db, pull_all, updated_date_from, export_office, metrics):
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
    logger.info('Starting feed: %s', API_URL.replace(CONFIG_BROKER['sam']['api_key'], '[API_KEY]'))

    if filename:
        logger.info(f'Creating a file ({filename}) with the data from this pull')
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
    levels = TOP_SUB_LEVELS + OFFICE_LEVELS if filename else OFFICE_LEVELS
    for level in levels:
        # Create URL with the level and status parameter (default is just active, we want inactive as well)
        params = {'level': level, 'status': 'all'}

        # Add updateddatefrom parameter to the URL
        if not pull_all:
            params['updateddatefrom'] = updated_date_from

        # Retrieve the total count of expected records for this pull
        param_string = '&'.join(f'{k}={v}' for k, v in params.items())
        total_expected_records = get_with_exception_hand(f'{API_URL}&{param_string}')['totalrecords']
        metrics[f'level_{level}_records'] = total_expected_records
        logger.info(f'{total_expected_records} level-{level} record(s) expected')
        if total_expected_records == 0:
            empty_pull_count += 1
            continue

        entries_processed = 0
        while True:
            # Create an dataframe with all the data from the API
            raw_dataframe = pd.DataFrame()
            new_offices = pd.DataFrame()
            start = entries_processed + 1
            for response_dict in pull_offices(params, entries_processed):
                # Process the entry if it isn't an error
                for org in response_dict.get('orglist', []):
                    entries_processed += 1

                    # Add to the file data structure
                    if filename:
                        row = pd.json_normalize(flatten_json(org))
                        raw_dataframe = pd.concat([raw_dataframe, row])

                    # Don't process the TOP_SUB_LEVELS, but store them in the fed hierarchy export
                    if level in TOP_SUB_LEVELS:
                        continue

                    if update_db:
                        new_office = parse_raw_office(org)
                        if new_office:
                            # store all the cgacs/subtiers loaded in from this run, to be filtered later
                            metrics['missing_cgacs'].append(new_office['agency_code'])
                            metrics['missing_subtier_codes'].append(org.get('agencycode'))

                            new_offices = pd.concat([pd.DataFrame(new_office, index=[0]), new_offices])
            new_offices.reset_index(drop=True, inplace=True)

            if filename and len(raw_dataframe.index) > 0:
                # Ensure headers are handled correctly
                for header in list(raw_dataframe.columns.values):
                    if header not in file_headers:
                        file_headers.append(header)
                        logger.info('Headers missing column: %s', header)

                # Adding all the extra headers we might not have in the dataframe for any reason
                raw_dataframe = raw_dataframe.reindex(columns=file_headers)

                # Write to file
                with open(filename, 'a') as f:
                    raw_dataframe.to_csv(f, index=False, header=False, columns=file_headers)

            if update_db and not new_offices.empty:
                store_offices(sess, new_offices, pull_all, level)

            logger.info('Processed rows %s-%s', start, entries_processed)
            if entries_processed == total_expected_records:
                # Feed has finished
                break

            if entries_processed > total_expected_records:
                # We have somehow retrieved more records than existed at the beginning of the pull
                logger.error(f'Total expected records: {total_expected_records},'
                             f' Number of records retrieved: {entries_processed}')
                sys.exit(2)

    if update_db:
        sess.commit()

    if export_office:
        logger.info(f'Creating a file ({export_office}) with the data from the database')
        all_offices = sess.query(Office)
        write_query_to_file(sess, all_offices, export_office, generate_headers=True)

    if empty_pull_count == len(levels):
        logger.error('No records retrieved from the Federal Hierarchy API')
        sys.exit(3)

    logger.info('Finished')


def pull_offices(params, entries_processed=0):
    """ Hit the FH API and return the raw json responses

        Args:
            params: dict of the current params to lookup
            entries_processed: how many entries have been processed (for offset counting)

        Returns:
            list of json responses
    """
    params['limit'] = str(LIMIT)

    async def _fed_hierarchy_async_get(entries_already_processed):
        response_list = []
        loop = asyncio.get_event_loop()
        futures = []
        for start_offset in range(REQUESTS_AT_ONCE):
            params['offset'] = str(entries_already_processed + (start_offset * LIMIT))
            param_string = '&'.join(f'{k}={v}' for k, v in params.items())
            futures.append(
                loop.run_in_executor(
                    None,
                    get_with_exception_hand,
                    f'{API_URL}&{param_string}'
                )
            )
        for response in await asyncio.gather(*futures):
            response_list.append(response)
            pass
        return response_list

    # Retrieve LIMIT*REQUESTS_AT_ONCE records from the API
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(_fed_hierarchy_async_get(entries_processed))


def parse_raw_office(org):
    """ Given a raw office org object, translate it into a dictionary for the database

        Args:
            org: json dict of the raw office data to process

        Returns:
            dict of office prepped for the database (or None if it's to be skipped)
    """
    # trim incoming values
    org = trim_nested_obj(org)

    agency_code = get_normalized_agency_code(org.get('cgaclist', [{'cgac': None}])[0]['cgac'], org.get('agencycode'))
    # TEMPORARILY REPLACE Navy, Army, AND Air Force WITH DOD
    if agency_code in ['017', '021', '057']:
        agency_code = '097'
    if not org.get('aacofficecode') or not org.get('agencycode') or not agency_code:
        # Item from Fed Hierarchy is missing necessary data, ignore it
        return None
    if org.get('fhorgname') == "DO NOT USE":
        # this is an actual value for the office name in the data, which we should adhere to
        return None

    effective_start_date = org.get('effectivestartdate')
    # The FH data does include a start date that is too early for pandas to process ("0016-04-12")
    # This marks it a little more reasonable to our earliest default start date.
    if not effective_start_date or effective_start_date < '2000-01-01 00:00':
        effective_start_date = '2000-01-01 00:00'
    effective_end_date = (org.get('effectiveenddate') if org['status'] == 'ACTIVE'
                          else org.get('effectiveenddate') or '2000-01-02 00:00')

    new_office = {
        "office_code": org.get('aacofficecode'),
        "office_name": org.get('fhorgname'),
        "sub_tier_code": org.get('agencycode'),
        "agency_code": agency_code,
        "effective_start_date": effective_start_date,
        "effective_end_date": effective_end_date,
        "contract_funding_office": False,
        "contract_awards_office": False,
        "financial_assistance_awards_office": False,
        "financial_assistance_funding_office": False
    }

    for off_type in org.get('fhorgofficetypelist', []):
        office_type = off_type['officetype'].lower().replace(' ', '_')
        if office_type in OFFICE_TYPES:
            new_office[f'{office_type}_office'] = True

    return new_office


# TODO: reuse dataactbroker.helpers.script_helper.trim_nested_obj
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


# TODO: reuse dataactbroker.helpers.script_helper.flatten_json
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
    if agency_code in ['011', '016', '352', '537', '033', '511']:
        st_agency = sess.query(SubTierAgency).filter(SubTierAgency.sub_tier_agency_code == subtier_code).one_or_none()
        if st_agency:
            agency_code = st_agency.frec.frec_code
        else:
            agency_code = None

    return agency_code


def store_offices(sess, new_offices, pull_all, level):
    """ The feed can include *duplicate* office records that were generated by FPDS before 4/1/2016.
        This consolidates the duplicate office records in the incoming feed with potentially the already existing
        offices in the database, and then updates the database accordingly.

        Args:
            sess: database connection
            new_offices: dataframe of the new office data to be stored
            pull_all: Boolean; pull all historical data, instead of just the latest.
            level: the current level we're processing (for checking inactive office histories)
    """
    date_cols = ['effective_start_date', 'effective_end_date']
    type_cols = [f'{office_type}_office' for office_type in OFFICE_TYPES]
    other_cols = ['office_name', 'agency_code', 'sub_tier_code']
    shared_cols = date_cols + type_cols + other_cols
    shared_df_cols = ['office_code'] + shared_cols

    # Pull the relevant office data from the database
    old_offices = sess.query(Office).filter(Office.office_code.in_(list(new_offices['office_code'])))
    old_offices_df = pd.read_sql(old_offices.statement, old_offices.session.bind, parse_dates=date_cols)
    for date_col in date_cols:
        old_offices_df[date_col] = old_offices_df[date_col].dt.strftime('%Y-%m-%d %H:%M')

    if pull_all:
        # Full load:
        #   If there are active records in the db, they're still true for the full load on that day.
        #   No need for looking up all the inactive records status as we have them all here.
        #   Just merge them all together normally as we're working off a full snapshot.
        shared_df = pd.concat([new_offices[shared_df_cols], old_offices_df[shared_df_cols]])
        merged_offices = merge_offices(shared_df)
    else:
        # Nightly load: split into active and inactive for different scenarios
        new_active_df = new_offices[new_offices['effective_end_date'].isnull()]
        new_inactive_df = new_offices[new_offices['effective_end_date'].notnull()]

        # If, for some odd reason, a daily load has *both* an active and inactive record with the same code,
        # delete it from the active list. The new inactive record may have an older start date
        # and we'd be merging it with the active record anyways
        new_active_df = new_active_df[~new_active_df['office_code'].isin(new_inactive_df['office_code'])]

        merged_offices = pd.DataFrame(columns=shared_df_cols)
        if not new_active_df.empty:
            # Active: Keep everything the same but take the older start date and orgtypes if applicable.
            old_active_df = old_offices_df[old_offices_df['office_code'].isin(new_active_df['office_code'])]
            if not old_active_df.empty:
                # Set the DB effective end date to way back to distinguish it from the incoming active record
                old_active_df['effective_end_date'] = '2000-01-01 00:00'
                new_active_df = pd.concat([new_active_df[shared_df_cols], old_active_df[shared_df_cols]])
                new_active_df = merge_offices(new_active_df)
            merged_offices = pd.concat([merged_offices[shared_df_cols], new_active_df[shared_df_cols]])

        if not new_inactive_df.empty:
            # Inactive: Discard incoming record but keep the code. It could be either
            #   officially declaring this record is now inactive
            #   OR an older historical record
            # Get the office's history and figure out its status from there
            new_inactive_office_codes = list(new_inactive_df['office_code'])
            logger.info(f'New inactive records found ({new_inactive_office_codes}). Looking them up...')
            new_inactive_df = pd.DataFrame(columns=shared_df_cols)
            for new_inactive_office_code in new_inactive_office_codes:
                inactive_params = {
                    'aacofficecode': new_inactive_office_code,
                    'level': level,
                    'status': 'all'
                }
                # This assumes an office's historical record count is always less than LIMIT
                # If it's not, use pull_offices instead to multiply it by REQUESTS_AT_ONCE
                param_string = '&'.join(f'{k}={v}' for k, v in inactive_params.items())
                office_history = get_with_exception_hand(f'{API_URL}&{param_string}')
                for org in office_history.get('orglist', []):
                    historical_record = parse_raw_office(org)
                    if historical_record:
                        new_inactive_df = pd.concat([pd.DataFrame(historical_record, index=[0]), new_inactive_df])
            logger.info('Inactive records figured out')
            new_inactive_df = merge_offices(new_inactive_df)
            merged_offices = pd.concat([merged_offices[shared_df_cols], new_inactive_df[shared_df_cols]])

    merged_offices['created_at'] = datetime.now()
    merged_offices['updated_at'] = datetime.now()

    old_offices.delete(synchronize_session=False)
    insert_dataframe(merged_offices, 'office', sess.connection())
    sess.commit()


def merge_offices(office_df, combine_org_types=True):
    """ Given a dataframe of duplicate offices, merge them into unique records with the appropriate values.

        Merge logic:
            Sorted by the effective end date, effective_start_date and grouped by the office code...
              Set the earliest effective start date ("min")
              Set the boolean orgtypecols to True if *any* of them are True over time, or latest
              Set the rest to the latest record (active or latest inactive)
                  - for full loads, this will include the database record which could be active and null
                  - for iterative loads, this will ignore the database record if it's currently active (i.e. null)
                    by setting it to a really old date; otherwise it will pull the database end date too
        Args:
            office_df: dataframe of office data that may include multiple records of the same office_code
            combine_org_types: if True, an orgtype is true if at any point it was True; otherwise, use the latest.
                               Defaults to True

        Returns:
            dataframe of the same columns but with the duplicates merged to single records
    """
    # generally effective end_date is our best way of determining the order but sometimes they can match too
    # so also sort by the effective_start_date too in case of a tie
    office_df.sort_values(by=['effective_end_date', 'effective_start_date'], inplace=True)
    shared_grouped = office_df.groupby(by=['office_code'], sort=False, as_index=False, dropna=False)
    # panda's aggregates ("first", "last", "min", "max") ignore NATs, so for selecting the "latest" record, we're
    # using sorting on end date and this lambda function to effectively be "last" but include the NATs.
    # If there is a NAT, it's considered "active".
    agg_selections = {col: lambda x: x.values[-1] for col in office_df.columns}
    agg_selections['effective_start_date'] = "min"
    for org_type in OFFICE_TYPES:
        agg_selections[f'{org_type}_office'] = "any" if combine_org_types else (lambda x: x.values[-1])
    shared_df = shared_grouped.agg(agg_selections)
    return shared_df


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
        last_pull_date = sess.query(ExternalDataLoadDate.last_load_date_start).\
            filter_by(external_data_type_id=EXTERNAL_DATA_TYPE_DICT['office']).one_or_none()
        if not last_pull_date:
            logger.error('The -a or -d flag must be set when there is no latest run in the database.')
            sys.exit(1)
        # We want to make the date one day earlier to account for any timing weirdness between the two systems
        updated_date_from = last_pull_date[0].date() - timedelta(days=1)

    # Handle the filename parameter
    filename = args.filename[0] if args.filename else None

    # Handle the export office parameter
    export_office = args.export_office[0] if args.export_office else None

    # Handle a complete data reload
    if args.all and not args.ignore_db:
        logger.info('Emptying out the Office table for a complete reload.')
        sess.execute('''TRUNCATE TABLE office RESTART IDENTITY''')
        sess.commit()

    try:
        load_offices(sess, filename, not args.ignore_db, args.all, updated_date_from, export_office, metrics_json)
    except Exception as e:
        logger.exception(e)
        sys.exit(1)

    # find if there were any new cgacs/subtiers added
    all_cgacs = [cgac.cgac_code for cgac in sess.query(CGAC.cgac_code)]
    all_frecs = [frec.frec_code for frec in sess.query(FREC.frec_code)]
    all_subtiers = [subtier.sub_tier_agency_code for subtier in sess.query(SubTierAgency.sub_tier_agency_code)]
    metrics_json['missing_cgacs'] = list(set(metrics_json['missing_cgacs']) - set(all_cgacs + all_frecs))
    metrics_json['missing_subtier_codes'] = list(set(metrics_json['missing_subtier_codes']) - set(all_subtiers))

    update_external_data_load_date(now, datetime.now(), 'office')

    metrics_json['duration'] = str(datetime.now() - now)

    with open('load_federal_hierarchy_metrics.json', 'w+') as metrics_file:
        json.dump(metrics_json, metrics_file)
    logger.info('Script complete')


if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()
