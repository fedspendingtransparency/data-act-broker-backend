import logging
import os
import time
import json
import zipfile
import datetime
import requests
import numpy as np
import pandas as pd
from collections import OrderedDict
from sqlalchemy import and_, func
from ratelimit import limits, sleep_and_retry
from backoff import on_exception, expo

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import DUNS
from dataactbroker.helpers.generic_helper import batch, RETRY_REQUEST_EXCEPTIONS
from dataactvalidator.scripts.loader_utils import clean_data, trim_item, insert_dataframe
from dataactcore.models.lookups import DUNS_BUSINESS_TYPE_DICT
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa

logger = logging.getLogger(__name__)

DUNS_COLUMNS = [col.key for col in DUNS.__table__.columns]
EXCLUDE_FROM_API = ['registration_date', 'expiration_date', 'last_sam_mod_date', 'activation_date',
                    'legal_business_name', 'historic', 'created_at', 'updated_at', 'duns_id', 'deactivation_date',
                    'last_exec_comp_mod_date']
LOAD_BATCH_SIZE = 10000

# SAM's Rate Limit is 10k requests/day
RATE_LIMIT_CALLS = 10000
RATE_LIMIT_PERIOD = 24 * 60 * 60  # seconds


def clean_sam_data(data):
    """ Wrapper around clean_data with the DUNS context

        Args:
            data: the dataframe to be cleaned

        Returns:
            a cleaned/updated dataframe to be imported
    """
    if not data.empty:
        column_mappings = {col: col for col in data.columns}
        return clean_data(data, DUNS, column_mappings, {})
    return data


def parse_duns_file(file_path, metrics=None):
    """ Takes in a DUNS file and adds the DUNS data to the database

        Args:
            file_path: the path to the SAM file
            metrics: dictionary representing metrics data for the load

        Returns:
            dataframe representing the contents in the file
    """
    if not metrics:
        metrics = {
            'files_processed': [],
            'records_received': 0,
            'records_processed': 0,
            'adds_received': 0,
            'updates_received': 0,
            'deletes_received': 0
        }

    logger.info("Starting file " + str(file_path))

    file_name = os.path.splitext(os.path.basename(file_path))[0]
    dat_file_name = file_name + '.dat'
    file_name_props = file_name.split('_')
    dat_file_date = file_name_props[-1]
    version = 'v2' if 'V2' in file_name else 'v1'
    period = file_name_props[3]

    zfile = zipfile.ZipFile(file_path)

    v1_column_header_mapping = {
        "awardee_or_recipient_uniqu": 0,
        "sam_extract_code": 4,
        "registration_date": 6,
        "expiration_date": 7,
        "last_sam_mod_date": 8,
        "activation_date": 9,
        "legal_business_name": 10,
        "dba_name": 11,
        "address_line_1": 14,
        "address_line_2": 15,
        "city": 16,
        "state": 17,
        "zip": 18,
        "zip4": 19,
        "country_code": 20,
        "congressional_district": 21,
        "entity_structure": 27,
        "business_types_raw": 31,
        "ultimate_parent_legal_enti": 186,
        "ultimate_parent_unique_ide": 187
    }
    v2_column_header_mapping = {
        "uei": 0,
        "awardee_or_recipient_uniqu": 1,
        "sam_extract_code": 5,
        "registration_date": 7,
        "expiration_date": 8,
        "last_sam_mod_date": 9,
        "activation_date": 10,
        "legal_business_name": 11,
        "dba_name": 12,
        "address_line_1": 15,
        "address_line_2": 16,
        "city": 17,
        "state": 18,
        "zip": 19,
        "zip4": 20,
        "country_code": 21,
        "congressional_district": 22,
        "entity_structure": 29,
        "business_types_raw": 33,
        "ultimate_parent_legal_enti": 199,
        "ultimate_parent_uei": 200,
        "ultimate_parent_unique_ide": 201
    }
    column_header_mapping = v1_column_header_mapping if version == 'v1' else v2_column_header_mapping
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))

    nrows = 0
    with zfile.open(dat_file_name) as dat_file:
        nrows = len(dat_file.readlines()) - 2  # subtract the header and footer
    with zfile.open(dat_file_name) as dat_file:
        csv_data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=1, nrows=nrows, sep='|',
                               usecols=column_header_mapping_ordered.values(),
                               names=column_header_mapping_ordered.keys(), quoting=3)
    total_data = csv_data.copy()
    rows_received = len(total_data.index)
    logger.info('%s DUNS records received', rows_received)

    total_data = total_data[total_data['awardee_or_recipient_uniqu'].notnull()]
    rows_processed = len(total_data.index)

    if version == 'v1':
        total_data = total_data.assign(uei=np.nan, ultimate_parent_uei=np.nan)

    # trimming all columns before cleaning to ensure the sam_extract is working as intended
    total_data = total_data.applymap(lambda x: trim_item(x) if len(str(x).strip()) else None)

    # add deactivation_date column for delete records
    lambda_func = (lambda sam_extract: pd.Series([dat_file_date if sam_extract == "1" else np.nan]))
    total_data = total_data.assign(deactivation_date=total_data["sam_extract_code"].apply(lambda_func))
    # convert business types string to array
    bt_func = (lambda bt_raw: pd.Series([[str(code).strip() for code in str(bt_raw).split('~')
                                          if isinstance(bt_raw, str)]]))
    total_data = total_data.assign(business_types_codes=total_data["business_types_raw"].apply(bt_func))
    bt_str_func = (lambda bt_codes: pd.Series([[DUNS_BUSINESS_TYPE_DICT[code] for code in bt_codes
                                                if code in DUNS_BUSINESS_TYPE_DICT]]))
    total_data = total_data.assign(business_types=total_data["business_types_codes"].apply(bt_str_func))
    del total_data["business_types_raw"]

    relevant_data = total_data[total_data['sam_extract_code'].isin(['A', 'E', '1', '2', '3'])]
    # order by sam to exclude deletes befores adds/updates when dropping duplicates
    relevant_data.sort_values(by=['sam_extract_code'], inplace=True)
    # drop DUNS duplicates, taking only the last one for dailies, first one for monthlies
    keep = 'first' if period == 'MONTHLY' else 'last'
    relevant_data.drop_duplicates(subset=['awardee_or_recipient_uniqu'], keep=keep, inplace=True)

    delete_data = relevant_data[relevant_data['sam_extract_code'] == '1']
    deletes_received = len(delete_data.index)
    add_data = relevant_data[relevant_data['sam_extract_code'].isin(['A', 'E', '2'])]
    adds_received = len(add_data.index)
    update_data = relevant_data[relevant_data['sam_extract_code'] == '3']
    updates_received = len(update_data.index)
    add_update_data = relevant_data[relevant_data['sam_extract_code'].isin(['A', 'E', '2', '3'])]
    del add_update_data["sam_extract_code"]
    del delete_data["sam_extract_code"]

    # cleaning and replacing NaN/NaT with None's
    add_update_data = clean_sam_data(add_update_data)
    delete_data = clean_sam_data(delete_data)

    metrics['files_processed'].append(dat_file_name)
    metrics['records_received'] += rows_received
    metrics['records_processed'] += rows_processed
    metrics['adds_received'] += adds_received
    metrics['updates_received'] += updates_received
    metrics['deletes_received'] += deletes_received

    return add_update_data, delete_data


def create_temp_duns_table(sess, table_name, data):
    """ Creates a temporary duns table with the given name and data.

        Args:
            sess: database connection
            table_name: what to name the table being created
            data: pandas dataframe representing duns data
    """
    logger.info('Making {} table'.format(table_name))
    column_types = {
        'created_at': 'TIMESTAMP WITHOUT TIME ZONE',
        'updated_at': 'TIMESTAMP WITHOUT TIME ZONE',
        'uei': 'TEXT',
        'awardee_or_recipient_uniqu': 'TEXT',
        'activation_date': 'DATE',
        'expiration_date': 'DATE',
        'deactivation_date': 'DATE',
        'registration_date': 'DATE',
        'last_sam_mod_date': 'DATE',
        'legal_business_name': 'TEXT',
        'dba_name': 'TEXT',
        'ultimate_parent_uei': 'TEXT',
        'ultimate_parent_unique_ide': 'TEXT',
        'ultimate_parent_legal_enti': 'TEXT',
        'address_line_1': 'TEXT',
        'address_line_2': 'TEXT',
        'city': 'TEXT',
        'state': 'TEXT',
        'zip': 'TEXT',
        'zip4': 'TEXT',
        'country_code': 'TEXT',
        'congressional_district': 'TEXT',
        'business_types_codes': 'TEXT[]',
        'business_types': 'TEXT[]',
        'entity_structure': 'TEXT',
        'high_comp_officer1_amount': 'TEXT',
        'high_comp_officer1_full_na': 'TEXT',
        'high_comp_officer2_amount': 'TEXT',
        'high_comp_officer2_full_na': 'TEXT',
        'high_comp_officer3_amount': 'TEXT',
        'high_comp_officer3_full_na': 'TEXT',
        'high_comp_officer4_amount': 'TEXT',
        'high_comp_officer4_full_na': 'TEXT',
        'high_comp_officer5_amount': 'TEXT',
        'high_comp_officer5_full_na': 'TEXT',
        'last_exec_comp_mod_date': 'DATE'
    }
    columns = ', '.join(['{} {}'.format(column_name, column_type) for column_name, column_type in column_types.items()
                         if column_name in list(data.columns)])
    create_table_sql = 'CREATE TABLE IF NOT EXISTS {} ({});'.format(table_name, columns)
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE {};'.format(table_name))
    insert_dataframe(data, table_name, sess.connection())


def update_duns(sess, duns_data, table_name='duns', metrics=None, deletes=False):
    """ Takes in a dataframe of duns and adds/updates associated DUNS

        Args:
            sess: database connection
            duns_data: pandas dataframe representing duns data
            table_name: the table to update ('duns', 'historic_duns')
            metrics: dictionary representing metrics of the script
            deletes: whether the data provided contains only delete records

        Returns:
            list of DUNS updated
    """
    if table_name not in ('duns', 'historic_duns'):
        raise ValueError('table argument must be \'duns\' or \'historic_duns\'')
    if not metrics:
        metrics = {
            'added_duns': [],
            'updated_duns': []
        }

    tmp_name = 'temp_duns_update' if table_name == 'duns' else 'temp_historic_duns_update'
    tmp_abbr = 'tdu' if table_name == 'duns' else 'thdu'
    create_temp_duns_table(sess, tmp_name, duns_data)

    logger.info('Getting list of DUNS that will be added/updated for metrics')
    insert_sql = """
        SELECT {tmp_abbr}.awardee_or_recipient_uniqu
        FROM {tmp_name} AS {tmp_abbr}
        LEFT JOIN {table_name} ON {table_name}.awardee_or_recipient_uniqu={tmp_abbr}.awardee_or_recipient_uniqu
        WHERE {table_name}.awardee_or_recipient_uniqu IS NULL;
    """.format(tmp_name=tmp_name, tmp_abbr=tmp_abbr, table_name=table_name)
    added_duns_list = [row['awardee_or_recipient_uniqu'] for row in sess.execute(insert_sql).fetchall()]
    update_sql = """
        SELECT {table_name}.awardee_or_recipient_uniqu
        FROM {table_name}
        JOIN {tmp_name} AS {tmp_abbr} ON {table_name}.awardee_or_recipient_uniqu={tmp_abbr}.awardee_or_recipient_uniqu;
    """.format(tmp_name=tmp_name, tmp_abbr=tmp_abbr, table_name=table_name)
    updated_duns_list = [row['awardee_or_recipient_uniqu'] for row in sess.execute(update_sql).fetchall()]

    logger.info('Adding/updating DUNS based on {}'.format(tmp_name))
    if deletes:
        update_cols = ['deactivation_date = {tmp_abbr}.deactivation_date'.format(tmp_abbr=tmp_abbr)]
    else:
        update_cols = ['{col} = {tmp_abbr}.{col}'.format(col=col, tmp_abbr=tmp_abbr)
                       for col in list(duns_data.columns)
                       if col not in ['created_at', 'updated_at', 'deactivation_date', 'awardee_or_recipient_uniqu']]
        if table_name == 'duns':
            update_cols.append('historic = FALSE')
    update_cols.append('updated_at = NOW()')
    update_cols = ', '.join(update_cols)
    update_sql = """
        UPDATE {table_name}
        SET
            {update_cols}
        FROM {tmp_name} AS {tmp_abbr}
        WHERE {tmp_abbr}.awardee_or_recipient_uniqu = {table_name}.awardee_or_recipient_uniqu;
    """.format(table_name=table_name, update_cols=update_cols, tmp_name=tmp_name, tmp_abbr=tmp_abbr)
    sess.execute(update_sql)

    insert_cols = ', '.join(list(duns_data.columns))
    insert_historic = ('historic,', 'FALSE,') if table_name == 'duns' else ('', '')
    insert_sql = """
        INSERT INTO {table_name} (
            {historic_col}
            {insert_cols}
        )
        SELECT
            {historic_val}
            {insert_cols}
        FROM {tmp_name} AS {tmp_abbr}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {table_name}
            WHERE {table_name}.awardee_or_recipient_uniqu = {tmp_abbr}.awardee_or_recipient_uniqu
        );
    """.format(table_name=table_name, insert_cols=insert_cols, tmp_name=tmp_name, tmp_abbr=tmp_abbr,
               historic_col=insert_historic[0], historic_val=insert_historic[1])
    sess.execute(insert_sql)

    logger.info('Dropping {}'.format(tmp_name))
    sess.execute('DROP TABLE {};'.format(tmp_name))

    sess.commit()

    metrics['added_duns'].extend(added_duns_list)
    metrics['updated_duns'].extend(updated_duns_list)


def parse_exec_comp_file(file_path, metrics=None):
    """ Parses the executive compensation file to update corresponding DUNS records

        Args:
            file_path: the path to the SAM file
            metrics: dictionary representing metrics of the script

        Raises:
            Exception: couldn't extract the last exec comp modification date, this generally means the filename provided
                doesn't match the expected format.

        Returns:
            dataframe representing the contents in the file
    """
    if not metrics:
        metrics = {
            'files_processed': [],
            'records_received': 0,
            'records_processed': 0
        }
    logger.info('Starting file ' + file_path)

    file_name = os.path.splitext(os.path.basename(file_path))[0]
    dat_file_name = file_name + '.dat'
    file_name_props = file_name.split('_')
    dat_file_date = file_name_props[-1]
    period = file_name_props[3]

    zfile = zipfile.ZipFile(file_path)

    # It's the same column mapping between the versions
    column_header_mapping = {
        'awardee_or_recipient_uniqu': 0,
        'sam_extract_code': 4,
        'exec_comp_str': 89
    }
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))

    # can't use skipfooter, pandas' c engine doesn't work with skipfooter and the python engine doesn't work with dtype
    nrows = 0
    with zfile.open(dat_file_name) as dat_file:
        nrows = len(dat_file.readlines()) - 2  # subtract the header and footer
    with zfile.open(dat_file_name) as dat_file:
        csv_data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=1, nrows=nrows, sep='|',
                               usecols=column_header_mapping_ordered.values(),
                               names=column_header_mapping_ordered.keys(), quoting=3)
    total_data = csv_data.copy()
    records_received = len(total_data.index)
    total_data = total_data[total_data['awardee_or_recipient_uniqu'].notnull()
                            & total_data['sam_extract_code'].isin(['2', '3', 'A', 'E'])]
    records_processed = len(total_data.index)
    del total_data['sam_extract_code']

    # drop DUNS duplicates, taking only the last one
    keep = 'first' if period == 'MONTHLY' else 'last'
    total_data.drop_duplicates(subset=['awardee_or_recipient_uniqu'], keep=keep, inplace=True)

    # Note: we're splitting these up cause it vastly saves memory parsing only the records that are populated
    blank_exec = total_data[total_data['exec_comp_str'].isnull()]
    pop_exec = total_data[total_data['exec_comp_str'].notnull()]

    # parse out executive compensation from row 90 for populated records
    if not pop_exec.empty:
        lambda_func = (lambda ecs: pd.Series(list(parse_exec_comp(ecs).values())))
        parsed_data = pop_exec['exec_comp_str'].apply(lambda_func)
        parsed_data.columns = list(parse_exec_comp().keys())
        pop_exec = pd.concat([pop_exec, parsed_data], axis=1)
    else:
        pop_exec = pop_exec.assign(**parse_exec_comp())

    # leave blanks
    blank_exec = blank_exec.assign(**parse_exec_comp())

    # setup the final dataframe
    total_data = pd.concat([pop_exec, blank_exec])
    del total_data['exec_comp_str']
    total_data.replace('', np.nan, inplace=True)
    last_exec_comp_mod_date = datetime.datetime.strptime(dat_file_date, '%Y%m%d').date()
    total_data = total_data.assign(last_exec_comp_mod_date=last_exec_comp_mod_date)

    # Cleaning out any untrimmed strings
    if not total_data.empty:
        total_data = clean_data(total_data, DUNS, {
            'awardee_or_recipient_uniqu': 'awardee_or_recipient_uniqu',
            'high_comp_officer1_amount': 'high_comp_officer1_amount',
            'high_comp_officer1_full_na': 'high_comp_officer1_full_na',
            'high_comp_officer2_amount': 'high_comp_officer2_amount',
            'high_comp_officer2_full_na': 'high_comp_officer2_full_na',
            'high_comp_officer3_amount': 'high_comp_officer3_amount',
            'high_comp_officer3_full_na': 'high_comp_officer3_full_na',
            'high_comp_officer4_amount': 'high_comp_officer4_amount',
            'high_comp_officer4_full_na': 'high_comp_officer4_full_na',
            'high_comp_officer5_amount': 'high_comp_officer5_amount',
            'high_comp_officer5_full_na': 'high_comp_officer5_full_na',
            'last_exec_comp_mod_date': 'last_exec_comp_mod_date'
        }, {})
        total_data.drop(columns=['created_at', 'updated_at'], inplace=True)

    metrics['files_processed'].append(dat_file_name)
    metrics['records_received'] += records_received
    metrics['records_processed'] += records_processed

    return total_data


def parse_exec_comp(exec_comp_str=None):
    """ Parses the executive compensation string into a dictionary of exec comp data

        Args:
            exec_comp_str: the incoming compensation string

        Returns:
            dictionary of exec comp data
    """
    exec_comp_data = OrderedDict()
    for index in range(1, 6):
        exec_comp_data['high_comp_officer{}_full_na'.format(index)] = np.nan
        exec_comp_data['high_comp_officer{}_amount'.format(index)] = np.nan

    if isinstance(exec_comp_str, str) and not exec_comp_str.isdigit():
        high_comp_officers = exec_comp_str.split('~')

        # records have inconsistent values for Null
        # 'see note above' is excluded as it may contain relevant info
        unaccepted_titles = ['x', 'n/a', 'na', 'null', 'none', '. . .', 'no one', 'no  one']

        for index, high_comp_officer in enumerate(high_comp_officers):
            index += 1
            exec_comp_split = high_comp_officer.split('^')
            if len(exec_comp_split) != 3:
                continue
            exec_name = exec_comp_split[0]
            exec_title = exec_comp_split[1]
            exec_comp = exec_comp_split[2]
            if exec_title.lower() not in unaccepted_titles and exec_name.lower() not in unaccepted_titles:
                exec_comp_data['high_comp_officer{}_full_na'.format(index)] = exec_name
                exec_comp_data['high_comp_officer{}_amount'.format(index)] = exec_comp

    return exec_comp_data


def update_missing_parent_names(sess, updated_date=None):
    """ Updates DUNS rows in batches where the parent DUNS number is provided but not the parent name.
        Uses other instances of the parent DUNS number where the name is populated to derive blank parent names.
        Updated_date argument used for daily DUNS loads so that only data updated that day is updated.

        Args:
            sess: the database connection
            updated_date: the date to start importing from

        Returns:
            number of DUNS updated
    """
    logger.info("Updating missing parent names")

    # Create a mapping of all the unique parent duns -> name mappings from the database
    parent_duns_by_number_name = {}

    distinct_parent_duns = sess.query(DUNS.ultimate_parent_unique_ide, DUNS.ultimate_parent_legal_enti)\
        .filter(and_(func.coalesce(DUNS.ultimate_parent_legal_enti, '') != '',
                     DUNS.ultimate_parent_unique_ide.isnot(None))).distinct()

    # Creating a mapping (parent_duns_by_number_name) of parent duns numbers to parent name
    for duns in distinct_parent_duns:
        if parent_duns_by_number_name.get(duns.ultimate_parent_unique_ide):
            # Do not want to deal with parent ids with multiple names
            del parent_duns_by_number_name[duns.ultimate_parent_unique_ide]

        parent_duns_by_number_name[duns.ultimate_parent_unique_ide] = duns.ultimate_parent_legal_enti

    # Query to find rows where the parent duns number is present, but there is no legal entity name
    missing_parent_name = sess.query(DUNS).filter(and_(func.coalesce(DUNS.ultimate_parent_legal_enti, '') == '',
                                                       DUNS.ultimate_parent_unique_ide.isnot(None)))

    if updated_date:
        missing_parent_name = missing_parent_name.filter(DUNS.updated_at >= updated_date)

    missing_count = missing_parent_name.count()

    batch = 0
    block_size = 10000
    batches = missing_count // block_size
    total_updated_count = 0

    while batch <= batches:
        updated_count = 0
        start = time.time()
        batch_start = batch * block_size
        logger.info("Processing row {} - {} with missing parent duns name"
                    .format(str(batch * block_size + 1),
                            str(missing_count if batch == batches else (batch + 1) * block_size)
                            ))

        missing_parent_name_block = missing_parent_name.order_by(DUNS.duns_id).\
            slice(batch_start, batch_start + block_size)

        for row in missing_parent_name_block:
            if parent_duns_by_number_name.get(row.ultimate_parent_unique_ide):
                setattr(row, 'ultimate_parent_legal_enti', parent_duns_by_number_name[row.ultimate_parent_unique_ide])
                updated_count += 1

        logger.info("Updated {} rows in {} with the parent name in {} s".format(updated_count, DUNS.__name__,
                                                                                time.time() - start))
        total_updated_count += updated_count

        batch += 1

    sess.commit()
    return total_updated_count


def request_sam_entity_api(duns_list):
    """ Calls the SAM entity API to retrieve SAM data by the DUNS numbers provided.

        Args:
            duns_list: list of DUNS to search

        Returns:
            json list of SAM objects representing entities
    """
    params = {
        'sensitivity': 'fouo',
        'ueiDUNS': '[{}]'.format('~'.join(duns_list))
    }
    content = _request_sam_api(CONFIG_BROKER['sam']['duns']['entity_api_url'], request_type='post', accept_type='json',
                               params=params)
    return json.loads(content)['entityData']


def request_sam_csv_api(root_dir, file_name):
    """ Downloads the requested csv from the SAM CSV API

        Args:
            root_dir: where to download the file
            file_name: the name of the file to download
    """
    logger.info('Downloading the following DUNS file: {}'.format(file_name))
    local_sam_file = os.path.join(root_dir, file_name)
    params = {
        'fileName': file_name
    }
    file_content = _request_sam_api(CONFIG_BROKER['sam']['duns']['csv_api_url'], request_type='get', accept_type='zip',
                                    params=params)
    open(local_sam_file, 'wb').write(file_content)


def is_nonexistent_file_error(e):
    """ Differentiates between an abnormal connection issue or the file not existing

        Args:
            e: the HTTP exception to analyze

        Returns:
            bool whether the error is a nonexistent file http error
    """
    no_file_msg = 'The File does not exist with the provided parameters.'
    return e.response is not None and (json.loads(e.response.content).get('detail') == no_file_msg)


def give_up(e):
    """ Determines whether to give up retrying the request, sleeps if rate limiting

        Args:
            e: the HTTP exception to analyze

        Returns:
            bool whether to stop retrying a specified request
    """
    if is_nonexistent_file_error(e):
        return True
    if e.response is not None and e.response.status_code == 429:
        # Seem like the rate limit resets at midnight, so wait until then
        now = datetime.datetime.now()
        tomorrow = now + datetime.timedelta(days=1)
        wait = (datetime.datetime.combine(tomorrow, datetime.time.min) - now).seconds
        logger.info('Rate limit hit. Sleeping for {} seconds until the next day where we can continue.'.format(wait))
        time.sleep(wait)
    return False


@limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
@sleep_and_retry
@on_exception(expo, RETRY_REQUEST_EXCEPTIONS, max_tries=10, logger=logger, giveup=give_up)
def _request_sam_api(url, request_type, accept_type, params=None, body=None):
    """ Calls one of the SAM APIs and returns its content

        Args:
            url: the url to request
            request_type: the REST type, get or post
            accept_type: what type of data to accept (either zip or json)
            params: query filters to use for the API
            body: json filters to use for the API

        Returns:
            the response's content

        Raises:
            ConnectionError if it's an unsuccessful request
    """
    if request_type not in ['get', 'post']:
        return ValueError('request_type must be \'get\' or \'post\'')
    if accept_type not in ['zip', 'json']:
        return ValueError('accept_type must be \'zip\' or \'json\'')
    auth = (CONFIG_BROKER['sam']['account_user_id'], CONFIG_BROKER['sam']['account_password'])
    headers = {
        'x-api-key': CONFIG_BROKER['sam']['api_key'],
        'Accept': 'application/{}'.format(accept_type),
        'Content-Type': 'application/json'
    }
    r = requests.request(request_type.upper(), url, headers=headers, params=params, json=json.dumps(body), auth=auth,
                         timeout=60)
    # raise for server HTTP errors (requests.exceptions.HTTPError) asides from connection issues
    r.raise_for_status()
    return r.content


def get_duns_props_from_sam(duns_list):
    """ Calls SAM API to retrieve DUNS data by DUNS number. Returns relevant DUNS info as Data Frame

        Args:
            duns_list: list of DUNS to search

        Returns:
            dataframe representing the DUNS props
    """
    duns_props_mappings = {
        'awardee_or_recipient_uniqu': 'entityRegistration.ueiDUNS',
        'uei': 'entityRegistration.ueiSAM',
        'legal_business_name': 'entityRegistration.legalBusinessName',
        'dba_name': 'entityRegistration.dbaName',
        'entity_structure': 'coreData.generalInformation.entityStructureCode',
        'ultimate_parent_unique_ide': 'coreData.entityHierarchyInformation.ultimateParentEntity.ueiDUNS',
        'ultimate_parent_uei': 'coreData.entityHierarchyInformation.ultimateParentEntity.ueiSAM',
        'ultimate_parent_legal_enti': 'coreData.entityHierarchyInformation.ultimateParentEntity.legalBusinessName',
        'address_line_1': 'coreData.physicalAddress.addressLine1',
        'address_line_2': 'coreData.physicalAddress.addressLine2',
        'city': 'coreData.physicalAddress.city',
        'state': 'coreData.physicalAddress.stateOrProvince',
        'zip': 'coreData.physicalAddress.zipCode',
        'zip4': 'coreData.physicalAddress.zipCodePlus4',
        'country_code': 'coreData.physicalAddress.countryCode',
        'congressional_district': 'coreData.congressionalDistrict',
        'business_types_codes': 'coreData.businessTypes',
        'executive_comp_data': 'coreData.executiveCompensationInformation'
    }
    duns_props = []
    for duns_obj in request_sam_entity_api(duns_list):
        duns_props_dict = {}
        for duns_props_name, duns_prop_path in duns_props_mappings.items():
            nested_obj = duns_obj
            value = None
            for nested_layer in duns_prop_path.split('.'):
                nested_obj = nested_obj.get(nested_layer, None)
                if not nested_obj:
                    break
                elif nested_layer == duns_prop_path.split('.')[-1]:
                    value = nested_obj
            if duns_props_name == 'business_types_codes':
                value = [busi_type['businessTypeCode'] for busi_type in nested_obj.get('businessTypeList', [])]
                duns_props_dict['business_types'] = [DUNS_BUSINESS_TYPE_DICT[type] for type in value
                                                     if type in DUNS_BUSINESS_TYPE_DICT]
            if duns_props_name == 'executive_comp_data':
                for index in range(1, 6):
                    duns_props_dict['high_comp_officer{}_full_na'.format(index)] = None
                    duns_props_dict['high_comp_officer{}_amount'.format(index)] = None
                if nested_obj:
                    for index, exec_comp in enumerate(nested_obj.get('listOfExecutiveCompensation', []), start=1):
                        if exec_comp['execName'] is not None:
                            duns_props_dict['high_comp_officer{}_full_na'.format(index)] = exec_comp['execName']
                            duns_props_dict['high_comp_officer{}_amount'.format(index)] = \
                                exec_comp['compensationAmount']
                continue
            duns_props_dict[duns_props_name] = value
        duns_props.append(duns_props_dict)
    return pd.DataFrame(duns_props)


def update_duns_props(df):
    """ Returns same dataframe with extraneous data updated"

        Args:
            df: the dataframe containing the duns data

        Returns:
            a merged dataframe with the duns updated with extraneous info from SAM's individual DUNS API
    """
    request_cols = [col for col in DUNS_COLUMNS if col not in EXCLUDE_FROM_API]
    empty_row_template = {request_col: None for request_col in request_cols}
    for array_col in ['business_types_codes', 'business_types']:
        empty_row_template[array_col] = []
    prefilled_cols = [col for col in list(df.columns) if col not in ['awardee_or_recipient_uniqu']]

    all_duns = df['awardee_or_recipient_uniqu'].tolist()
    duns_props_df = pd.DataFrame(columns=request_cols)
    # SAM service only takes in batches of 100
    index = 0
    batch_size = 100
    for duns_list in batch(all_duns, batch_size):
        logger.info('Gathering data for the following DUNS: {}'.format(duns_list))
        duns_props_batch = get_duns_props_from_sam(duns_list)
        duns_props_batch.drop(prefilled_cols, axis=1, inplace=True, errors='ignore')
        # Adding in blank rows for DUNS where data was not found
        added_duns_list = []
        if not duns_props_batch.empty:
            added_duns_list = [str(duns) for duns in duns_props_batch['awardee_or_recipient_uniqu'].tolist()]
        logger.info('Retrieved data for DUNS records: {}'.format(added_duns_list))
        empty_duns_rows = []
        for duns in (set(added_duns_list) ^ set(duns_list)):
            empty_duns_row = empty_row_template.copy()
            empty_duns_row['awardee_or_recipient_uniqu'] = duns
            empty_duns_rows.append(empty_duns_row)
        duns_props_batch = duns_props_batch.append(pd.DataFrame(empty_duns_rows), sort=True)
        duns_props_df = duns_props_df.append(duns_props_batch, sort=True)
        index += batch_size
    return pd.merge(df, duns_props_df, on=['awardee_or_recipient_uniqu'])


def backfill_uei(sess, table):
    """ Backfill any extraneous data (ex. uei) missing from V1 data that wasnt updated by V2

        Args:
            sess: database connection
            table: table to backfill
    """
    duns_to_update = sess.query(table.awardee_or_recipient_uniqu).filter_by(uei=None, ultimate_parent_uei=None).all()
    for duns_batch in batch(duns_to_update, LOAD_BATCH_SIZE):
        df = pd.DataFrame(columns=['awardee_or_recipient_uniqu'])
        df = df.append(duns_batch)
        df = update_duns_props(df)
        df = df[['awardee_or_recipient_uniqu', 'uei', 'ultimate_parent_uei']]
        update_duns(sess, df, table_name=table.__table__.name)
