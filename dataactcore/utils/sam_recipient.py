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

from dataactbroker.helpers.generic_helper import batch, RETRY_REQUEST_EXCEPTIONS
from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import SAMRecipient, SAMRecipientUnregistered
from dataactcore.interfaces.function_bag import get_utc_now
from dataactcore.models.lookups import SAM_BUSINESS_TYPE_DICT
from dataactcore.models.jobModels import Submission # noqa
from dataactcore.models.userModel import User # noqa
from dataactcore.utils.loader_utils import clean_data, trim_item, insert_dataframe

logger = logging.getLogger(__name__)

SAM_COLUMNS = [col.key for col in SAMRecipient.__table__.columns]
SAM_ENTITY_MAPPINGS = {
    'entityRegistration.ueiDUNS': 'awardee_or_recipient_uniqu',
    'entityRegistration.ueiSAM': 'uei',
    'entityRegistration.legalBusinessName': 'legal_business_name',
    'entityRegistration.dbaName': 'dba_name',
    'coreData.generalInformation.entityStructureCode': 'entity_structure',
    'coreData.entityHierarchyInformation.ultimateParentEntity.ueiDUNS': 'ultimate_parent_unique_ide',
    'coreData.entityHierarchyInformation.ultimateParentEntity.ueiSAM': 'ultimate_parent_uei',
    'coreData.entityHierarchyInformation.ultimateParentEntity.legalBusinessName': 'ultimate_parent_legal_enti',
    'coreData.physicalAddress.addressLine1': 'address_line_1',
    'coreData.physicalAddress.addressLine2': 'address_line_2',
    'coreData.physicalAddress.city': 'city',
    'coreData.physicalAddress.stateOrProvinceCode': 'state',
    'coreData.physicalAddress.zipCode': 'zip',
    'coreData.physicalAddress.zipCodePlus4': 'zip4',
    'coreData.physicalAddress.countryCode': 'country_code',
    'coreData.congressionalDistrict.congressionalDistrict': 'congressional_district',
    'coreData.businessTypes.businessTypeList': 'business_types_codes',
    'coreData.executiveCompensationInformation': 'executive_comp_data'
}
SAM_IQAAS_MAPPINGS = {
    'ueiDUNS': 'awardee_or_recipient_uniqu',
    'ueiSAM': 'uei'
}
EXCLUDE_FROM_API = ['registration_date', 'expiration_date', 'last_sam_mod_date', 'activation_date',
                    'legal_business_name', 'historic', 'created_at', 'updated_at', 'sam_recipient_id',
                    'deactivation_date', 'last_exec_comp_mod_date']
LOAD_BATCH_SIZE = 10000

# SAM's Rate Limit is 259,200 requests/day
RATE_LIMIT_CALLS = 259000
RATE_LIMIT_PERIOD = 24 * 60 * 60  # seconds


def clean_sam_data(data):
    """ Wrapper around clean_data with the SAM Recipient context

        Args:
            data: the dataframe to be cleaned

        Returns:
            a cleaned/updated dataframe to be imported
    """
    if not data.empty:
        column_mappings = {col: col for col in data.columns}
        return clean_data(data, SAMRecipient, column_mappings, {})
    return data


def parse_sam_recipient_file(file_path, metrics=None):
    """ Takes in a SAMRecipient file and adds the SAMRecipient data to the database

        Args:
            file_path: the path to the SAM file
            metrics: dictionary representing metrics data for the load

        Returns:
            dataframes representing the contents in the file
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
    key_col = 'awardee_or_recipient_uniqu' if version == 'v1' else 'uei'

    nrows = 0
    with zfile.open(dat_file_name) as dat_file:
        nrows = len(dat_file.readlines()) - 2  # subtract the header and footer
    with zfile.open(dat_file_name) as dat_file:
        csv_data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=1, nrows=nrows, sep='|',
                               usecols=column_header_mapping_ordered.values(),
                               names=column_header_mapping_ordered.keys(), quoting=3)
    total_data = csv_data.copy()
    rows_received = len(total_data.index)
    logger.info('%s SAM Recipient records received', rows_received)

    total_data = total_data[total_data[key_col].notnull()]
    rows_processed = len(total_data.index)

    if version == 'v1':
        total_data = total_data.assign(uei=np.nan, ultimate_parent_uei=np.nan)

    # trimming all columns before cleaning to ensure the sam_extract is working as intended
    total_data = total_data.map(lambda x: trim_item(x) if len(str(x).strip()) else None)

    # add deactivation_date column for delete records
    lambda_func = (lambda sam_extract: pd.Series([dat_file_date if sam_extract == "1" else np.nan]))
    total_data = total_data.assign(deactivation_date=total_data["sam_extract_code"].apply(lambda_func))
    # convert business types string to array
    bt_func = (lambda bt_raw: pd.Series([[str(code).strip() for code in str(bt_raw).split('~')
                                          if isinstance(bt_raw, str)]]))
    total_data = total_data.assign(business_types_codes=total_data["business_types_raw"].apply(bt_func))
    bt_str_func = (lambda bt_codes: pd.Series([[SAM_BUSINESS_TYPE_DICT[code] for code in bt_codes
                                                if code in SAM_BUSINESS_TYPE_DICT]]))
    total_data = total_data.assign(business_types=total_data["business_types_codes"].apply(bt_str_func))
    del total_data["business_types_raw"]

    relevant_data = total_data[total_data['sam_extract_code'].isin(['A', 'E', '1', '2', '3'])]
    # order by sam to exclude deletes befores adds/updates when dropping duplicates
    relevant_data.sort_values(by=['sam_extract_code'], inplace=True)
    # drop SAM duplicates, taking only the last one for dailies, first one for monthlies
    keep = 'first' if period == 'MONTHLY' else 'last'
    relevant_data.drop_duplicates(subset=[key_col], keep=keep, inplace=True)

    delete_data = relevant_data[relevant_data['sam_extract_code'] == '1'].copy()
    deletes_received = len(delete_data.index)
    add_data = relevant_data[relevant_data['sam_extract_code'].isin(['A', 'E', '2'])]
    adds_received = len(add_data.index)
    update_data = relevant_data[relevant_data['sam_extract_code'] == '3']
    updates_received = len(update_data.index)
    add_update_data = relevant_data[relevant_data['sam_extract_code'].isin(['A', 'E', '2', '3'])].copy()
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


def create_temp_sam_recipient_table(sess, table_name, data):
    """ Creates a temporary SAM table with the given name and data.

        Args:
            sess: database connection
            table_name: what to name the table being created
            data: pandas dataframe representing SAM data
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


def update_sam_recipient(sess, sam_recipient_data, table_name='sam_recipient', metrics=None, deletes=False,
                         includes_uei=True):
    """ Takes in a dataframe of sam_recipient data and adds/updates associated SAMRecipient/HistoricalDUNS table

        Args:
            sess: database connection
            sam_recipient_data: pandas dataframe representing sam_recipient data
            table_name: the table to update (ex. 'sam_recipient', 'historic_duns')
            metrics: dictionary representing metrics of the script
            deletes: whether the data provided contains only delete records
            includes_uei: whether or not the dataframe includes uei

        Returns:
            list of UEI updated
    """
    if not metrics:
        metrics = {
            'added_uei': [],
            'updated_uei': []
        }

    key_cols = ['awardee_or_recipient_uniqu']
    if includes_uei:
        key_cols.append('uei')

        # SAM V2 files at one point have both DUNS and UEI populated. After a point, only the UEI is populated.
        # This ensures that DUNS doesn't get overwritten with blank values in that case.
        if sam_recipient_data['awardee_or_recipient_uniqu'].dropna().empty:
            sam_recipient_data.drop(columns=['awardee_or_recipient_uniqu'], axis=1, inplace=True)
            key_cols.remove('awardee_or_recipient_uniqu')

        # Also ensuring that if parent DUNS is empty, it's not overwritten
        if 'ultimate_parent_unique_ide' in list(sam_recipient_data.columns)\
                and sam_recipient_data['ultimate_parent_unique_ide'].dropna().empty:
            sam_recipient_data.drop(columns=['ultimate_parent_unique_ide'], axis=1, inplace=True)

    tmp_name = 'temp_{}_update'.format(table_name)
    tmp_abbr = 'tu'
    create_temp_sam_recipient_table(sess, tmp_name, sam_recipient_data)

    logger.info('Getting list of recipients that will be added/updated for metrics')
    join_condition = ' OR '.join(['{table_name}.{key_col} = {tmp_abbr}.{key_col}'.format(table_name=table_name,
                                                                                         tmp_abbr=tmp_abbr,
                                                                                         key_col=key_col)
                                  for key_col in key_cols])
    null_condition = ' AND '.join(['{table_name}.{key_col} IS NULL'.format(table_name=table_name,
                                                                           key_col=key_col)
                                  for key_col in key_cols])
    insert_cols = ', '.join(['{tmp_abbr}.{key_col}'.format(tmp_abbr=tmp_abbr, key_col=key_col)
                             if key_col in key_cols else 'NULL AS \"{key_col}\"'.format(key_col=key_col)
                             for key_col in ['awardee_or_recipient_uniqu', 'uei']])
    insert_sql = """
        SELECT {insert_cols}
        FROM {tmp_name} AS {tmp_abbr}
        LEFT JOIN {table_name} ON ({join_condition})
        WHERE ({null_condition});
    """.format(insert_cols=insert_cols, tmp_name=tmp_name, tmp_abbr=tmp_abbr, table_name=table_name,
               null_condition=null_condition, join_condition=join_condition)
    added_uei_list = ['{}/{}'.format(row['awardee_or_recipient_uniqu'], row['uei'])
                      for row in sess.execute(insert_sql).fetchall()]
    update_sql = """
        SELECT {table_name}.awardee_or_recipient_uniqu, {table_name}.uei
        FROM {table_name}
        JOIN {tmp_name} AS {tmp_abbr} ON ({join_condition});
    """.format(tmp_name=tmp_name, tmp_abbr=tmp_abbr, table_name=table_name, join_condition=join_condition)
    updated_uei_list = ['{}/{}'.format(row['awardee_or_recipient_uniqu'], row['uei'])
                        for row in sess.execute(update_sql).fetchall()]

    # Double checking we have a one-to-one match between the data provided and what we're adding/updating
    # Accounting for the extreme case if they provide a non-matching DUNS and UEI combo, leading us to update two values
    if len(added_uei_list) + len(updated_uei_list) != len(sam_recipient_data):
        raise ValueError('Unable to add/update sam data. A record matched on more than one recipient: {}'
                         .format(updated_uei_list))

    logger.info('Adding/updating recipients based on {}'.format(tmp_name))
    if deletes:
        update_cols = ['{col} = {tmp_abbr}.{col}'.format(col=col, tmp_abbr=tmp_abbr)
                       for col in key_cols + ['deactivation_date']]
    else:
        update_cols = ['{col} = {tmp_abbr}.{col}'.format(col=col, tmp_abbr=tmp_abbr)
                       for col in list(sam_recipient_data.columns)
                       if col not in ['created_at', 'updated_at', 'deactivation_date']]
        if table_name == 'sam_recipient':
            update_cols.append('historic = FALSE')
    if table_name in ['sam_recipient', 'historic_duns']:
        update_cols.append('updated_at = NOW()')
    update_cols = ', '.join(update_cols)
    update_sql = """
        UPDATE {table_name}
        SET
            {update_cols}
        FROM {tmp_name} AS {tmp_abbr}
        WHERE ({join_condition});
    """.format(table_name=table_name, update_cols=update_cols, tmp_name=tmp_name, tmp_abbr=tmp_abbr,
               join_condition=join_condition)
    sess.execute(update_sql)

    insert_cols = ', '.join(list(sam_recipient_data.columns))
    insert_historic = ('historic,', 'FALSE,') if table_name == 'sam_recipient' else ('', '')
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
            WHERE ({join_condition})
        );
    """.format(table_name=table_name, insert_cols=insert_cols, tmp_name=tmp_name, tmp_abbr=tmp_abbr,
               historic_col=insert_historic[0], historic_val=insert_historic[1], join_condition=join_condition)
    sess.execute(insert_sql)

    logger.info('Dropping {}'.format(tmp_name))
    sess.execute('DROP TABLE {};'.format(tmp_name))

    sess.commit()

    metrics['added_uei'].extend(added_uei_list)
    metrics['updated_uei'].extend(updated_uei_list)


def parse_exec_comp_file(file_path, metrics=None):
    """ Parses the executive compensation file to update corresponding SAM records

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
    version = 'v2' if 'V2' in file_name else 'v1'
    period = file_name_props[3]

    zfile = zipfile.ZipFile(file_path)

    v1_column_header_mapping = {
        'awardee_or_recipient_uniqu': 0,
        'sam_extract_code': 4,
        'exec_comp_str': 89
    }
    v2_column_header_mapping = {
        'uei': 0,
        'awardee_or_recipient_uniqu': 1,
        'sam_extract_code': 5,
        'exec_comp_str': 91
    }
    column_header_mapping = v1_column_header_mapping if version == 'v1' else v2_column_header_mapping
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))
    key_col = 'awardee_or_recipient_uniqu' if version == 'v1' else 'uei'

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

    # trimming all columns before cleaning to ensure the sam_extract is working as intended
    total_data = total_data.map(lambda x: trim_item(x) if len(str(x).strip()) else None)

    total_data = total_data[total_data[key_col].notnull()
                            & total_data['sam_extract_code'].isin(['2', '3', 'A', 'E'])]
    records_processed = len(total_data.index)
    del total_data['sam_extract_code']

    # drop SAM duplicates, taking only the last one
    keep = 'first' if period == 'MONTHLY' else 'last'
    total_data.drop_duplicates(subset=[key_col], keep=keep, inplace=True)

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
        exec_comp_maps = {
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
        }
        if version == 'v2':
            exec_comp_maps['uei'] = 'uei'
        total_data = clean_data(total_data, SAMRecipient, exec_comp_maps, {})
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
    """ Updates SAMRecipient rows in batches where the parent recipient uei is provided but not the parent name.
        Uses other instances of the parent recipient uei where the name is populated to derive blank parent names.
        Updated_date argument used for daily recipient loads so that only data updated that day is updated.

        Args:
            sess: the database connection
            updated_date: the date to start importing from

        Returns:
            number of recipients updated
    """
    logger.info("Updating missing parent names")

    # Create a mapping of all the unique parent recipient -> name mappings from the database
    parent_recipient_by_uei_name = {}

    distinct_parent_recipients = sess.query(SAMRecipient.ultimate_parent_uei, SAMRecipient.ultimate_parent_legal_enti)\
        .filter(and_(func.coalesce(SAMRecipient.ultimate_parent_legal_enti, '') != '',
                     SAMRecipient.ultimate_parent_uei.isnot(None))).distinct()

    # Creating a mapping (parent_recipient_by_uei_name) of parent recipient ueis to parent name
    for recipient in distinct_parent_recipients:
        if parent_recipient_by_uei_name.get(recipient.ultimate_parent_uei):
            # Do not want to deal with parent ids with multiple names
            del parent_recipient_by_uei_name[recipient.ultimate_parent_uei]

        parent_recipient_by_uei_name[recipient.ultimate_parent_uei] = recipient.ultimate_parent_legal_enti

    # Query to find rows where the parent recipient uei is present, but there is no legal entity name
    missing_parent_name = sess.query(SAMRecipient).filter(and_(
        func.coalesce(SAMRecipient.ultimate_parent_legal_enti, '') == '',
        SAMRecipient.ultimate_parent_uei.isnot(None)))

    if updated_date:
        missing_parent_name = missing_parent_name.filter(SAMRecipient.updated_at >= updated_date)

    missing_count = missing_parent_name.count()

    batch = 0
    block_size = 10000
    batches = missing_count // block_size
    total_updated_count = 0

    while batch <= batches:
        updated_count = 0
        start = time.time()
        batch_start = batch * block_size
        logger.info("Processing row {} - {} with missing parent recipient name"
                    .format(str(batch * block_size + 1),
                            str(missing_count if batch == batches else (batch + 1) * block_size)
                            ))

        missing_parent_name_block = missing_parent_name.order_by(SAMRecipient.sam_recipient_id).\
            slice(batch_start, batch_start + block_size)

        for row in missing_parent_name_block:
            if parent_recipient_by_uei_name.get(row.ultimate_parent_uei):
                setattr(row, 'ultimate_parent_legal_enti', parent_recipient_by_uei_name[row.ultimate_parent_uei])
                updated_count += 1

        logger.info("Updated {} rows in {} with the parent name in {} s".format(updated_count, SAMRecipient.__name__,
                                                                                time.time() - start))
        total_updated_count += updated_count

        batch += 1

    sess.commit()
    return total_updated_count


def request_sam_entity_api(filters, download_url=None):
    """ Calls the SAM entity API to retrieve SAM data by the filters

        Args:
            filters: dict of filters to search
            download_url: the generated download_url sent by a previous request (for csvs)

        Returns:
            json list of SAM objects representing entities,
            OR binary stream to be saved to a file
    """
    headers = {
        'x-api-key': CONFIG_BROKER['sam']['api_key'],
        'Accept': 'application/zip' if download_url else 'application/json',
        'Content-Type': 'application/json'
    }
    if not filters:
        filters = {}
    url = download_url if download_url else CONFIG_BROKER['sam']['duns']['entity_api_url']
    return _request_sam_api(url, request_type='post', headers=headers, params=filters)


def request_sam_iqaas_uei_api(filters):
    """ Calls the SAM IQaaS API to retrieve SAM UEI data by the keys provided.

        Args:
            filters: dict of filters to search

        Returns:
            json list of SAM objects representing entities
    """
    params = {
        'api_key': CONFIG_BROKER['sam']['api_key']
    }
    if not filters:
        filters = {}
    params.update(filters)
    return _request_sam_api(CONFIG_BROKER['sam']['duns']['uei_iqaas_api_url'], request_type='get', params=params)


def request_sam_extracts_api(root_dir, file_name):
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
    headers = {
        'x-api-key': CONFIG_BROKER['sam']['api_key'],
        'Accept': 'application/zip',
        'Content-Type': 'application/json'
    }
    resp = _request_sam_api(CONFIG_BROKER['sam']['duns']['csv_api_url'], request_type='get', headers=headers,
                            params=params)
    open(local_sam_file, 'wb').write(resp.content)


def is_nonexistent_file_error(e):
    """ Differentiates between an abnormal connection issue or the file not existing

        Args:
            e: the HTTP exception to analyze

        Returns:
            bool whether the error is a nonexistent file http error
    """
    no_file_msg = 'The File does not exist with the provided parameters.'
    nonexistent_file_error = False
    if e.response is not None and e.response.content is not None:
        try:
            nonexistent_file_error = (json.loads(e.response.content).get('detail') == no_file_msg)
        except json.decoder.JSONDecodeError:
            pass
    return nonexistent_file_error


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
def _request_sam_api(url, request_type, headers=None, params=None, body=None):
    """ Calls one of the SAM APIs and returns its content

        Args:
            url: the url to request
            request_type: the REST type, get or post
            headers: headers to use for the API
            params: query filters to use for the API
            body: json filters to use for the API

        Returns:
            the response's content

        Raises:
            ConnectionError if it's an unsuccessful request
    """
    if request_type not in ['get', 'post']:
        return ValueError('request_type must be \'get\' or \'post\'')
    auth = (CONFIG_BROKER['sam']['account_user_id'], CONFIG_BROKER['sam']['account_password'])
    r = requests.request(request_type.upper(), url, headers=headers, params=params, json=json.dumps(body), auth=auth,
                         timeout=60)
    # raise for server HTTP errors (requests.exceptions.HTTPError) asides from connection issues
    r.raise_for_status()
    return r


def load_unregistered_recipients(sess, df, skip_updates=False, metrics=None):
    """ Takes in the dataframe containing sam entity API data and stores it in the sam_recipient_unregistered

        Args:
            sess: database connection
            df: the dataframe to process
            skip_updates: True to skip the process of delete/update (i.e. total backfills)
            metrics: the metrics dict
    """
    # looks like the csv version drops the topmost parent section (coreData, entityRegistration)
    mapping_filtered = {k[k.index('.') + 1:]: v for k, v in SAM_ENTITY_MAPPINGS.items()}
    mapping_filtered = {k: v for k, v in mapping_filtered.items() if k in df.columns}
    df.rename(columns=mapping_filtered, inplace=True)
    df.drop([col for col in df.columns if col not in SAMRecipientUnregistered.__table__.columns], axis=1, inplace=True)

    updated_count = 0
    if not skip_updates:
        # delete any that are already in there for updating
        existing_unreg = sess.query(SAMRecipientUnregistered).filter(SAMRecipientUnregistered.uei.in_(df['uei']))
        updated_count = existing_unreg.delete()
        metrics['unregistered_updated'] += updated_count

    df['created_at'] = df['updated_at'] = get_utc_now()
    insert_dataframe(df, SAMRecipientUnregistered.__table__.name, sess.connection())
    sess.commit()
    metrics['unregistered_added'] += len(df.index) - updated_count


def get_sam_props(api='entity', **kwargs):
    """ Calls SAM API to retrieve SAM data. Returns relevant SAM info as Data Frame

        Args:
            api: which api to hit (must be 'entity' or 'iqaaas')
            kwargs: additional filters that can be passed into the request

        Returns:
            dataframe representing the SAM props
    """
    filters = kwargs if kwargs else {}
    if api == 'entity':
        request_api_method = request_sam_entity_api
        sam_props_mappings = SAM_ENTITY_MAPPINGS
        filters['sensitivity'] = 'fouo'
    elif api == 'iqaas':
        request_api_method = request_sam_iqaas_uei_api
        sam_props_mappings = SAM_IQAAS_MAPPINGS
    else:
        raise ValueError('APIs available are \'entity\' or \'iqaas\'')

    sam_props = []

    for sam_obj in json.loads(request_api_method(filters).content)['entityData']:
        sam_props_dict = {}
        for sam_prop_path, sam_props_name in sam_props_mappings.items():
            nested_obj = sam_obj
            value = None
            for nested_layer in sam_prop_path.split('.'):
                nested_obj = nested_obj.get(nested_layer, None)
                if not nested_obj:
                    break
                elif nested_layer == sam_prop_path.split('.')[-1]:
                    value = nested_obj
            if sam_props_name == 'business_types_codes':
                value = [busi_type['businessTypeCode'] for busi_type in nested_obj.get('businessTypeList', [])]
                sam_props_dict['business_types'] = [SAM_BUSINESS_TYPE_DICT[bus_type] for bus_type in value
                                                    if bus_type in SAM_BUSINESS_TYPE_DICT]
            if sam_props_name == 'executive_comp_data':
                for index in range(1, 6):
                    sam_props_dict['high_comp_officer{}_full_na'.format(index)] = None
                    sam_props_dict['high_comp_officer{}_amount'.format(index)] = None
                if nested_obj:
                    for index, exec_comp in enumerate(nested_obj.get('listOfExecutiveCompensation', []), start=1):
                        if exec_comp['execName'] is not None:
                            sam_props_dict['high_comp_officer{}_full_na'.format(index)] = exec_comp['execName']
                            sam_props_dict['high_comp_officer{}_amount'.format(index)] = \
                                exec_comp['compensationAmount']
                continue
            sam_props_dict[sam_props_name] = value
        for k, v in sam_props_dict.items():
            if isinstance(v, str) and v.lower() == 'currently not available':
                sam_props_dict[k] = None
        sam_props.append(sam_props_dict)
    return pd.DataFrame(sam_props)


def update_existing_recipients(df, api='entity'):
    """ Returns same dataframe with extraneous data updated

        Args:
            df: the dataframe containing the recipient data
            api: which api to extract the recipient data

        Returns:
            a merged dataframe with the reicpient updated with extraneous info from SAM's individual recipient API
    """
    request_cols = [col for col in SAM_COLUMNS if col not in EXCLUDE_FROM_API]
    empty_row_template = {request_col: None for request_col in request_cols}
    for array_col in ['business_types_codes', 'business_types']:
        empty_row_template[array_col] = []
    if 'uei' in list(df.columns):
        key_col = 'uei'
        includes_uei = True
    else:
        key_col = 'awardee_or_recipient_uniqu'
        includes_uei = False
    prefilled_cols = [col for col in list(df.columns) if col != key_col]

    all_keys = df[key_col].tolist()
    sam_props_df = pd.DataFrame(columns=request_cols)
    # SAM service only takes in batches of 100
    index = 0
    batch_size = 100

    for key_list in batch(all_keys, batch_size):
        logger.info('Gathering data for the following recipients: {}'.format(key_list))

        key_list_str = '[{}]'.format('~'.join(key_list)) if api == 'entity' else '{}'.format(','.join(key_list))
        key_type = 'sam' if includes_uei else 'duns'
        filters = {'uei{}'.format(key_type.upper()): key_list_str}
        sam_props_batch = get_sam_props(api=api, **filters)
        sam_props_batch.drop(prefilled_cols, axis=1, inplace=True, errors='ignore')

        # Adding in blank rows for recipients where data was not found
        added_keys_list = []
        if not sam_props_batch.empty:
            added_keys_list = [str(key) for key in sam_props_batch[key_col].tolist()]
        logger.info('Retrieved data for recipients: {}'.format(added_keys_list))

        empty_sam_rows = []
        for key in (set(added_keys_list) ^ set(key_list)):
            empty_recp_row = empty_row_template.copy()
            empty_recp_row[key_col] = key
            empty_sam_rows.append(empty_recp_row)
        sam_props_batch = pd.concat([sam_props_batch, pd.DataFrame(empty_sam_rows)], sort=True)
        sam_props_df = pd.concat([sam_props_df, sam_props_batch], sort=True)
        index += batch_size
    return pd.merge(df, sam_props_df, on=[key_col])
