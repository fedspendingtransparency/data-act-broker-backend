import logging
import os
import zipfile
import datetime
from collections import OrderedDict

import numpy as np
import pandas as pd

from dataactcore.models.domainModels import DUNS
from dataactvalidator.scripts.loader_utils import clean_data, trim_item, insert_dataframe
from dataactcore.models.lookups import DUNS_BUSINESS_TYPE_DICT

logger = logging.getLogger(__name__)

BUSINESS_TYPES_SEPARATOR = '~'


def clean_sam_data(data):
    """ Wrapper around clean_data with the DUNS context

        Args:
            data: the dataframe to be cleaned

        Returns:
            a cleaned/updated dataframe to be imported
    """
    if not data.empty:
        return clean_data(data, DUNS, {
            "uei": "uei",
            "awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
            "activation_date": "activation_date",
            "deactivation_date": "deactivation_date",
            "registration_date": "registration_date",
            "expiration_date": "expiration_date",
            "last_sam_mod_date": "last_sam_mod_date",
            "legal_business_name": "legal_business_name",
            "dba_name": "dba_name",
            "address_line_1": "address_line_1",
            "address_line_2": "address_line_2",
            "city": "city",
            "state": "state",
            "zip": "zip",
            "zip4": "zip4",
            "country_code": "country_code",
            "congressional_district": "congressional_district",
            "entity_structure": "entity_structure",
            "business_types_codes": "business_types_codes",
            "business_types": "business_types",
            "ultimate_parent_legal_enti": "ultimate_parent_legal_enti",
            "ultimate_parent_unique_ide": "ultimate_parent_unique_ide",
            "ultimate_parent_uei": "ultimate_parent_uei"
        }, {})
    return data


def parse_duns_file(file_path, metrics=None):
    """ Takes in a DUNS file and adds the DUNS data to the database

        Args:
            file_path: the path to the SAM file
            metrics: dictionary representing metrics data for the load
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
        "entity_structure": 29,
        "business_types_raw": 33,
        "ultimate_parent_legal_enti": 199,
        "ultimate_parent_uei": 200,
        "ultimate_parent_unique_ide": 201
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
        total_data = total_data.assign(uei=np.nan, ultimate_parent_unique_id=np.nan)

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
    del relevant_data["sam_extract_code"]

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
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            created_at TIMESTAMP WITHOUT TIME ZONE,
            updated_at TIMESTAMP WITHOUT TIME ZONE,
            uei TEXT,
            awardee_or_recipient_uniqu TEXT,
            activation_date DATE,
            expiration_date DATE,
            deactivation_date DATE,
            registration_date DATE,
            last_sam_mod_date DATE,
            legal_business_name TEXT,
            dba_name TEXT,
            ultimate_parent_uei TEXT,
            ultimate_parent_unique_ide TEXT,
            ultimate_parent_legal_enti TEXT,
            address_line_1 TEXT,
            address_line_2 TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            zip4 TEXT,
            country_code TEXT,
            congressional_district TEXT,
            business_types_codes TEXT[],
            business_types TEXT[],
            entity_structure TEXT
        );
    """.format(table_name)
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE {};'.format(table_name))
    insert_dataframe(data, table_name, sess.connection())


def update_duns(sess, duns_data, metrics=None, deletes=False):
    """ Takes in a dataframe of duns and adds/updates associated DUNS

        Args:
            sess: database connection
            duns_data: pandas dataframe representing duns data
            metrics: dictionary representing metrics of the script
            deletes: whether the data provided contains only delete records

        Returns:
            list of DUNS updated
    """
    if not metrics:
        metrics = {
            'added_duns': [],
            'updated_duns': []
        }

    temp_table_name = 'temp_duns_update'
    create_temp_duns_table(sess, temp_table_name, duns_data)

    logger.info('Getting list of DUNS that will be added/updated for metrics')
    insert_sql = """
        SELECT tdu.awardee_or_recipient_uniqu
        FROM temp_duns_update AS tdu
        LEFT JOIN duns ON duns.awardee_or_recipient_uniqu=tdu.awardee_or_recipient_uniqu
        WHERE duns.awardee_or_recipient_uniqu IS NULL;
    """
    added_duns_list = [row['awardee_or_recipient_uniqu'] for row in sess.execute(insert_sql).fetchall()]
    update_sql = """
        SELECT duns.awardee_or_recipient_uniqu
        FROM duns
        JOIN temp_duns_update AS tdu ON duns.awardee_or_recipient_uniqu=tdu.awardee_or_recipient_uniqu;
    """
    updated_duns_list = [row['awardee_or_recipient_uniqu'] for row in sess.execute(update_sql).fetchall()]

    logger.info('Adding/updating DUNS based on temp_duns_update')
    update_cols = """
        updated_at = tdu.updated_at,
        uei = tdu.uei,
        activation_date = tdu.activation_date,
        expiration_date = tdu.expiration_date,
        deactivation_date = NULL,
        registration_date = tdu.registration_date,
        last_sam_mod_date = tdu.last_sam_mod_date,
        legal_business_name = tdu.legal_business_name,
        dba_name = tdu.dba_name,
        ultimate_parent_unique_ide = tdu.ultimate_parent_unique_ide,
        ultimate_parent_legal_enti = tdu.ultimate_parent_legal_enti,
        address_line_1 = tdu.address_line_1,
        address_line_2 = tdu.address_line_2,
        city = tdu.city,
        state = tdu.state,
        zip = tdu.zip,
        zip4 = tdu.zip4,
        country_code = tdu.country_code,
        congressional_district = tdu.congressional_district,
        business_types_codes = tdu.business_types_codes,
        business_types = tdu.business_types,
        entity_structure = tdu.entity_structure,
    """
    if deletes:
        update_cols = """
            deactivation_date = tdu.deactivation_date,
        """
    update_sql = """
        UPDATE duns
        SET
            {}
            historic = FALSE
        FROM temp_duns_update AS tdu
        WHERE tdu.awardee_or_recipient_uniqu = duns.awardee_or_recipient_uniqu;
    """.format(update_cols)
    sess.execute(update_sql)

    insert_sql = """
        INSERT INTO duns (
            created_at,
            updated_at,
            uei,
            awardee_or_recipient_uniqu,
            activation_date,
            expiration_date,
            deactivation_date,
            registration_date,
            last_sam_mod_date,
            legal_business_name,
            dba_name,
            ultimate_parent_uei,
            ultimate_parent_unique_ide,
            ultimate_parent_legal_enti,
            address_line_1,
            address_line_2,
            city,
            state,
            zip,
            zip4,
            country_code,
            congressional_district,
            business_types_codes,
            business_types,
            entity_structure,
            historic
        )
        SELECT
            *,
            FALSE
        FROM temp_duns_update AS tdu
        WHERE NOT EXISTS (
            SELECT 1
            FROM duns
            WHERE duns.awardee_or_recipient_uniqu = tdu.awardee_or_recipient_uniqu
        );
    """
    sess.execute(insert_sql)

    logger.info('Dropping {}'.format(temp_table_name))
    sess.execute('DROP TABLE {};'.format(temp_table_name))

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
        'sam_extract': 4,
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
                            & total_data['sam_extract'].isin(['2', '3', 'A', 'E'])]
    records_processed = len(total_data.index)
    del total_data['sam_extract']

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


def create_temp_exec_comp_table(sess, table_name, data):
    """ Creates a temporary executive compensation table with the given name and data.

        Args:
            sess: database connection
            table_name: what to name the table being created
            data: pandas dataframe representing exec comp data
    """
    logger.info('Making {} table'.format(table_name))
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS {} (
            awardee_or_recipient_uniqu TEXT,
            high_comp_officer1_amount TEXT,
            high_comp_officer1_full_na TEXT,
            high_comp_officer2_amount TEXT,
            high_comp_officer2_full_na TEXT,
            high_comp_officer3_amount TEXT,
            high_comp_officer3_full_na TEXT,
            high_comp_officer4_amount TEXT,
            high_comp_officer4_full_na TEXT,
            high_comp_officer5_amount TEXT,
            high_comp_officer5_full_na TEXT,
            last_exec_comp_mod_date DATE
        );
    """.format(table_name)
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE {};'.format(table_name))
    insert_dataframe(data, table_name, sess.connection())


def update_exec_comp_duns(sess, exec_comp_data, metrics=None):
    """ Takes in a dataframe of exec comp data and updates associated DUNS

        Args:
            sess: database connection
            exec_comp_data: pandas dataframe representing exec comp data
            metrics: dictionary representing metrics of the script

        Returns:
            list of DUNS updated
    """
    if not metrics:
        metrics = {
            'updated_duns': []
        }

    temp_table_name = 'temp_exec_comp_update'
    create_temp_exec_comp_table(sess, temp_table_name, exec_comp_data)

    # Note: this can work just by getting the row count from the following SQL
    #       but this can run multiple times on possibly the same DUNS over several days,
    #       so it'll be more accurate to keep track of which DUNS get updated
    logger.info('Getting list of DUNS that will be updated for metrics')
    update_sql = """
        SELECT duns.awardee_or_recipient_uniqu
        FROM duns
        JOIN temp_exec_comp_update AS tecu ON duns.awardee_or_recipient_uniqu=tecu.awardee_or_recipient_uniqu;
    """
    duns_list = [row['awardee_or_recipient_uniqu'] for row in sess.execute(update_sql).fetchall()]

    logger.info('Updating DUNS based on temp_exec_comp_update')
    update_sql = """
        UPDATE duns
        SET
            high_comp_officer1_amount = tecu.high_comp_officer1_amount,
            high_comp_officer1_full_na = tecu.high_comp_officer1_full_na,
            high_comp_officer2_amount = tecu.high_comp_officer2_amount,
            high_comp_officer2_full_na = tecu.high_comp_officer2_full_na,
            high_comp_officer3_amount = tecu.high_comp_officer3_amount,
            high_comp_officer3_full_na = tecu.high_comp_officer3_full_na,
            high_comp_officer4_amount = tecu.high_comp_officer4_amount,
            high_comp_officer4_full_na = tecu.high_comp_officer4_full_na,
            high_comp_officer5_amount = tecu.high_comp_officer5_amount,
            high_comp_officer5_full_na = tecu.high_comp_officer5_full_na,
            last_exec_comp_mod_date = tecu.last_exec_comp_mod_date
        FROM temp_exec_comp_update AS tecu
        WHERE duns.awardee_or_recipient_uniqu=tecu.awardee_or_recipient_uniqu;
    """
    sess.execute(update_sql)

    logger.info('Dropping {}'.format(temp_table_name))
    sess.execute('DROP TABLE {};'.format(temp_table_name))

    sess.commit()
    metrics['updated_duns'].extend(duns_list)


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
