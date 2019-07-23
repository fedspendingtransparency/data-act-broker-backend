import logging
import os
import re
import time
import zipfile
import paramiko
import datetime
from collections import OrderedDict

import numpy as np
import pandas as pd
from sqlalchemy.exc import IntegrityError

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import DUNS
from dataactvalidator.scripts.loader_utils import clean_data, insert_dataframe
from dataactbroker.helpers.uri_helper import RetrieveFileFromUri

logger = logging.getLogger(__name__)

REMOTE_SAM_DUNS_DIR = '/current/SAM/2_FOUO/UTF-8/'
REMOTE_SAM_EXEC_COMP_DIR = '/current/SAM/6_EXECCOMP/UTF-8'
BUSINESS_TYPES_SEPARATOR = '~'


def get_client(ssh_key=None):
    """ Connects to the SAM client and returns a usable object for interaction

        Arguments:
            ssh_key: private ssh key to connect to the secure API

        Returns:
            client object to interact with the SAM service
    """
    sam_config = CONFIG_BROKER.get('sam_duns')
    if not sam_config:
        return None

    if ssh_key:
        host = sam_config.get('host_ssh')
        username = sam_config.get('username_ssh')
        password = sam_config.get('password_ssh')

        ssh_key_file = RetrieveFileFromUri(ssh_key, binary_data=False).get_file_object()
        pkey = paramiko.RSAKey.from_private_key(ssh_key_file, password=sam_config.get('ssh_key_password'))

    else:
        host = sam_config.get('host')
        username = sam_config.get('username')
        password = sam_config.get('password')
        pkey = None

    if None in (host, username, password) or ssh_key and not pkey:
        raise Exception("Missing config elements for connecting to SAM")

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        username=username,
        password=password,
        pkey=pkey
    )
    return client

def clean_sam_data(data):
    """ Wrapper around clean_data with the DUNS context

        Args:
            data: the dataframe to be cleaned

        Returns:
            a cleaned/updated dataframe to be imported
    """
    return clean_data(data, DUNS, {
        "awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
        "activation_date": "activation_date",
        "deactivation_date": "deactivation_date",
        "registration_date": "registration_date",
        "expiration_date": "expiration_date",
        "last_sam_mod_date": "last_sam_mod_date",
        "sam_extract_code": "sam_extract_code",
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
        "ultimate_parent_legal_enti": "ultimate_parent_legal_enti",
        "ultimate_parent_unique_ide": "ultimate_parent_unique_ide"
    }, {})


def parse_duns_file(file_path, sess, monthly=False, benchmarks=False, metrics=None):
    """ Takes in a DUNS file and adds the DUNS data to the database

        Args:
            file_path: the path to the SAM file
            sess: the database connection
            monthly: whether it's a monthly file
            benchmarks: whether to log times
            metrics: dictionary representing metrics data for the load
    """
    if not metrics:
        metrics = {
            'files_processed': [],
            'records_received': 0,
            'adds_received': 0,
            'updates_received': 0,
            'deletes_received': 0,
            'records_ignored': 0,
            'added_duns': [],
            'updated_duns': []
        }

    parse_start_time = time.time()
    logger.info("Starting file " + str(file_path))

    dat_file_name = os.path.splitext(os.path.basename(file_path))[0]+'.dat'
    sam_file_type = "MONTHLY" if monthly else "DAILY"
    dat_file_date = re.findall(".*{}_(.*).dat".format(sam_file_type), dat_file_name)[0]
    zfile = zipfile.ZipFile(file_path)

    column_header_mapping = {
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

    total_data = total_data[total_data.awardee_or_recipient_uniqu.notnull() &
                                     total_data.sam_extract_code.isin(['1', '2', '3'])]
    rows_processed = len(total_data.index)
    delete_data = total_data[total_data.sam_extract_code == '1']
    deletes_received = len(delete_data.index)
    add_data = total_data[total_data.sam_extract_code == '2']
    adds_received = len(add_data.index)
    update_data = total_data[total_data.sam_extract_code == '3']
    updates_received = len(update_data.index)
    del total_data["sam_extract_code"]

    # add deactivation_date column for delete records
    lambda_func = (lambda sam_extract: pd.Series([dat_file_date if sam_extract == "1" else np.nan]))
    total_data = total_data.assign(deactivation_date=pd.Series([np.nan], name='deactivation_date')
                               if monthly else total_data["sam_extract_code"].apply(lambda_func))
    # convert business types string to array
    bt_func = (lambda bt_raw: pd.Series([[str(code) for code in str(bt_raw).split('~')
                                          if isinstance(bt_raw, str)]]))
    total_data = total_data.assign(business_types_codes=total_data["business_types_raw"].apply(bt_func))
    del total_data["business_types_raw"]

    # cleaning and replacing NaN/NaT with None's
    total_data = clean_sam_data(total_data.where(pd.notnull(total_data), None))

    if benchmarks:
        logger.info("Parsing {} took {} seconds with {} rows".format(dat_file_name, time.time()-parse_start_time,
                                                                     rows_received))
    metrics['files_processed'].append(dat_file_name)
    metrics['records_received'] += rows_received
    metrics['records_processed'] += rows_processed
    metrics['adds_received'] += adds_received
    metrics['updates_received'] += updates_received
    metrics['deletes_received'] += deletes_received

    return total_data

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
                awardee_or_recipient_uniqu TEXT,
                activation_date DATE,
                deactivation_date DATE,
                registration_date DATE,
                last_sam_mod_date DATE,
                legal_business_name TEXT,
                dba_name TEXT,
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
                entity_structure TEXT
            );
        """.format(table_name)
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE {};'.format(table_name))
    insert_dataframe(data, table_name, sess.connection())


def update_duns(sess, duns_data, metrics=None):
    """ Takes in a dataframe of duns and adds/updates associated DUNS

        Args:
            sess: database connection
            duns_data: pandas dataframe representing exec comp data
            metrics: dictionary representing metrics of the script

        Returns:
            list of DUNS updated
    """
    if not metrics:
        metrics = {
            'added_duns': []
            'updated_duns': []
        }

    temp_table_name = 'temp_duns_update'
    create_temp_duns_table(sess, temp_table_name, duns_data)

    logger.info('Getting list of DUNS that will be added/updated for metrics')
    update_sql = """
        SELECT tdu.awardee_or_recipient_uniqu
        FROM temp_duns_update AS tdu
        LEFT JOIN duns ON duns.awardee_or_recipient_uniqu=tdu.awardee_or_recipient_uniqu
        WHERE duns.awardee_or_recipient_uniqu IS NULL;
    """
    added_duns_list = [row['awardee_or_recipient_uniqu'] for row in sess.execute(update_sql).fetchall()]
    update_sql = """
        SELECT duns.awardee_or_recipient_uniqu
        FROM duns
        JOIN temp_duns_update AS tdu ON duns.awardee_or_recipient_uniqu=tdu.awardee_or_recipient_uniqu;
    """
    updated_duns_list = [row['awardee_or_recipient_uniqu'] for row in sess.execute(update_sql).fetchall()]

    logger.info('Adding/updating DUNS based on temp_duns_update')
    upsert_sql = """
        INSERT INTO duns (
            awardee_or_recipient_uniqu TEXT,
            activation_date DATE,
            deactivation_date DATE,
            registration_date DATE,
            last_sam_mod_date DATE,
            legal_business_name TEXT,
            dba_name TEXT,
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
            entity_structure TEXT
        )
        SELECT *
        FROM temp_duns_update tdu
        ON CONFLICT DO
            UPDATE duns
            SET
                duns.activation_date = COALESCE(tdu.activation_date, duns.activation_date),
                duns.deactivation_date = COALESCE(tdu.deactivation_date, duns.deactivation_date),
                duns.registration_date = COALESCE(tdu.registration_date, duns.registration_date),
                duns.last_sam_mod_date = COALESCE(tdu.last_sam_mod_date, duns.last_sam_mod_date),
                duns.legal_business_name = COALESCE(tdu.legal_business_name, duns.legal_business_name),
                duns.dba_name = COALESCE(tdu.dba_name, duns.dba_name),
                duns.ultimate_parent_unique_ide = COALESCE(tdu.ultimate_parent_unique_ide, 
                                                           duns.ultimate_parent_unique_ide),
                duns.ultimate_parent_legal_enti = COALESCE(tdu.ultimate_parent_legal_enti, 
                                                           duns.ultimate_parent_legal_enti),
                duns.address_line_1 = COALESCE(tdu.address_line_1, duns.address_line_1),
                duns.address_line_2 = COALESCE(tdu.address_line_2, duns.address_line_2),
                duns.city = COALESCE(tdu.city, duns.city),
                duns.state = COALESCE(tdu.state, duns.state),
                duns.zip = COALESCE(tdu.zip, duns.zip),
                duns.zip4 = COALESCE(tdu.zip4, duns.zip4),
                duns.country_code = COALESCE(tdu.country_code, duns.country_code),
                duns.congressional_district = COALESCE(tdu.congressional_district, duns.congressional_district),
                duns.business_types_codes = COALESCE(tdu.business_types_codes, duns.business_types_codes),
                duns.entity_structure = COALESCE(tdu.entity_structure, duns.entity_structure)
            FROM temp_duns_update AS tdu
            WHERE duns.awardee_or_recipient_uniqu = tdu.awardee_or_recipient_uniqu;
    """
    sess.execute(upsert_sql)

    logger.info('Dropping {}'.format(temp_table_name))
    sess.execute('DROP TABLE {};'.format(temp_table_name))

    sess.commit()

    metrics['added_duns'].extend(added_duns_list)
    metrics['updated_duns'].extend(updated_duns_list)


def parse_exec_comp_file(filename, root_dir, sftp=None, ssh_key=None, metrics=None):
    """ Parses the executive compensation file to update corresponding DUNS records

        Args:
            filename: name of file to import
            root_dir: working directory
            sftp: connection to remote server
            ssh_key: ssh_key for reconnecting
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

    file_path = os.path.join(root_dir, filename)
    logger.info('starting file ' + file_path)

    csv_file = os.path.splitext(filename)[0]+'.dat'
    zfile = zipfile.ZipFile(file_path)

    column_header_mapping = {
        'awardee_or_recipient_uniqu': 0,
        'sam_extract': 4,
        'exec_comp_str': 89
    }
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))

    # can't use skipfooter, pandas' c engine doesn't work with skipfooter and the python engine doesn't work with dtype
    nrows = 0
    with zfile.open(csv_file) as dat_file:
        nrows = len(dat_file.readlines()) - 2  # subtract the header and footer
    with zfile.open(csv_file) as dat_file:
        csv_data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=1, nrows=nrows, sep='|',
                               usecols=column_header_mapping_ordered.values(),
                               names=column_header_mapping_ordered.keys(), quoting=3)
    total_data = csv_data.copy()
    records_received = len(total_data.index)
    total_data = total_data[total_data.awardee_or_recipient_uniqu.notnull() &
                            total_data.sam_extract.isin(['2', '3', 'A', 'E'])]
    records_processed = len(total_data.index)
    del total_data['sam_extract']
    # Note: we're splitting these up cause it vastly saves memory parsing only the records that are populated
    blank_exec = total_data[total_data.exec_comp_str.isnull()]
    pop_exec = total_data[total_data.exec_comp_str.notnull()]

    # parse out executive compensation from row 90 for populated records
    lambda_func = (lambda ecs: pd.Series(list(parse_exec_comp(ecs).values())))
    parsed_data = pop_exec['exec_comp_str'].apply(lambda_func)
    parsed_data.columns = list(parse_exec_comp().keys())
    del pop_exec['exec_comp_str']
    pop_exec = pop_exec.join(parsed_data)

    # leave blanks
    del blank_exec['exec_comp_str']
    blank_exec = blank_exec.assign(**parse_exec_comp())

    # setup the final dataframe
    total_data = pd.concat([pop_exec, blank_exec])
    total_data.replace('', np.nan, inplace=True)
    last_exec_comp_mod_date_str = re.findall('[0-9]{8}', filename)
    if not last_exec_comp_mod_date_str:
        raise Exception('Last Executive Compensation Mod Date not found in filename.')
    last_exec_comp_mod_date = datetime.datetime.strptime(last_exec_comp_mod_date_str[0], '%Y%m%d').date()
    total_data = total_data.assign(last_exec_comp_mod_date=last_exec_comp_mod_date)

    metrics['files_processed'].append(filename)
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