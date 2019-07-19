import logging
import os
import re
import time
import zipfile
import paramiko
from collections import OrderedDict

import numpy as np
import pandas as pd
from sqlalchemy.exc import IntegrityError

from dataactcore.config import CONFIG_BROKER
from dataactcore.models.domainModels import DUNS
from dataactvalidator.health_check import create_app
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


def get_relevant_models(data, sess, benchmarks=False):
    """ Get a list of the duns we're gonna work off of to prevent multiple calls to the database

        Args:
            data: dataframe representing the original list of duns we have available
            sess: the database connection
            benchmarks: whether or not to log times

        Returns:
            A list of models, models which have been activatated
    """
    if benchmarks:
        get_models = time.time()
    logger.info("Getting relevant models")
    duns_found = [duns.strip().zfill(9) for duns in list(data["awardee_or_recipient_uniqu"].unique())]
    dun_objects_found = sess.query(DUNS).filter(DUNS.awardee_or_recipient_uniqu.in_(duns_found))
    models = {duns.awardee_or_recipient_uniqu: duns for duns in dun_objects_found}
    logger.info("Getting models with activation dates already set")
    activated_models = {duns_num: duns for duns_num, duns in models.items() if duns.activation_date is not None}
    if benchmarks:
        logger.info("Getting models took {} seconds".format(time.time() - get_models))
    return models, activated_models


def load_duns_by_row(data, sess, models, activated_models, benchmarks=False):
    """ Updates the DUNS in the database that match to the models provided

        Args:
            data: dataframe representing the original list of duns we have available
            sess: the database connection
            models: the DUNS objects representing the updated data
            activated_models: the DUNS objects that have been activated
            benchmarks: whether or not to log times

        Returns:
            tuple of added and updated duns lists
    """
    added, updated = update_duns(models, data, benchmarks=benchmarks)
    sess.add_all(models.values())
    return added, updated


def update_duns(models, new_data, benchmarks=False):
    """ Modify existing models or create new ones

        Args:
            models: the DUNS objects representing the updated data
            new_data: the new data to update
            benchmarks: whether or not to log times

        Returns:
            tuple of added and updated duns lists
    """
    added = []
    updated = []
    logger.info("Updating duns")
    if benchmarks:
        update_duns_start = time.time()
    for _, row in new_data.iterrows():
        awardee_or_recipient_uniqu = row['awardee_or_recipient_uniqu']
        if awardee_or_recipient_uniqu not in models:
            models[awardee_or_recipient_uniqu] = DUNS()
            added.append(awardee_or_recipient_uniqu)
        else:
            updated.append(awardee_or_recipient_uniqu)
        for field, value in row.items():
            if value:
                setattr(models[awardee_or_recipient_uniqu], field, value)
    if benchmarks:
        logger.info("Updating duns took {} seconds".format(time.time() - update_duns_start))
    return added, updated


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

    with create_app().app_context():

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

        # Initial sweep of the file to see rows and possibly what DUNS we're updating
        if benchmarks:
            initial_sweep = time.time()
        nrows = 0
        with zipfile.ZipFile(file_path) as zip_file:
            with zip_file.open(dat_file_name) as dat_file:
                nrows = len(dat_file.readlines())
        if benchmarks:
            logger.info("Initial sweep took {} seconds".format(time.time() - initial_sweep))

        block_size = 10000
        batches = (nrows-1)//block_size
        # skip the first line again if the last batch is also the first batch
        skiplastrows = 2 if batches == 0 else 1
        last_block_size = ((nrows % block_size) or block_size)-skiplastrows
        batch = 0
        rows_received = 0
        adds_received = 0
        updates_received = 0
        deletes_received = 0
        records_ignored = 0
        added_duns = []
        updated_duns = []
        while batch <= batches:
            skiprows = 1 if batch == 0 else (batch*block_size)
            nrows = (((batch+1)*block_size)-skiprows) if (batch < batches) else last_block_size
            logger.info('Loading rows %s to %s', skiprows+1, nrows+skiprows)

            with zipfile.ZipFile(file_path) as zip_file:
                with zip_file.open(dat_file_name) as dat_file:
                    csv_data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=skiprows, nrows=nrows, sep='|',
                                           usecols=column_header_mapping_ordered.values(),
                                           names=column_header_mapping_ordered.keys(), quoting=3)

                    # add deactivation_date column for delete records
                    lambda_func = (lambda sam_extract: pd.Series([dat_file_date if sam_extract == "1" else np.nan]))
                    csv_data = csv_data.assign(deactivation_date=pd.Series([np.nan], name='deactivation_date')
                                               if monthly else csv_data["sam_extract_code"].apply(lambda_func))
                    # convert business types string to array
                    bt_func = (lambda bt_raw: pd.Series([[str(code) for code in str(bt_raw).split('~')
                                                          if isinstance(bt_raw, str)]]))
                    csv_data = csv_data.assign(business_types_codes=csv_data["business_types_raw"].apply(bt_func))
                    del csv_data["business_types_raw"]
                    # removing rows where DUNS number isn't even provided
                    csv_data = csv_data.where(csv_data["awardee_or_recipient_uniqu"].notnull())
                    # cleaning and replacing NaN/NaT with None's
                    csv_data = clean_sam_data(csv_data.where(pd.notnull(csv_data), None))

                    delete_data = csv_data[csv_data.sam_extract_code == '1']
                    deletes_received += len(delete_data.index)
                    add_data = csv_data[csv_data.sam_extract_code == '2']
                    adds_received += len(add_data.index)
                    update_data = csv_data[csv_data.sam_extract_code == '3']
                    updates_received += len(update_data.index)
                    total_received_data = csv_data[csv_data.sam_extract_code.isin(['1', '2', '3'])]
                    records_ignored += (nrows - len(total_received_data.index))

                    if monthly:
                        logger.info("Adding all monthly data with bulk load")
                        if benchmarks:
                            bulk_month_load = time.time()
                        del csv_data["sam_extract_code"]
                        insert_dataframe(csv_data, DUNS.__table__.name, sess.connection())
                        if benchmarks:
                            logger.info("Bulk month load took {} seconds".format(time.time()-bulk_month_load))
                        added_duns.extend(csv_data['awardee_or_recipient_uniqu'])
                    else:
                        update_delete_data = csv_data[(csv_data.sam_extract_code == '3') |
                                                      (csv_data.sam_extract_code == '1')]
                        for dataframe in [add_data, update_delete_data]:
                            del dataframe["sam_extract_code"]

                        if not add_data.empty:
                            try:
                                logger.info("Attempting to bulk load add data")
                                insert_dataframe(add_data, DUNS.__table__.name, sess.connection())
                                added_duns.extend(add_data['awardee_or_recipient_uniqu'])
                            except IntegrityError:
                                logger.info("Bulk loading add data failed, loading add data by row")
                                sess.rollback()
                                models, activated_models = get_relevant_models(add_data, sess, benchmarks=benchmarks)
                                logger.info("Loading add data ({} rows)".format(len(add_data.index)))
                                added, updated = load_duns_by_row(add_data, sess, models, activated_models,
                                                                  benchmarks=benchmarks)
                                added_duns.extend(added)
                                updated_duns.extend(updated)
                        if not update_delete_data.empty:
                            models, activated_models = get_relevant_models(update_delete_data, sess,
                                                                           benchmarks=benchmarks)
                            logger.info("Loading update_delete data ({} rows)".format(len(update_delete_data.index)))
                            added, updated = load_duns_by_row(update_delete_data, sess, models, activated_models,
                                                              benchmarks=benchmarks)
                            added_duns.extend(added)
                            updated_duns.extend(updated)

                    sess.commit()

            rows_received += nrows
            batch += 1
            logger.info('%s DUNS records received', rows_received)

        if benchmarks:
            logger.info("Parsing {} took {} seconds with {} rows".format(dat_file_name, time.time()-parse_start_time,
                                                                         rows_received))
        metrics['files_processed'].append(dat_file_name)
        metrics['records_received'] += rows_received
        metrics['adds_received'] += adds_received
        metrics['updates_received'] += updates_received
        metrics['deletes_received'] += deletes_received
        metrics['records_ignored'] += records_ignored
        metrics['added_duns'].extend(added_duns)
        metrics['updated_duns'].extend(updated_duns)


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

    logger.info('starting file ' + file_path)
    metrics['files_processed'].append(filename)

    csv_file = os.path.splitext(filename)[0]+'.dat'
    zfile = zipfile.ZipFile(file_path)

    # can't use skipfooter, pandas' c engine doesn't work with skipfooter and the python engine doesn't work with dtype
    nrows = 0
    with zfile.open(csv_file) as zip_file:
        nrows = len(zip_file.readlines()) - 2  # subtract the header and footer
    column_header_mapping = {
        'awardee_or_recipient_uniqu': 0,
        'sam_extract': 4,
        'exec_comp_str': 89
    }
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))
    with zfile.open(csv_file) as zip_file:
        csv_data = pd.read_csv(zip_file, dtype=str, header=None, skiprows=1, nrows=nrows, sep='|',
                               usecols=column_header_mapping_ordered.values(),
                               names=column_header_mapping_ordered.keys())
    total_data = csv_data.copy()
    metrics['records_received'] += len(total_data.index)
    total_data = total_data[total_data.awardee_or_recipient_uniqu.notnull() &
                            total_data.sam_extract.isin(['2', '3', 'A', 'E'])]
    metrics['records_processed'] += len(total_data.index)
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

    if sftp:
        os.remove(os.path.join(root_dir, filename))

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