import logging
import os
import pandas as pd
import zipfile
import re
from collections import OrderedDict
import numpy as np
import argparse
import datetime
import json

from dataactcore.models.domainModels import DUNS
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.duns import get_client, REMOTE_SAM_EXEC_COMP_DIR
from dataactvalidator.scripts.loader_utils import insert_dataframe

logger = logging.getLogger(__name__)


def parse_exec_comp_file(filename, sess, root_dir, sftp=None, ssh_key=None, metrics=None):
    """ Parses the executive compensation file to update corresponding DUNS records

        Arguments:
            filename: name of file to import
            sess: database connection
            root_dir: working directory
            sftp: connection to remote server
            ssh_key: ssh_key for reconnecting
            metrics: dictionary representing metrics of the script
    """
    if not metrics:
        metrics = {
            'files_processed': [],
            'records_received': 0,
            'records_processed': 0,
            'updated_duns': []
        }

    file_path = os.path.join(root_dir, filename)
    if sftp:
        if sftp.sock.closed:
            # Reconnect if channel is closed
            ssh_client = get_client(ssh_key=ssh_key)
            sftp = ssh_client.open_sftp()
        with open(os.path.join(root_dir, filename), 'wb') as file:
            sftp.getfo(''.join([REMOTE_SAM_EXEC_COMP_DIR, '/', filename]), file)

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

    updated_duns = update_exec_comp_duns(sess, total_data)
    metrics['updated_duns'].extend(updated_duns)

    if sftp:
        os.remove(os.path.join(root_dir, filename))


def update_exec_comp_duns(sess, exec_comp_data):
    """ Takes in a dataframe of exec comp data and updates associated DUNS

        Arguments:
            sess: database connection
            exec_comp_data: pandas dataframe representing exec comp data

        Returns:
            list of DUNS updated
    """

    logger.info('Making temp_exec_comp_update table')
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS temp_exec_comp_update (
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
            high_comp_officer5_full_na TEXT
        );
    """
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE temp_exec_comp_update;')
    insert_dataframe(exec_comp_data, 'temp_exec_comp_update', sess.connection())

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
            high_comp_officer5_full_na = tecu.high_comp_officer5_amount
        FROM temp_exec_comp_update AS tecu
        WHERE duns.awardee_or_recipient_uniqu=tecu.awardee_or_recipient_uniqu;
    """
    sess.execute(update_sql)

    logger.info('Dropping temp_exec_comp_update')
    sess.execute('DROP TABLE temp_exec_comp_update;')

    sess.commit()
    return duns_list


def parse_exec_comp(exec_comp_str=None):
    """ Parses the executive compensation string into a dictionary for the ExecutiveCompensation data model

        Arguments:
            exec_comp_str: the incoming compensation string

        Returns:
            dictionary for the ExecutiveCompensation data model
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
            exec_name, exec_title, exec_comp = high_comp_officer.split('^')
            if exec_title.lower() not in unaccepted_titles and exec_name.lower() not in unaccepted_titles:
                exec_comp_data['high_comp_officer{}_full_na'.format(index)] = exec_name
                exec_comp_data['high_comp_officer{}_amount'.format(index)] = exec_comp

    return exec_comp_data


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
    parser = argparse.ArgumentParser(description='Get data from SAM and update execution_compensation table')
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument('--historic', '-i', action='store_true', help='reload from the first monthly file on')
    scope.add_argument('--update', '-u', action='store_true', help='load only the latest daily file')
    environ = parser.add_mutually_exclusive_group(required=True)
    environ.add_argument('--local', '-l', type=str, default=None, help='local directory to work from')
    environ.add_argument('--ssh_key', '-k', type=str, default=None, help='private key used to access the API remotely')
    return parser

if __name__ == '__main__':
    now = datetime.datetime.now()

    configure_logging()
    parser = get_parser()
    args = parser.parse_args()

    historic = args.historic
    update = args.update
    local = args.local
    ssh_key = args.ssh_key

    metrics = {
        'script_name': 'load_exec_duns.py',
        'start_time': str(now),
        'files_processed': [],
        'records_received': 0,
        'records_processed': 0,
        'updated_duns': [],
        'records_updated': 0
    }

    with create_app().app_context():
        sess = GlobalDB.db().session
        sftp = None

        if ssh_key:
            root_dir = CONFIG_BROKER['d_file_storage_path']
            client = get_client(ssh_key=ssh_key)
            sftp = client.open_sftp()
            # dirlist on remote host
            dirlist = sftp.listdir(REMOTE_SAM_EXEC_COMP_DIR)
        elif local:
            root_dir = local
            dirlist = os.listdir(local)

        # generate chronological list of daily files
        sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist if re.match('.*MONTHLY_\d+',
                                                                                                 monthly_file)])
        sorted_daily_file_names = sorted([daily_file for daily_file in dirlist if re.match('.*DAILY_\d+', daily_file)])

        if historic:
            parse_exec_comp_file(sorted_monthly_file_names[0], sess, root_dir, sftp=sftp, ssh_key=ssh_key,
                                 metrics=metrics)
            for daily_file in sorted_daily_file_names:
                parse_exec_comp_file(daily_file, sess, root_dir, sftp=sftp, ssh_key=ssh_key, metrics=metrics)
        elif update:
            # Insert item into sorted file list with date of last sam mod
            last_update = sess.query(DUNS.last_sam_mod_date). \
                order_by(DUNS.last_sam_mod_date.desc()). \
                filter(DUNS.last_sam_mod_date.isnot(None)). \
                first()[0].strftime("%Y%m%d")
            earliest_daily_file = re.sub("_DAILY_[0-9]{8}\.ZIP", "_DAILY_" +
                                         last_update + ".ZIP", sorted_daily_file_names[0])
            if earliest_daily_file:
                sorted_full_list = sorted(sorted_daily_file_names + [earliest_daily_file])
                daily_files_after = sorted_full_list[sorted_full_list.index(earliest_daily_file) + 1:]
            else:
                daily_files_after = sorted_daily_file_names

            if daily_files_after:
                for daily_file in daily_files_after:
                    parse_exec_comp_file(daily_file, sess, root_dir, sftp=sftp, ssh_key=ssh_key, metrics=metrics)
            else:
                logger.info("No daily file found.")

    metrics['records_updated'] = len(set(metrics['updated_duns']))
    del metrics['updated_duns']

    metrics['duration'] = str(datetime.datetime.now() - now)
    with open('load_exec_comp_metrics.json', 'w+') as metrics_file:
        json.dump(metrics, metrics_file)
