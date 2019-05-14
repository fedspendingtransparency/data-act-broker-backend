import logging
import os
import pandas as pd
import zipfile
import re
from collections import OrderedDict
import numpy as np
import argparse

from dataactcore.models.domainModels import DUNS
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactcore.config import CONFIG_BROKER
from dataactcore.utils.duns import get_client, REMOTE_SAM_EXEC_COMP_DIR
from dataactvalidator.scripts.loader_utils import insert_dataframe

logger = logging.getLogger(__name__)


def parse_exec_comp_file(filename, sess, root_dir, sftp=None):
    """ Parses the executive compensation file to update corresponding DUNS records

        Arguments:
            filename: name of file to import
            sess: database connection
            root_dir: working directory
            sftp: connection to remote server
    """

    if sftp:
        file = open(os.path.join(root_dir, filename), 'wb')
        sftp.getfo(''.join([REMOTE_SAM_EXEC_COMP_DIR, '/', filename]), file)
    else:
        file = open(os.path.join(root_dir, filename))

    logger.info('starting file ' + str(file.name))

    csv_file = os.path.splitext(os.path.basename(file.name))[0]+'.dat'
    zfile = zipfile.ZipFile(file.name)

    # can't use skipfooter, pandas' c engine doesn't work with skipfooter and the python engine doesn't work with dtype
    nrows = 0
    with zfile.open(csv_file) as f:
        nrows = len(f.readlines()) - 2  # subtract the header and footer
    column_header_mapping = {
        'awardee_or_recipient_uniqu': 0,
        'sam_extract': 4,
        'exec_comp_str': 89
    }
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))
    csv_data = pd.read_csv(zfile.open(csv_file), dtype=str, header=None, skiprows=1, nrows=nrows, sep='|',
                           usecols=column_header_mapping_ordered.values(), names=column_header_mapping_ordered.keys())
    total_data = csv_data.copy()
    total_data = total_data[total_data.awardee_or_recipient_uniqu.notnull() &
                            total_data.sam_extract.isin(['2', '3', 'A', 'E'])]
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

    update_exec_comp_duns(sess, total_data)

    file.close()
    if sftp:
        os.remove(os.path.join(root_dir, filename))


def update_exec_comp_duns(sess, exec_comp_data):
    """ Takes in a dataframe of exec comp data and updates associated DUNS

        Arguments:
            sess: database connection
            exec_comp_data: pandas dataframe representing exec comp data
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
        FROM temp_exec_comp_update as tecu
        WHERE duns.awardee_or_recipient_uniqu=tecu.awardee_or_recipient_uniqu;
    """
    result = sess.execute(update_sql)
    logger.info('Updated {} DUNS records exec comp data'.format(result.rowcount))

    logger.info('Dropping temp_exec_comp_update')
    sess.execute('DROP TABLE temp_exec_comp_update;')

    sess.commit()


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
        unaccepted_titles = ['x', 'n/a', 'na', 'null', 'none']

        for index, high_comp_officer in enumerate(high_comp_officers):
            index += 1
            exec_name, exec_title, exec_comp = high_comp_officer.split('^')
            if exec_title.lower() not in unaccepted_titles:
                exec_comp_data['high_comp_officer{}_full_na'.format(index)] = exec_name
                exec_comp_data['high_comp_officer{}_amount'.format(index)] = exec_comp

    return exec_comp_data


def get_parser():
    """ Generates list of command-line arguments

        Returns:
            argument parser to be used for commandline
    """
    parser = argparse.ArgumentParser(description='Get data from SAM and update execution_compensation table')
    group = parser.add_mutually_exclusive_group(required=True)
    parser.add_argument('--historic', '-i', action='store_true', help='populate based on historical data')
    group.add_argument('--local', '-l', type=str, default=None, help='local directory to work from')
    group.add_argument('--ssh_key', '-k', type=str, default=None, help='private key used to access the API remotely')
    return parser

if __name__ == '__main__':
    configure_logging()
    parser = get_parser()
    args = parser.parse_args()

    historic = args.historic
    local = args.local
    ssh_key = args.ssh_key

    with create_app().app_context():
        sess = GlobalDB.db().session
        sftp = None

        if not local:
            root_dir = CONFIG_BROKER['d_file_storage_path']

            client = get_client(ssh_key=ssh_key)
            sftp = client.open_sftp()
            # dirlist on remote host
            dirlist = sftp.listdir(REMOTE_SAM_EXEC_COMP_DIR)
        else:
            root_dir = local
            dirlist = os.listdir(local)

        # generate chronological list of daily files
        sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist if re.match('.*MONTHLY_\d+',
                                                                                                 monthly_file)])
        sorted_daily_file_names = sorted([daily_file for daily_file in dirlist if re.match('.*DAILY_\d+', daily_file)])

        if historic:
            parse_exec_comp_file(sorted_monthly_file_names[0], sess, root_dir, sftp=sftp)
            for daily_file in sorted_daily_file_names:
                parse_exec_comp_file(daily_file, sess, root_dir, sftp=sftp)
        else:
            parse_exec_comp_file(sorted_daily_file_names[-1], sess, root_dir, sftp=sftp)
