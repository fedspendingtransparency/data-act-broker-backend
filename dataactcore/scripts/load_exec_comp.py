import logging
import os
import sys
import pandas as pd
import paramiko
import zipfile
import re
from collections import OrderedDict
import numpy as np
import argparse

from dataactcore.models.domainModels import ExecutiveCompensation
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import insert_dataframe
from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)

REMOTE_SAM_DIR = '/current/SAM/6_EXECCOMP'


def parse_sam_file(file, sess):
    logger.info("starting file " + str(file.name))

    csv_file = os.path.splitext(os.path.basename(file.name))[0]+'.dat'
    zfile = zipfile.ZipFile(file.name)

    # can't use skipfooter, pandas' c engine doesn't work with skipfooter and the python engine doesn't work with dtype
    nrows = 0
    with zfile.open(csv_file) as f:
        nrows = len(f.readlines()) - 2  # subtract the header and footer
    column_header_mapping = {
        "awardee_or_recipient_uniqu": 0,
        "sam_extract": 4,
        "expiration_date": 7,
        "activation_date": 9,
        "ultimate_parent_legal_enti": 10,
        "ultimate_parent_unique_ide": 48,
        "exec_comp_str": 89
    }
    column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))
    csv_data = pd.read_csv(zfile.open(csv_file), dtype=str, header=None, skiprows=1, nrows=nrows, sep='|',
                           usecols=column_header_mapping_ordered.values(), names=column_header_mapping_ordered.keys())
    total_data = csv_data.copy()

    # skipping when sam_extract == '4' as it's expired
    total_data = total_data[total_data.sam_extract != '4']

    # parse out executive compensation from row 90
    lambda_func = (lambda ecs: pd.Series(list(parse_exec_comp(ecs).values())))
    parsed_data = total_data["exec_comp_str"].apply(lambda_func)
    parsed_data.columns = list(parse_exec_comp().keys())
    del total_data["exec_comp_str"]
    total_data = total_data.join(parsed_data)

    # split into 3 dataframes based on row 8 ('1', '2', '3')
    delete_data = total_data[total_data.sam_extract == '1'].replace(np.nan, "", regex=True)
    add_data = total_data[total_data.sam_extract == '2'].replace(np.nan, "", regex=True)
    update_data = total_data[total_data.sam_extract == '3'].replace(np.nan, "", regex=True)
    for dataframe in [add_data, update_data, delete_data, total_data]:
        del dataframe["sam_extract"]

    table_name = ExecutiveCompensation.__table__.name
    insert_dataframe(add_data, table_name, sess.connection())
    for _, row in update_data.iterrows():
        sess.query(ExecutiveCompensation).filter_by(awardee_or_recipient_uniqu=row['awardee_or_recipient_uniqu']).\
            update(row, synchronize_session=False)
    for _, row in delete_data.iterrows():
        sess.query(ExecutiveCompensation).filter_by(awardee_or_recipient_uniqu=row['awardee_or_recipient_uniqu']).\
            delete(synchronize_session=False)
    sess.commit()


def parse_exec_comp(exec_comp_str=None):
    """
    Parses the executive compensation string into a dictionary for the ExecutiveCompensation data model
    :param exec_comp_str: the incoming compensation string
    :return: dictionary for the ExecutiveCompensation data model
    """
    exec_comp_data = OrderedDict()
    for index in range(1, 6):
        exec_comp_data["high_comp_officer{}_full_na".format(index)] = np.nan
        exec_comp_data["high_comp_officer{}_amount".format(index)] = np.nan

    if isinstance(exec_comp_str, str) and not exec_comp_str.isdigit():
        high_comp_officers = exec_comp_str.split('~')

        # records have inconsistent values for Null
        # "see note above" is excluded as it may contain relevant info
        unaccepted_titles = ["x", "n/a", "na", "null", "none"]

        for index, high_comp_officer in enumerate(high_comp_officers):
            index += 1
            exec_name, exec_title, exec_comp = high_comp_officer.split('^')
            if exec_title.lower() not in unaccepted_titles:
                exec_comp_data["high_comp_officer{}_full_na".format(index)] = exec_name
                exec_comp_data["high_comp_officer{}_amount".format(index)] = exec_comp

    return exec_comp_data


def get_config():
    sam_config = CONFIG_BROKER.get('sam')

    if sam_config:
        return sam_config.get('private_key'), sam_config.get('username'), sam_config.get('password'), \
               sam_config.get('host'), sam_config.get('port')

    return None, None, None, None, None


def get_parser():
    parser = argparse.ArgumentParser(description="Get data from SAM and update execution_compensation table")
    parser.add_argument("--historic", "-i", action="store_true", help='populate based on historical data')
    parser.add_argument("--local", "-l", type=str, default=None, help='use a local directory')
    return parser

if __name__ == '__main__':
    configure_logging()
    parser = get_parser()
    args = parser.parse_args()

    historic = args.historic
    local = args.local

    with create_app().app_context():
        sess = GlobalDB.db().session

        if not local:
            root_dir = CONFIG_BROKER["d_file_storage_path"]
            private_key, username, password, host, port = get_config()
            if None in (private_key, username, password):
                logger.error("Missing config elements for connecting to SAM")
                sys.exit(1)

            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.connect(
                hostname=host,
                username=username,
                password=password,
                key_filename=private_key
            )
            sftp = client.open_sftp()
            # dirlist on remote host
            dirlist = sftp.listdir(REMOTE_SAM_DIR)
        else:
            root_dir = local
            dirlist = os.listdir(local)

        # generate chronological list of daily files
        sorted_daily_file_names = sorted([daily_file for daily_file in dirlist if re.match(".*DAILY_\d+", daily_file)])

        if historic:
            for daily_file in sorted_daily_file_names:
                if local:
                    file = open(os.path.join(root_dir, daily_file))
                else:
                    file = open(os.path.join(root_dir, daily_file), 'wb')
                    sftp.getfo(''.join([REMOTE_SAM_DIR, '/', daily_file]), file)
                parse_sam_file(file, sess)
                file.close()
                os.remove(os.path.join(root_dir, daily_file))
        elif not local:
            file = open(os.path.join(root_dir, sorted_daily_file_names[-1]), 'wb')
            sftp.getfo(''.join([REMOTE_SAM_DIR, '/', sorted_daily_file_names[-1]]), file)
            parse_sam_file(file, sess)
            file.close()
            os.remove(os.path.join(root_dir, sorted_daily_file_names[-1]))
        else:
            parse_sam_file(open(os.path.join(root_dir, sorted_daily_file_names[-1])), sess)
