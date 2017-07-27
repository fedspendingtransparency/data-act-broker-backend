import logging
import os
import sys
import pandas as pd
import re
from collections import OrderedDict
import numpy as np
import math
import argparse
import zipfile
import paramiko
from sqlalchemy.exc import IntegrityError

from dataactcore.models.domainModels import DUNS
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe
from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)

REMOTE_SAM_DIR = '/current/SAM/2_FOUO/'

def get_config():
    sam_config = CONFIG_BROKER.get('sam')

    if sam_config:
        return sam_config.get('username'), sam_config.get('password'), sam_config.get('host'), \
               sam_config.get('port')

    return None, None, None, None, None

def load_duns_by_row(data, sess, models, prepopulated_models):
    activation_check(data, prepopulated_models)
    update_duns(models, data)
    sess.add_all(models.values())
    sess.commit()

def activation_check(data, prepopulated_models):
    # if activation_date's already set, keep it, otherwise update it (default)
    for index, row in data.iterrows():
        row_duns = str(row.awardee_or_recipient_uniqu).strip().zfill(9)
        if row_duns in prepopulated_models:
            data.loc[index, 'activation_date'] = str(prepopulated_models[row_duns].activation_date)

def update_duns(models, new_data):
    """Modify existing models or create new ones"""
    for _, row in new_data.iterrows():
        awardee_or_recipient_uniqu = row['awardee_or_recipient_uniqu']
        if awardee_or_recipient_uniqu not in models:
            models[awardee_or_recipient_uniqu] = DUNS()
        for field, value in row.items():
            value = None if (value in [np.nan, '']) else value
            setattr(models[awardee_or_recipient_uniqu], field, value)

def clean_sam_sata(data):
    return clean_data(
                data,
                DUNS,
                {"awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
                 "activation_date": "activation_date",
                 "deactivation_date": "deactivation_date",
                 "expiration_date": "expiration_date",
                 "last_sam_mod_date": "last_sam_mod_date",
                 "legal_business_name": "legal_business_name"},
                {'awardee_or_recipient_uniqu': {'pad_to_length': 9, 'keep_null': True}}
            )

def parse_sam_file(file, monthly=False):
    logger.info("starting file " + str(file.name))

    zip_file = zipfile.ZipFile(file.name)
    dat_file = os.path.splitext(os.path.basename(file.name))[0]+'.dat'
    sam_file_type = "MONTHLY" if monthly else "DAILY"
    dat_file_date = re.findall(".*{}_(.*).dat".format(sam_file_type), dat_file)[0]

    with create_app().app_context():
        sess = GlobalDB.db().session

        models = {duns.awardee_or_recipient_uniqu: duns for duns in sess.query(DUNS)}
        prepopulated_models = {duns_num: duns for duns_num, duns in models.items() if duns.activation_date != None}

        # models = {cgac.cgac_code: cgac for cgac in sess.query(CGAC)}
        nrows = 0
        with zip_file.open(dat_file) as f:
            nrows = len(f.readlines()) - 2
        block = 10000
        batches = math.modf(nrows/block)

        column_header_mapping = {
            "awardee_or_recipient_uniqu": 0,
            "sam_extract_code": 4,
            "expiration_date": 7,
            "last_sam_mod_date": 8,
            "activation_date": 9,
            "legal_business_name": 10
        }
        column_header_mapping_ordered = OrderedDict(sorted(column_header_mapping.items(), key=lambda c: c[1]))
        batch = 0
        added_rows = 0
        while batch <= batches[1]:
            skiprows = 1 if batch == 0 else (batch*block)
            nrows = (((batch+1)*block)-skiprows) if (batch < batches[1]) else batches[0]*block
            logger.info('loading rows %s to %s',skiprows+1,nrows+skiprows)

            csv_data = pd.read_csv(zip_file.open(dat_file), dtype=str, header=None, skiprows=skiprows, nrows=nrows, sep='|',
                                   usecols=column_header_mapping_ordered.values(), names=column_header_mapping_ordered.keys())

            # add deactivation_date column for delete records
            lambda_func = (lambda sam_extract: pd.Series([dat_file_date if sam_extract == "1" else '']))
            parsed_data = pd.Series([np.nan], name='deactivation_date') if monthly else csv_data["sam_extract_code"].apply(lambda_func)
            parsed_data.columns = ["deactivation_date"]
            csv_data = csv_data.join(parsed_data)

            if monthly:
                insert_dataframe(clean_sam_sata(csv_data), DUNS.__table__.name, sess.connection())
            else:
                add_data = clean_sam_sata(csv_data[csv_data.sam_extract_code == '2'])
                update_data = clean_sam_sata(csv_data[csv_data.sam_extract_code == '3'])
                delete_data = clean_sam_sata(csv_data[csv_data.sam_extract_code == '1'])
                try:
                    insert_dataframe(add_data, DUNS.__table__.name, sess.connection())
                    sess.commit()
                except IntegrityError:
                    sess.rollback()
                    load_duns_by_row(add_data, sess, models, prepopulated_models)
                load_duns_by_row(update_data, sess, models, prepopulated_models)
                load_duns_by_row(delete_data, sess, models, prepopulated_models)

            added_rows+=nrows
            batch+=1
            logger.info('%s DUNS records inserted', added_rows)
        logger.info('Load complete. %s DUNS records inserted', len(models))

def get_parser():
    parser = argparse.ArgumentParser(description="Get data from SAM and update execution_compensation table")
    parser.add_argument("--local", "-l", type=str, default=None, help='use a local directory')
    return parser

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    local = args.local

    with create_app().app_context():
        configure_logging()

        if not local:
            root_dir = CONFIG_BROKER["d_file_storage_path"]
            username, password, host, port = get_config()
            if None in (username, password):
                logger.error("Missing config elements for connecting to SAM")
                sys.exit(1)

            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.connect(
                hostname=host,
                username=username,
                password=password
            )
            sftp = client.open_sftp()
            # dirlist on remote host
            dirlist = sftp.listdir(REMOTE_SAM_DIR)
        else:
            root_dir = local
            dirlist = os.listdir(local)

        # generate chronological list of daily and monthy files
        sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist if re.match(".*MONTHLY_\d+",
                                                                                                 monthly_file)])
        sorted_daily_file_names = sorted([daily_file for daily_file in dirlist if re.match(".*DAILY_\d+", daily_file)])

        earliest_monthly_file = sorted_monthly_file_names[0]
        earliest_daily_file = sorted_monthly_file_names[0].replace("MONTHLY", "DAILY")
        sorted_daily_monthly = sorted(sorted_daily_file_names + [earliest_daily_file])
        daily_files_after = sorted_daily_monthly[sorted_daily_monthly.index(earliest_daily_file)+1:]

        # parse the earliest monthly file
        if local:
            file = open(os.path.join(root_dir, earliest_monthly_file))
        else:
            file = open(os.path.join(root_dir, earliest_monthly_file), 'wb')
            sftp.getfo(''.join([REMOTE_SAM_DIR, '/', earliest_monthly_file]), file)
        parse_sam_file(file, monthly=True)
        file.close()
        if not local:
            os.remove(os.path.join(root_dir, earliest_monthly_file))

        # parse all the daily files after that
        for daily_file in daily_files_after:
            if local:
                file = open(os.path.join(root_dir, daily_file))
            else:
                file = open(os.path.join(root_dir, daily_file), 'wb')
                sftp.getfo(''.join([REMOTE_SAM_DIR, '/', daily_file]), file)
            parse_sam_file(file)
            file.close()
            if not local:
                os.remove(os.path.join(root_dir, daily_file))