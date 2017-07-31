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

REMOTE_SAM_DIR = '/current/SAM/2_FOUO/UTF-8/'

def get_config():
    sam_config = CONFIG_BROKER.get('sam_duns')

    if sam_config:
        return sam_config.get('username'), sam_config.get('password'), sam_config.get('host'), \
               sam_config.get('port')

    return None, None, None, None, None

def load_duns_by_row(data, sess, models, prepopulated_models):
    logger.info("going through activation check")
    data = activation_check(data, prepopulated_models).where(pd.notnull(data), None)
    logger.info("updating duns")
    update_duns(models, data)
    sess.add_all(models.values())
    sess.commit()

def activation_check(data, prepopulated_models):
    # if activation_date's already set, keep it, otherwise update it (default)
    lambda_func = (lambda duns_num: pd.Series([prepopulated_models[duns_num].activation_date.strftime("%Y%m%d")
                                               if duns_num in prepopulated_models else np.nan]))
    data["old_activation_date"] = data["awardee_or_recipient_uniqu"].apply(lambda_func)
    data.loc[pd.notnull(data["old_activation_date"]), "activation_date"] = data["old_activation_date"]
    del data["old_activation_date"]
    return data

def update_duns(models, new_data):
    """Modify existing models or create new ones"""
    for _, row in new_data.iterrows():
        awardee_or_recipient_uniqu = row['awardee_or_recipient_uniqu']
        if awardee_or_recipient_uniqu not in models:
            models[awardee_or_recipient_uniqu] = DUNS()
        for field, value in row.items():
            setattr(models[awardee_or_recipient_uniqu], field, value)

def clean_sam_data(data):
    return clean_data(
                data,
                DUNS,
                {"awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
                 "activation_date": "activation_date",
                 "deactivation_date": "deactivation_date",
                 "expiration_date": "expiration_date",
                 "last_sam_mod_date": "last_sam_mod_date",
                 "sam_extract_code": "sam_extract_code",
                 "legal_business_name": "legal_business_name"},
                {'awardee_or_recipient_uniqu': {'pad_to_length': 9, 'keep_null': True}}
            )

def parse_sam_file(file_path, monthly=False):
    logger.info("starting file " + str(file_path))

    zip_file = zipfile.ZipFile(file_path)
    dat_file_name = os.path.splitext(os.path.basename(file_path))[0]+'.dat'
    sam_file_type = "MONTHLY" if monthly else "DAILY"
    dat_file_date = re.findall(".*{}_(.*).dat".format(sam_file_type), dat_file_name)[0]

    with create_app().app_context():
        sess = GlobalDB.db().session

        logger.info("getting models")
        models = {duns.awardee_or_recipient_uniqu: duns for duns in sess.query(DUNS)}
        logger.info("getting models with activation dates already set")
        prepopulated_models = {duns_num: duns for duns_num, duns in models.items() if duns.activation_date != None}

        # models = {cgac.cgac_code: cgac for cgac in sess.query(CGAC)}
        nrows = 0
        with zipfile.ZipFile(file_path) as zip_file:
            with zip_file.open(dat_file_name) as f:
                nrows = len(f.readlines())
        block_size = 10000
        batches = nrows//block_size
        # skip the first line again if the last batch is also the first batch
        skiplastrows = 2 if batches == 0 else 1
        last_block_size = (nrows%block_size)-skiplastrows

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
        while batch <= batches:
            skiprows = 1 if batch == 0 else (batch*block_size)
            nrows = (((batch+1)*block_size)-skiprows) if (batch < batches) else last_block_size
            logger.info('loading rows %s to %s',skiprows+1,nrows+skiprows)

            with zipfile.ZipFile(file_path) as zip_file:
                with zip_file.open(dat_file_name) as dat_file:
                    csv_data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=skiprows, nrows=nrows, sep='|',
                                           usecols=column_header_mapping_ordered.values(),
                                           names=column_header_mapping_ordered.keys())

                    # add deactivation_date column for delete records
                    lambda_func = (lambda sam_extract: pd.Series([dat_file_date if sam_extract == "1" else np.nan]))
                    csv_data["deactivation_date"] = (pd.Series([np.nan], name='deactivation_date')
                                                     if monthly else csv_data["sam_extract_code"].apply(lambda_func))
                    csv_data = clean_sam_data(csv_data.where(pd.notnull(csv_data), None))

                    if monthly:
                        logger.info("adding all monthly data with bulk load")
                        del csv_data["sam_extract_code"]
                        insert_dataframe(csv_data, DUNS.__table__.name, sess.connection())
                        sess.commit()
                    else:
                        logger.info("splitting daily file into add/update/delete rows")
                        add_data = csv_data[csv_data.sam_extract_code == '2']
                        update_data = csv_data[csv_data.sam_extract_code == '3']
                        delete_data = csv_data[csv_data.sam_extract_code == '1']
                        for dataframe in [add_data, update_data, delete_data]:
                            del dataframe["sam_extract_code"]

                        if not add_data.empty:
                            try:
                                logger.info("attempting to bulk load add data")
                                insert_dataframe(add_data, DUNS.__table__.name, sess.connection())
                                sess.commit()
                            except IntegrityError:
                                logger.info("bulk loading add data failed, loading add data by row")
                                sess.rollback()
                                load_duns_by_row(add_data, sess, models, prepopulated_models)
                        if not update_data.empty:
                            logger.info("loading update data by row")
                            load_duns_by_row(update_data, sess, models, prepopulated_models)
                        if not delete_data.empty:
                            logger.info("loading delete data by row")
                            load_duns_by_row(delete_data, sess, models, prepopulated_models)

            added_rows+=nrows
            batch+=1
            logger.info('%s DUNS records inserted', added_rows)
        sess.close()

def get_parser():
    parser = argparse.ArgumentParser(description="Get data from SAM and update execution_compensation table")
    parser.add_argument("--local", "-l", type=str, default=None, help='use a local directory')
    parser.add_argument("--monthly", "-m", type=str, default=None, help='load a local monthly file')
    parser.add_argument("--daily", "-d", type=str, default=None, help='load a local daily file')
    return parser

if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    local = args.local
    monthly = args.monthly
    daily = args.daily

    with create_app().app_context():
        configure_logging()

        if monthly and daily:
            print("For loading a single local file, you must provide either monthly or daily.")
            logger.error("For loading a single local file, you must provide either monthly or daily.")
            sys.exit(1)
        if (monthly or daily) and local:
            print("Local directory specified with a local file. Please choose one.")
            logger.error("Local directory specified with a local file.")
            sys.exit(1)
        elif monthly:
            file = open(monthly)
            parse_sam_file(file, monthly=True)
        elif daily:
            file = open(daily)
            parse_sam_file(file)
        else:
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
            sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist if re.match(".*MONTHLY_\d+\.ZIP",
                                                                                                     monthly_file.upper())])
            sorted_daily_file_names = sorted([daily_file for daily_file in dirlist if re.match(".*DAILY_\d+\.ZIP",
                                                                                               daily_file.upper())])
            earliest_monthly_file = sorted_monthly_file_names[0]
            earliest_daily_file = sorted_monthly_file_names[0].replace("MONTHLY", "DAILY")
            sorted_daily_monthly = sorted(sorted_daily_file_names + [earliest_daily_file])
            daily_files_after = sorted_daily_monthly[sorted_daily_monthly.index(earliest_daily_file)+1:]
            latest_daily_file = sorted_daily_file_names[-1]

            # parse the earliest monthly file
            file_path = os.path.join(root_dir, earliest_monthly_file)
            if not local:
                logger.info("Pulling {}".format(earliest_monthly_file))
                with open(file_path, "wb") as zip_file:
                    sftp.getfo(''.join([REMOTE_SAM_DIR, '/', earliest_monthly_file]), zip_file)
            parse_sam_file(file_path, monthly=True)
            if not local:
                os.remove(file_path)

            # parse all the daily files after that
            for daily_file in daily_files_after:
                file_path = os.path.join(root_dir, daily_file)
                if not local:
                    logger.info("Pulling {}".format(daily_file))
                    with open(file_path, 'wb') as zip_file:
                        sftp.getfo(''.join([REMOTE_SAM_DIR, '/', daily_file]), zip_file)
                parse_sam_file(file_path)
                if not local:
                    os.remove(file_path)