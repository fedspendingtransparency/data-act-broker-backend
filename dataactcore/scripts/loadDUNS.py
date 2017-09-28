import logging
import os
import sys
import pandas as pd
import re
from collections import OrderedDict
import numpy as np
import argparse
import zipfile
import paramiko
import time
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


def get_relevant_models(data, benchmarks=False):
    # Get a list of the duns we're gonna work off of to prevent multiple calls to the database
    if benchmarks:
        get_models = time.time()
    logger.info("getting relevant models")
    duns_found = [duns.strip().zfill(9) for duns in list(data["awardee_or_recipient_uniqu"].unique())]
    dun_objects_found = sess.query(DUNS).filter(DUNS.awardee_or_recipient_uniqu.in_(duns_found))
    models = {duns.awardee_or_recipient_uniqu: duns for duns in dun_objects_found}
    logger.info("getting models with activation dates already set")
    activated_models = {duns_num: duns for duns_num, duns in models.items() if duns.activation_date is not None}
    if benchmarks:
        logger.info("Getting models took {} seconds".format(time.time() - get_models))
    return models, activated_models


def load_duns_by_row(data, sess, models, activated_models, benchmarks=False):
    # data = activation_check(data, activated_models, benchmarks).where(pd.notnull(data), None)
    update_duns(models, data, benchmarks=benchmarks)
    sess.add_all(models.values())


# Removed this function when adding registration_date
# def activation_check(data, activated_models, benchmarks=False):
#     # if activation_date's already set, keep it, otherwise update it (default)
#     logger.info("going through activation check")
#     if benchmarks:
#         activation_check_start = time.time()
#     lambda_func = (lambda duns_num: pd.Series([activated_models[duns_num].activation_date
#                                                if duns_num in activated_models else np.nan]))
#     data = data.assign(old_activation_date=data["awardee_or_recipient_uniqu"].apply(lambda_func))
#     data.loc[pd.notnull(data["old_activation_date"]), "activation_date"] = data["old_activation_date"]
#     del data["old_activation_date"]
#     if benchmarks:
#         logger.info("Activation check took {} seconds".format(time.time()-activation_check_start))
#     return data

def update_duns(models, new_data, benchmarks=False):
    """Modify existing models or create new ones"""
    logger.info("updating duns")
    if benchmarks:
        update_duns_start = time.time()
    for _, row in new_data.iterrows():
        awardee_or_recipient_uniqu = row['awardee_or_recipient_uniqu']
        if awardee_or_recipient_uniqu not in models:
            models[awardee_or_recipient_uniqu] = DUNS()
        for field, value in row.items():
            setattr(models[awardee_or_recipient_uniqu], field, value)
    if benchmarks:
        logger.info("Updating duns took {} seconds".format(time.time() - update_duns_start))


def clean_sam_data(data):
    return clean_data(data, DUNS, {
        "awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
        "activation_date": "activation_date",
        "deactivation_date": "deactivation_date",
        "registration_date": "registration_date",
        "expiration_date": "expiration_date",
        "last_sam_mod_date": "last_sam_mod_date",
        "sam_extract_code": "sam_extract_code",
        "legal_business_name": "legal_business_name"
    }, {
        'awardee_or_recipient_uniqu': {'pad_to_length': 9, 'keep_null': True}
    })


def parse_sam_file(file_path, sess, monthly=False, benchmarks=False):
    parse_start_time = time.time()
    logger.info("starting file " + str(file_path))

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
            "legal_business_name": 10
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
        batches = nrows//block_size
        # skip the first line again if the last batch is also the first batch
        skiplastrows = 2 if batches == 0 else 1
        last_block_size = (nrows % block_size)-skiplastrows
        batch = 0
        added_rows = 0
        while batch <= batches:
            skiprows = 1 if batch == 0 else (batch*block_size)
            nrows = (((batch+1)*block_size)-skiprows) if (batch < batches) else last_block_size
            logger.info('loading rows %s to %s', skiprows+1, nrows+skiprows)

            with zipfile.ZipFile(file_path) as zip_file:
                with zip_file.open(dat_file_name) as dat_file:
                    csv_data = pd.read_csv(dat_file, dtype=str, header=None, skiprows=skiprows, nrows=nrows, sep='|',
                                           usecols=column_header_mapping_ordered.values(),
                                           names=column_header_mapping_ordered.keys())

                    # add deactivation_date column for delete records
                    lambda_func = (lambda sam_extract: pd.Series([dat_file_date if sam_extract == "1" else np.nan]))
                    csv_data = csv_data.assign(deactivation_date=pd.Series([np.nan], name='deactivation_date')
                                               if monthly else csv_data["sam_extract_code"].apply(lambda_func))
                    # removing rows where DUNS number isn't even provided
                    csv_data = csv_data.where(csv_data["awardee_or_recipient_uniqu"].notnull())
                    # cleaning and replacing NaN/NaT with None's
                    csv_data = clean_sam_data(csv_data.where(pd.notnull(csv_data), None))

                    if monthly:
                        logger.info("adding all monthly data with bulk load")
                        if benchmarks:
                            bulk_month_load = time.time()
                        del csv_data["sam_extract_code"]
                        insert_dataframe(csv_data, DUNS.__table__.name, sess.connection())
                        if benchmarks:
                            logger.info("Bulk month load took {} seconds".format(time.time()-bulk_month_load))
                    else:
                        add_data = csv_data[csv_data.sam_extract_code == '2']
                        update_delete_data = csv_data[(csv_data.sam_extract_code == '3') |
                                                      (csv_data.sam_extract_code == '1')]
                        for dataframe in [add_data, update_delete_data]:
                            del dataframe["sam_extract_code"]

                        if not add_data.empty:
                            try:
                                logger.info("attempting to bulk load add data")
                                insert_dataframe(add_data, DUNS.__table__.name, sess.connection())
                            except IntegrityError:
                                logger.info("bulk loading add data failed, loading add data by row")
                                sess.rollback()
                                models, activated_models = get_relevant_models(add_data, benchmarks=benchmarks)
                                logger.info("loading add data ({} rows)".format(len(add_data.index)))
                                load_duns_by_row(add_data, sess, models, activated_models, benchmarks=benchmarks)
                        if not update_delete_data.empty:
                            models, activated_models = get_relevant_models(update_delete_data, benchmarks=benchmarks)
                            logger.info("loading update_delete data ({} rows)".format(len(update_delete_data.index)))
                            load_duns_by_row(update_delete_data, sess, models, activated_models, benchmarks=benchmarks)
                    sess.commit()

            added_rows += nrows
            batch += 1
            logger.info('%s DUNS records inserted', added_rows)
        if benchmarks:
            logger.info("Parsing {} took {} seconds with {} rows".format(dat_file_name, time.time()-parse_start_time,
                                                                         added_rows))


def process_from_dir(root_dir, file_name, sess, local, monthly=False, benchmarks=False):
    file_path = os.path.join(root_dir, file_name)
    if not local:
        logger.info("Pulling {}".format(file_name))
        with open(file_path, "wb") as zip_file:
            sftp.getfo(''.join([REMOTE_SAM_DIR, '/', file_name]), zip_file)
    parse_sam_file(file_path, sess, monthly=monthly, benchmarks=benchmarks)
    if not local:
        os.remove(file_path)


def get_parser():
    parser = argparse.ArgumentParser(description="Get the latest data from SAM and update duns table. By default, it "
                                                 "loads the latest daily file.")
    parser.add_argument("--historic", "-i", action="store_true", help='load the oldest monthly zip and all the daily'
                                                                      'files afterwards from the directory.')
    parser.add_argument("--local", "-l", type=str, default=None, help='work from a local directory')
    parser.add_argument("--monthly", "-m", type=str, default=None, help='load a local monthly file')
    parser.add_argument("--daily", "-d", type=str, default=None, help='load a local daily file')
    parser.add_argument("--benchmarks", "-b", action="store_true",
                        help='log times of operations for testing')
    parser.add_argument("--update", "-u", action="store_true",
                        help='Run all daily files since latest last_sam_mod_date in table')
    return parser


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    historic = args.historic
    local = args.local
    monthly = args.monthly
    daily = args.daily
    benchmarks = args.benchmarks
    update = args.update

    with create_app().app_context():
        configure_logging()
        sess = GlobalDB.db().session

        if monthly and daily:
            logger.error("For loading a single local file, you must provide either monthly or daily.")
            sys.exit(1)
        if historic and update:
            logger.error("For multiple file loads you must choose either historic or update.")
            sys.exit(1)
        elif (monthly or daily) and local:
            logger.error("Local directory specified with a local file.")
            sys.exit(1)
        elif monthly:
            parse_sam_file(monthly, sess=sess, monthly=True, benchmarks=benchmarks)
        elif daily:
            parse_sam_file(daily, sess=sess, benchmarks=benchmarks)
        else:
            # dealing with a local or remote directory
            if not local:
                root_dir = CONFIG_BROKER["d_file_storage_path"]
                username, password, host, port = get_config()
                if None in (username, password):
                    logger.error("Missing config elements for connecting to SAM")
                    sys.exit(1)

                client = paramiko.SSHClient()
                client.load_system_host_keys()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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

            # generate chronological list of daily and monthly files
            sorted_monthly_file_names = sorted([monthly_file for monthly_file in dirlist
                                                if re.match(".*MONTHLY_\d+\.ZIP", monthly_file.upper())])
            sorted_daily_file_names = sorted([daily_file for daily_file in dirlist
                                              if re.match(".*DAILY_\d+\.ZIP", daily_file.upper())])

            if historic or update:
                if historic:
                    if sorted_monthly_file_names:
                        process_from_dir(root_dir, sorted_monthly_file_names[0],
                                         sess, local, monthly=True, benchmarks=benchmarks)
                    else:
                        logger.info("No monthly file found.")

                if sorted_daily_file_names:
                    if historic:
                        if sorted_monthly_file_names:
                            earliest_daily_file = sorted_monthly_file_names[0].replace("MONTHLY", "DAILY")
                    else:
                        # Insert item into sorted file list with date of last sam mod
                        last_update = sess.query(DUNS.last_sam_mod_date).\
                                order_by(DUNS.last_sam_mod_date.desc()). \
                                filter(DUNS.last_sam_mod_date.isnot(None)). \
                                limit(1).one()[0].strftime("%Y%m%d")
                        earliest_daily_file = re.sub("_DAILY_[0-9]{8}\.ZIP", "_DAILY_" +
                                                     last_update + ".ZIP", sorted_daily_file_names[0])
                    if earliest_daily_file:
                        sorted_full_list = sorted(sorted_daily_file_names + [earliest_daily_file])
                        daily_files_after = sorted_full_list[sorted_full_list.index(earliest_daily_file) + 1:]
                    else:
                        daily_files_after = sorted_daily_file_names

                if daily_files_after:
                    for daily_file in daily_files_after:
                        process_from_dir(root_dir, daily_file, sess, local, benchmarks=benchmarks)
                else:
                    logger.info("No daily file found.")
            else:
                if sorted_daily_file_names:
                    process_from_dir(root_dir, sorted_daily_file_names[-1], sess, local, benchmarks=benchmarks)
                else:
                    logger.info("No daily file found.")
        sess.close()
