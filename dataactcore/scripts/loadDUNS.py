import logging
import os
import sys
import pandas as pd
import re
from collections import OrderedDict
import numpy as np
import math

from dataactcore.models.domainModels import DUNS
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe
from dataactcore.config import CONFIG_BROKER


logger = logging.getLogger(__name__)

REMOTE_SAM_DIR = ' '

def update_duns(models, new_data):
    """Modify existing models or create new ones"""
    for _, row in new_data.iterrows():
        awardee_or_recipient_uniqu = row['awardee_or_recipient_uniqu']
        if awardee_or_recipient_uniqu not in models:
            models[awardee_or_recipient_uniqu] = DUNS()
        for field, value in row.items():
            setattr(models[awardee_or_recipient_uniqu], field, value)

def main():
    with create_app().app_context():
        sess = GlobalDB.db().session

        models = {duns.awardee_or_recipient_uniqu: duns for duns in sess.query(DUNS)}

        # models = {cgac.cgac_code: cgac for cgac in sess.query(CGAC)}
        file_name = 'SAM_PUBLIC_UTF-8_MONTHLY_20170702.txt'
        nrows = 0
        with open(file_name) as f:
            nrows = len(f.readlines()) - 2
        print(nrows)
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

            csv_data = pd.read_csv(file_name, dtype=str, header=None, skiprows=skiprows, nrows=nrows, sep='|',
                                   usecols=column_header_mapping_ordered.values(), names=column_header_mapping_ordered.keys())
            # clean data
            data = clean_data(
                csv_data,
                DUNS,
                {"awardee_or_recipient_uniqu": "awardee_or_recipient_uniqu",
                 "expiration_date": "expiration_date",
                 "last_sam_mod_date": "last_sam_mod_date",
                 "activation_date": "activation_date",
                 "legal_business_name": "legal_business_name"},
                {'awardee_or_recipient_uniqu': {'pad_to_length': 9, 'keep_null': True}}
            )
            # de-dupe
            # data.drop_duplicates(subset=['awardee_or_recipient_uniqu'], inplace=True)

            update_duns(models, data)

            sess.add_all(models.values())
            sess.commit()
            added_rows+=nrows
            batch+=1

            logger.info('%s DUNS records inserted', added_rows)
        logger.info('Load complete. %s DUNS records inserted', len(models))

if __name__ == '__main__':
    with create_app().app_context():
        configure_logging()
        main()