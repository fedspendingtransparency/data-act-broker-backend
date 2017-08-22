import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CGAC, SubTierAgency, FREC
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data

logger = logging.getLogger(__name__)


def delete_missing_cgacs(models, new_data):
    """If the new file doesn't contain CGACs we had before, we should delete
    the non-existent ones"""
    to_delete = set(models.keys()) - set(new_data['cgac_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(CGAC).filter(CGAC.cgac_code.in_(to_delete)).delete(synchronize_session=False)
    for cgac_code in to_delete:
        del models[cgac_code]


def update_cgacs(models, new_data):
    """Modify existing models or create new ones"""
    for _, row in new_data.iterrows():
        cgac_code = row['cgac_code']
        agency_abbreviation = row['agency_abbreviation']
        if cgac_code not in models:
            models[cgac_code] = CGAC()
        for field, value in row.items():
            if field == 'agency_name':
                value = ("%s (%s)" % (value, agency_abbreviation))
            setattr(models[cgac_code], field, value)


def load_cgac(file_name):
    """Load CGAC (high-level agency names) lookup table."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        models = {cgac.cgac_code: cgac for cgac in sess.query(CGAC)}

        # read CGAC values from csv
        data = pd.read_csv(file_name, dtype=str)
        # clean data
        data = clean_data(
            data,
            CGAC,
            {"cgac_agency_code": "cgac_code", "agency_name": "agency_name",
             "agency_abbreviation": "agency_abbreviation"},
            {"cgac_code": {"pad_to_length": 3}}
        )
        # de-dupe
        data.drop_duplicates(subset=['cgac_code'], inplace=True)

        delete_missing_cgacs(models, data)
        update_cgacs(models, data)
        sess.add_all(models.values())
        sess.commit()

        logger.info('%s CGAC records inserted', len(models))


def delete_missing_frecs(models, new_data):
    """If the new file doesn't contain CGACs we had before, we should delete
    the non-existent ones"""
    to_delete = set(models.keys()) - set(new_data['frec_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(FREC).filter(FREC.frec_code.in_(to_delete)).delete(synchronize_session=False)
    for frec_code in to_delete:
        del models[frec_code]


def update_frecs(models, new_data, cgac_dict):
    """Modify existing models or create new ones"""
    for _, row in new_data.iterrows():
        if row['cgac_code'] not in cgac_dict:
            new_data.drop(_)
            continue
        row['cgac_id'] = cgac_dict[row['cgac_code']]
        frec_code = row['frec_code']
        agency_abbreviation = row['agency_abbreviation']
        if frec_code not in models:
            models[frec_code] = FREC()
        for field, value in row.items():
            if field == 'agency_name' and agency_abbreviation:
                value = ("%s (%s)" % (value, agency_abbreviation))
            setattr(models[frec_code], field, value)


def load_frec(file_name):
    """Load FREC (high-level agency names) lookup table."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        models = {frec.frec_code: frec for frec in sess.query(FREC)}

        # read FREC values from csv
        data = pd.read_csv(file_name, dtype=str)

        # clean data
        data = clean_data(
            data,
            FREC,
            {"frec": "frec_code", "cgac_agency_code": "cgac_code", "frec_entity_description": "agency_name",
             "agency_abbreviation": "agency_abbreviation"},
            {"frec": {"keep_null": False}, "cgac_code": {"pad_to_length": 3}, "frec_code": {"pad_to_length": 4}}
        )
        # de-dupe
        data.drop_duplicates(subset=['frec_code'], inplace=True)
        # create foreign key dicts
        cgac_dict = {str(cgac.cgac_code): cgac.cgac_id for
                     cgac in sess.query(CGAC).filter(CGAC.cgac_code.in_(data["cgac_code"])).all()}

        # insert to db
        delete_missing_frecs(models, data)
        update_frecs(models, data, cgac_dict)
        sess.add_all(models.values())
        sess.commit()

        logger.info('%s FREC records inserted', len(models))


def delete_missing_sub_tier_agencies(models, new_data):
    """If the new file doesn't contain Sub Tier Agencies we had before, we should delete the non-existent ones"""
    to_delete = set(models.keys()) - set(new_data['sub_tier_agency_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(SubTierAgency).filter(SubTierAgency.sub_tier_agency_code.in_(to_delete)).delete(
            synchronize_session=False)
    for sub_tier_agency_code in to_delete:
        del models[sub_tier_agency_code]


def update_sub_tier_agencies(models, new_data, cgac_dict, frec_dict):
    """Modify existing models or create new ones"""
    for _, row in new_data.iterrows():
        if row['cgac_code'] not in cgac_dict or row['frec_code'] not in frec_dict:
            new_data.drop(_)
            continue
        row['cgac_id'] = cgac_dict[row['cgac_code']]
        row['frec_id'] = frec_dict[row['frec_code']]
        sub_tier_agency_code = row['sub_tier_agency_code']
        if sub_tier_agency_code not in models:
            models[sub_tier_agency_code] = SubTierAgency()
        for field, value in row.items():
            setattr(models[sub_tier_agency_code], field, value)


def load_sub_tier_agencies(file_name):
    """Load Sub Tier Agency (sub_tier-level agency names) lookup table."""
    with create_app().app_context():
        sess = GlobalDB.db().session
        models = {sub_tier_agency.sub_tier_agency_code: sub_tier_agency for
                  sub_tier_agency in sess.query(SubTierAgency)}

        # read Sub Tier Agency values from csv
        data = pd.read_csv(file_name, dtype=str)

        condition = data["FPDS DEPARTMENT ID"] == data["SUBTIER CODE"]
        data.loc[condition, "PRIORITY"] = 1
        data.loc[~condition, "PRIORITY"] = 2

        # clean data
        data = clean_data(
            data,
            SubTierAgency,
            {"cgac_agency_code": "cgac_code", "subtier_code": "sub_tier_agency_code", "priority": "priority",
             "fpds_department_id": "frec_code", "subtier_name": "sub_tier_agency_name", "is_frec": "is_frec"},
            {"cgac_code": {"pad_to_length": 3}, "frec_code": {"pad_to_length": 4},
             "sub_tier_agency_code": {"pad_to_length": 4}}
        )
        # de-dupe
        data.drop_duplicates(subset=['sub_tier_agency_code'], inplace=True)
        # create foreign key dicts
        cgac_dict = {str(cgac.cgac_code): cgac.cgac_id for
                     cgac in sess.query(CGAC).filter(CGAC.cgac_code.in_(data["cgac_code"])).all()}
        frec_dict = {str(frec.frec_code): frec.frec_id for
                     frec in sess.query(FREC).filter(FREC.frec_code.in_(data["frec_code"])).all()}

        delete_missing_sub_tier_agencies(models, data)
        update_sub_tier_agencies(models, data, cgac_dict, frec_dict)
        sess.add_all(models.values())
        sess.commit()

        logger.info('%s Sub Tier Agency records inserted', len(models))


def load_agency_data(base_path):
    """Load all domain value files.

    Parameters
    ----------
        base_path : directory that contains the domain values files.
        local_program_activity : optional location of the program activity file (None = use basePath)
    """
    if CONFIG_BROKER["use_aws"]:
        s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
        s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
        agency_list_file = s3bucket.get_key("agency_list.csv").generate_url(expires_in=600)
        cascading_agency_list_file = s3bucket.get_key("agency_codes_list.csv").generate_url(expires_in=600)
    else:
        agency_list_file = os.path.join(base_path, "agency_list.csv")
        cascading_agency_list_file = os.path.join(base_path, "agency_codes_list.csv")

    logger.info('Loading CGAC')
    load_cgac(agency_list_file)
    logger.info('Loading FREC')
    load_frec(cascading_agency_list_file)
    logger.info('Loading Sub Tier Agencies')
    load_sub_tier_agencies(cascading_agency_list_file)


if __name__ == '__main__':
    configure_logging()
    load_agency_data(os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"))
