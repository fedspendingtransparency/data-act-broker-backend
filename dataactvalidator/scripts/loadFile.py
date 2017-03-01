import os
import logging

import pandas as pd
import boto

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CGAC, SubTierAgency, ObjectClass, ProgramActivity, CountryCode
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def delete_missing_cgacs(models, new_data):
    """If the new file doesn't contain CGACs we had before, we should delete
    the non-existent ones"""
    to_delete = set(models.keys()) - set(new_data['cgac_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(CGAC).filter(CGAC.cgac_code.in_(to_delete)).delete(
            synchronize_session=False)
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


def delete_missing_sub_tier_agencies(models, new_data):
    """If the new file doesn't contain Sub Tier Agencies we had before, we should delete the non-existent ones"""
    to_delete = set(models.keys()) - set(new_data['sub_tier_agency_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(SubTierAgency).filter(SubTierAgency.sub_tier_agency_code.in_(to_delete)).delete(
            synchronize_session=False)
    for sub_tier_agency_code in to_delete:
        del models[sub_tier_agency_code]


def update_sub_tier_agencies(models, new_data, cgac_dict):
    """Modify existing models or create new ones"""
    for _, row in new_data.iterrows():
        row['cgac_id'] = cgac_dict[row['cgac_code']]
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
        # clean data
        data = clean_data(
            data,
            SubTierAgency,
            {
                "cgac_agency_code": "cgac_code",
                "sub_tier_code": "sub_tier_agency_code",
                "sub_tier_name": "sub_tier_agency_name",
            }, {
                "cgac_code": {"pad_to_length": 3},
                "sub_tier_agency_code": {"pad_to_length": 4}
            }
        )
        # de-dupe
        data.drop_duplicates(subset=['sub_tier_agency_code'], inplace=True)
        # create foreign key dict
        cgac_dict = {str(cgac.cgac_code): cgac.cgac_id for
                     cgac in sess.query(CGAC).filter(CGAC.cgac_code.in_(data["cgac_code"])).all()}

        delete_missing_sub_tier_agencies(models, data)
        update_sub_tier_agencies(models, data, cgac_dict)
        sess.add_all(models.values())
        sess.commit()

        logger.info('%s Sub Tier Agency records inserted', len(models))


def load_object_class(filename):
    """Load object class lookup table."""
    model = ObjectClass

    with create_app().app_context():
        sess = GlobalDB.db().session
        # for object class, delete and replace values
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            model,
            {"max_oc_code": "object_class_code", "max_object_class_name": "object_class_name"},
            {"object_class_code": {"pad_to_length": 3}}
        )
        # de-dupe
        data.drop_duplicates(subset=['object_class_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def load_program_activity(filename):
    """Load program activity lookup table."""
    model = ProgramActivity

    with create_app().app_context():
        sess = GlobalDB.db().session

        # for program activity, delete and replace values??
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            model,
            {"year": "budget_year", "agency_id": "agency_id", "alloc_id": "allocation_transfer_id",
             "account": "account_number", "pa_code": "program_activity_code", "pa_name": "program_activity_name"},
            {"program_activity_code": {"pad_to_length": 4}, "agency_id": {"pad_to_length": 3},
             "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True}, "account_number": {"pad_to_length": 4}}
        )
        # because we're only loading a subset of program activity info,
        # there will be duplicate records in the dataframe. this is ok,
        # but need to de-duped before the db load.
        data.drop_duplicates(inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def load_country_codes(filename):
    """Load object class lookup table."""
    model = CountryCode

    with create_app().app_context():
        sess = GlobalDB.db().session
        # for object class, delete and replace values
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = clean_data(
            data,
            model,
            {"country_code": "country_code", "country_name": "country_name"},
            {}
        )
        # de-dupe
        data.drop_duplicates(subset=['country_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def load_domain_values(base_path, local_program_activity=None):
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
        object_class_file = s3bucket.get_key("object_class.csv").generate_url(expires_in=600)
        program_activity_file = s3bucket.get_key("program_activity.csv").generate_url(expires_in=600)
        country_codes_file = s3bucket.get_key("country_codes.csv").generate_url(expires_in=600)

    else:
        agency_list_file = os.path.join(base_path, "agency_list.csv")
        object_class_file = os.path.join(base_path, "object_class.csv")
        program_activity_file = os.path.join(base_path, "program_activity.csv")
        country_codes_file = os.path.join(base_path, "country_codes.csv")

    logger.info('Loading CGAC')
    load_cgac(agency_list_file)
    logger.info('Loading Sub Tier Agencies')
    load_sub_tier_agencies(agency_list_file)
    logger.info('Loading object class')
    load_object_class(object_class_file)
    logger.info('Loading country codes')
    load_country_codes(country_codes_file)
    logger.info('Loading program activity')

    if local_program_activity is not None:
        load_program_activity(local_program_activity)
    else:
        load_program_activity(program_activity_file)


if __name__ == '__main__':
    configure_logging()
    load_domain_values(os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"))
