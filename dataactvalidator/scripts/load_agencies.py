import os
import logging
import requests
import re
import numpy as np

import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import CGAC, SubTierAgency, FREC
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import clean_data
from dataactbroker.helpers.pandas_helper import check_dataframe_diff

logger = logging.getLogger(__name__)


def extract_abbreviation(row):
    """ Helper function for comparing datasets, this extracts the abbreviation from the db name

        Args:
            row: row in the dataframe representing the db model

        Returns:
            abbreviation extracted from agency_name or np.nan
    """
    abbr = re.findall('^.* \((.*)\)$', row['agency_name'])
    if abbr and abbr[0] != 'nan':
        return abbr[0]
    else:
        return np.nan


def extract_name(row):
    """ Helper function for comparing datasets, this extracts the name from the db name

        Args:
            row: row in the dataframe representing the db model

        Returns:
            name extracted from agency_name or np.nan
    """
    return row['agency_name'][:row['agency_name'].rindex('(')-1]


def delete_missing_cgacs(models, new_data):
    """ If the new file doesn't contain CGACs we had before, we should delete the non-existent ones.

        Args:
            models: all existing frec models in the database
            new_data: All the entries gathered from the agency file
    """
    to_delete = set(models.keys()) - set(new_data['cgac_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(CGAC).filter(CGAC.cgac_code.in_(to_delete)).delete(synchronize_session=False)
    for cgac_code in to_delete:
        del models[cgac_code]


def update_cgacs(models, new_data):
    """ Modify existing models or create new ones.

        Args:
            models: all existing frec models in the database
            new_data: All the entries gathered from the agency file
    """
    for _, row in new_data.iterrows():
        cgac_code = row['cgac_code']
        agency_abbreviation = row['agency_abbreviation']
        if cgac_code not in models:
            models[cgac_code] = CGAC()
        for field, value in row.items():
            if field == 'agency_name':
                value = ("%s (%s)" % (value, agency_abbreviation))
            setattr(models[cgac_code], field, value)


def load_cgac(file_name, force_reload=False):
    """ Load CGAC (high-level agency names) lookup table.

        Args:
            file_name: path/url to the file to be read
            force_reload: whether to reload regardless
    """
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

    # compare to existing content in table
    diff_found = check_dataframe_diff(data, CGAC, 'cgac_id', ['cgac_code'],
                                      lambda_funcs=[('agency_abbreviation', extract_abbreviation),
                                                    ('agency_name', extract_name)])

    if force_reload or diff_found:
        delete_missing_cgacs(models, data)
        update_cgacs(models, data)
        sess.add_all(models.values())
        sess.commit()

        logger.info('%s CGAC records inserted', len(models))
    else:
        logger.info('No differences found, skipping cgac table reload.')


def delete_missing_frecs(models, new_data):
    """ If the new file doesn't contain FRECs we had before, we should delete the non-existent ones.

        Args:
            models: all existing frec models in the database
            new_data: All the entries gathered from the agency file
    """
    to_delete = set(models.keys()) - set(new_data['frec_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(FREC).filter(FREC.frec_code.in_(to_delete)).delete(synchronize_session=False)
    for frec_code in to_delete:
        del models[frec_code]


def update_frecs(models, new_data, cgac_dict):
    """ Modify existing models or create new ones.

        Args
            models: all existing frec models in the database
            new_data: All the entries gathered from the agency file
            cgac_dict: A dictionary of all cgacs in the database
    """
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


def load_frec(file_name, force_reload=False):
    """ Load FREC (high-level agency names) lookup table.

        Args:
            file_name: path/url to the file to be read
            force_reload: whether to reload regardless
    """
    sess = GlobalDB.db().session
    models = {frec.frec_code: frec for frec in sess.query(FREC)}

    # read FREC values from csv
    data = pd.read_csv(file_name, dtype=str)

    # clean data
    data = clean_data(
        data,
        FREC,
        {"frec": "frec_code", "cgac_agency_code": "cgac_code", "frec_entity_description": "agency_name",
         "agency_abbreviation": "agency_abbreviation", "frec_cgac_association": "frec_cgac"},
        {"frec": {"keep_null": False}, "cgac_code": {"pad_to_length": 3}, "frec_code": {"pad_to_length": 4}}
    )
    # de-dupe
    data = data[data.frec_cgac == 'TRUE']
    data.drop(['frec_cgac'], axis=1, inplace=True)
    data.drop_duplicates(subset=['frec_code'], inplace=True)
    # create foreign key dicts
    cgac_dict = {str(cgac.cgac_code): cgac.cgac_id for
                 cgac in sess.query(CGAC).filter(CGAC.cgac_code.in_(data["cgac_code"])).all()}
    cgac_dict_flipped = {cgac_id: cgac_code for cgac_code, cgac_id in cgac_dict.items()}

    # compare to existing content in table
    def extract_cgac(row):
        return cgac_dict_flipped[row['cgac_id']] if row['cgac_id'] in cgac_dict_flipped else np.nan

    diff_found = check_dataframe_diff(data, FREC, 'frec_id', ['frec_code'], fk_cols=['cgac_id'],
                                      lambda_funcs=[('agency_abbreviation', extract_abbreviation),
                                                    ('agency_name', extract_name),
                                                    ('cgac_code', extract_cgac)])
    if force_reload or diff_found:
        # create foreign key dicts

        # insert to db
        delete_missing_frecs(models, data)
        update_frecs(models, data, cgac_dict)
        sess.add_all(models.values())
        sess.commit()

        logger.info('%s FREC records inserted', len(models))
    else:
        logger.info('No differences found, skipping frec table reload.')


def delete_missing_sub_tier_agencies(models, new_data):
    """ If the new file doesn't contain Sub Tier Agencies we had before, we should delete the non-existent ones

        Args:
            models: all existing sub tier models in the database
            new_data: All the entries gathered from the agency file
    """
    to_delete = set(models.keys()) - set(new_data['sub_tier_agency_code'])
    sess = GlobalDB.db().session
    if to_delete:
        sess.query(SubTierAgency).filter(SubTierAgency.sub_tier_agency_code.in_(to_delete)).delete(
            synchronize_session=False)
    for sub_tier_agency_code in to_delete:
        del models[sub_tier_agency_code]


def update_sub_tier_agencies(models, new_data, cgac_dict, frec_dict):
    """ Modify existing models or create new ones

        Args:
            models: the list of existing models
            new_data: the data that was read in from the file
            cgac_dict: a dictionary of all cgac codes in the database
            frec_dict: a dictionary of all frec codes in the database
    """
    for _, row in new_data.iterrows():
        if row['cgac_code'] not in cgac_dict:
            new_data.drop(_)
            continue
        row['cgac_id'] = cgac_dict[row['cgac_code']]
        row['frec_id'] = frec_dict[row['frec_code']]
        sub_tier_agency_code = row['sub_tier_agency_code']
        if sub_tier_agency_code not in models:
            models[sub_tier_agency_code] = SubTierAgency()
        for field, value in row.items():
            setattr(models[sub_tier_agency_code], field, value)


def load_sub_tier_agencies(file_name, force_reload=False):
    """ Load Sub Tier Agency (sub_tier-level agency names) lookup table.

        Args:
            file_name: path/url to the file to be read
            force_reload: whether to reload regardless
    """
    sess = GlobalDB.db().session
    models = {sub_tier_agency.sub_tier_agency_code: sub_tier_agency for
              sub_tier_agency in sess.query(SubTierAgency)}

    # read Sub Tier Agency values from csv
    data = pd.read_csv(file_name, dtype=str)

    condition = data["FPDS DEPARTMENT ID"] == data["SUBTIER CODE"]
    data.loc[condition, "PRIORITY"] = 1
    data.loc[~condition, "PRIORITY"] = 2
    data.replace({'TRUE': True, 'FALSE': False}, inplace=True)

    # clean data
    data = clean_data(
        data,
        SubTierAgency,
        {"cgac_agency_code": "cgac_code", "subtier_code": "sub_tier_agency_code", "priority": "priority",
         "frec": "frec_code", "subtier_name": "sub_tier_agency_name", "is_frec": "is_frec"},
        {"cgac_code": {"pad_to_length": 3}, "frec_code": {"pad_to_length": 4},
         "sub_tier_agency_code": {"pad_to_length": 4}}
    )
    # de-dupe
    data.drop_duplicates(subset=['sub_tier_agency_code'], inplace=True)
    # create foreign key dicts
    cgac_dict = {str(cgac.cgac_code): cgac.cgac_id for
                 cgac in sess.query(CGAC).filter(CGAC.cgac_code.in_(data["cgac_code"])).all()}
    cgac_dict_flipped = {cgac_id: cgac_code for cgac_code, cgac_id in cgac_dict.items()}
    frec_dict = {str(frec.frec_code): frec.frec_id for
                 frec in sess.query(FREC).filter(FREC.frec_code.in_(data["frec_code"])).all()}
    frec_dict_flipped = {frec_id: frec_code for frec_code, frec_id in frec_dict.items()}

    # compare to existing content in table
    def int_to_float(row):
        return float(row['priority'])

    def extract_cgac(row):
        return cgac_dict_flipped[row['cgac_id']] if row['cgac_id'] in cgac_dict_flipped else np.nan

    def extract_frec(row):
        return frec_dict_flipped[row['frec_id']] if row['frec_id'] in frec_dict_flipped else np.nan

    diff_found = check_dataframe_diff(data, SubTierAgency, 'sub_tier_agency_id', ['sub_tier_agency_code'],
                                      fk_cols=['cgac_id', 'frec_id'],
                                      lambda_funcs=[('priority', int_to_float), ('cgac_code', extract_cgac),
                                                    ('frec_code', extract_frec)])
    if force_reload or diff_found:
        delete_missing_sub_tier_agencies(models, data)
        update_sub_tier_agencies(models, data, cgac_dict, frec_dict)
        sess.add_all(models.values())
        sess.commit()

        logger.info('%s Sub Tier Agency records inserted', len(models))
    else:
        logger.info('No differences found, skipping subtier table reload.')


def load_agency_data(base_path, force_reload=False):
    """ Load agency data into the database

        Args:
            base_path: directory that contains the agency files
            force_reload: whether to reload regardless
    """
    agency_codes_url = 'https://files.usaspending.gov/reference_data/agency_codes_test.csv'
    logger.info('Loading agency codes file from {}'.format(agency_codes_url))

    # Get data from public S3 bucket
    r = requests.get(agency_codes_url, allow_redirects=True)
    agency_codes_file = os.path.join(base_path, 'agency_codes.csv')
    open(agency_codes_file, 'wb').write(r.content)

    # Parse data
    with create_app().app_context():
        logger.info('Loading CGAC')
        load_cgac(agency_codes_file, force_reload=force_reload)
        logger.info('Loading FREC')
        load_frec(agency_codes_file, force_reload=force_reload)
        logger.info('Loading Sub Tier Agencies')
        load_sub_tier_agencies(agency_codes_file, force_reload=force_reload)

    # Delete file once we're done
    os.remove(agency_codes_file)


if __name__ == '__main__':
    configure_logging()
    load_agency_data(os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"))
