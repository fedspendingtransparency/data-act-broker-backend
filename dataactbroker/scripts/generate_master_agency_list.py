import os
import sys
import argparse
import logging
import datetime
import pandas as pd


from dataactbroker.helpers.uri_helper import RetrieveFileFromUri
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import pad_function, insert_dataframe

logger = logging.getLogger(__name__)

def pad_columns(df, pad_column_dict):
    """ Zero-pads columns in the dataframe provided

        Args:
            df: pandas dataframe
            pad_column_dict: dictionary of columns to pad {'column name': 4}

        Returns:
            updated dataframe
    """
    for column in pad_column_dict:
        df[column] = df[column].apply(pad_function, args=(pad_column_dict[column], True))
    return df


def load_temp_agency_list(sess, agency_list_path):
    """ Loads agency_list.csv into a temporary table named temp_agency_list

        Args:
            sess: database connection
            agency_list_path: uri to agency list csv
    """
    logger.info('Loading agency_list.csv')
    agency_list_col_names = [
        'cgac',
        'fpds_dep_id',
        'name',
        'abbreviation',
        'subtier_code',
        'subtier_name'
    ]
    with RetrieveFileFromUri(agency_list_path, 'r').get_file_object() as f:
        agency_list_df = pd.read_csv(f, dtype=str, skiprows=1, names=agency_list_col_names)
    pad_column_dict = {
        'cgac': 3,
        'fpds_dep_id': 4,
        'subtier_code': 4
    }
    agency_list_df = pad_columns(agency_list_df, pad_column_dict)
    create_temp_agency_list_table(sess, agency_list_df)


def create_temp_agency_list_table(sess, data):
    """ Creates a temporary agency list table with the given name and data.

        Args:
            sess: database connection
            data: pandas dataframe representing the agency list csv
    """
    logger.info('Making temp temp_agency_list table')
    create_table_sql = """
            CREATE TABLE IF NOT EXISTS temp_agency_list (
                cgac text,
                fpds_dep_id text,
                name text,
                abbreviation text,
                subtier_code text,
                subtier_name text
            );
        """
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE temp_agency_list;')
    insert_dataframe(data, 'temp_agency_list', sess.connection())
    sess.commit()


def load_temp_agency_list_codes(sess, agency_list_codes_path):
    """ Loads agency_list_codes.csv into a temporary table named temp_agency_list_codes

        Args:
            sess: database connection
            agency_list_codes_path: uri to agency codes list csv
    """
    logger.info('Loading agency_list_codes.csv')
    agency_list_codes_names = [
        'cgac',
        'fpds_dep_id',
        'name',
        'abbreviation',
        'registered_broker',
        'registered_asp',
        'original_source',
        'subtier_code',
        'subtier_name',
        'subtier_abbr',
        'frec',
        'frec_entity_desc',
        'subtier_source',
        'subtier_in_fpds_asp',
        'subtier_registered_asp',
        'original_name',
        'original_subtier_name',
        'comment',
        'is_frec'
    ]
    with RetrieveFileFromUri(agency_list_codes_path, 'r').get_file_object() as f:
        agency_list_codes_df = pd.read_csv(f, dtype=str, skiprows=1, names=agency_list_codes_names)
    pad_column_dict = {
        'cgac': 3,
        'fpds_dep_id': 4,
        'subtier_code': 4
    }
    agency_list_codes_df = pad_columns(agency_list_codes_df, pad_column_dict)
    create_temp_agency_list_codes_table(sess, agency_list_codes_df)


def create_temp_agency_list_codes_table(sess, data):
    """ Creates a temporary agency list table with the given name and data.

        Args:
            sess: database connection
            data: pandas dataframe representing the agency codes list csv
    """
    logger.info('Making temp temp_agency_list_codes table')
    create_table_sql = """
            CREATE TABLE IF NOT EXISTS temp_agency_list_codes (
                cgac text,
                fpds_dep_id text,
                name text,
                abbreviation text,
                registered_broker text,
                registered_asp text,
                original_source text,
                subtier_code text,
                subtier_name text,
                subtier_abbr text,
                frec text,
                frec_entity_desc text,
                subtier_source text,
                subtier_in_fpds_asp text,
                subtier_registered_asp text,
                original_name text,
                original_subtier_name text,
                comment text,
                is_frec text
            );
        """
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE temp_agency_list_codes;')
    insert_dataframe(data, 'temp_agency_list_codes', sess.connection())
    sess.commit()


def merge_broker_lists():
    """ Pulls in agencies from agency_list.csv that are not in agency_codes_list.csv """
    merge_sql = """
        INSERT INTO temp_agency_list_codes (
            "cgac",
            "fpds_dep_id",
            "name",
            "abbreviation",
            "registered_broker",
            "registered_asp",
            "original_source",
            "subtier_code",
            "subtier_name",
            "subtier_abbr",
            "frec",
            "frec_entity_desc",
            "subtier_source",
            "subtier_in_fpds_asp",
            "subtier_registered_asp",
            "original_name",
            "original_subtier_name",
            "comment",
            "is_frec"
        )
        SELECT 
            tal.cgac AS cgac,
            tal.fpds_dep_id AS fpds_dep_id,
            tal.name AS name,
            tal.abbreviation AS abbreviation,
            NULL AS registered_broker,
            NULL AS registered_asp,
            NULL AS original_source,
            tal.subtier_code AS subtier_code,
            tal.subtier_name AS subtier_name,
            NULL AS subtier_abbr,
            NULL AS frec,
            NULL AS frec_entity_desc,
            NULL AS subtier_source,
            NULL AS subtier_in_fpds_asp,
            NULL AS subtier_registered_asp,
            NULL AS original_name,
            NULL AS original_subtier_name,
            'pulled from agency_list.csv' AS comment,
            FALSE AS is_frec
        FROM temp_agency_list AS tal
        LEFT OUTER JOIN temp_agency_list_codes talc
            ON (tal.cgac = talc.cgac
            AND tal.subtier_code = talc.subtier_code
        )
        WHERE (talc.cgac IS NULL OR tal.subtier_code IS NULL);
    """
    inserted_rows = sess.execute(merge_sql)
    sess.commit()
    logger.info('Added {} rows from agency_list.csv to agency_codes_list.csv'.format(inserted_rows.rowcount))


def generate_master_agency_list(sess, agency_list, agency_list_codes):
    """ Main script algorithm that generates the master agency list """
    load_temp_agency_list(sess, agency_list)
    load_temp_agency_list_codes(sess, agency_list_codes)
    merge_broker_lists()

if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Merge broker agency lists')
    parser.add_argument('-acl', '--agency_list_codes', type=str, required=True, help="URI for agency_list_codes.csv")
    parser.add_argument('-al', '--agency_list', type=str, required=True, help="URI for agency_list.csv")

    with create_app().app_context():
        logger.info("Begin generating master agency list")
        sess = GlobalDB.db().session
        args = parser.parse_args()

        generate_master_agency_list(sess, args.agency_list, args.agency_list_codes)

        duration = str(datetime.datetime.now() - now)
        logger.info("Generating master agency list took {} seconds".format(duration))