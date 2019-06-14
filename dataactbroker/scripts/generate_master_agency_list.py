import argparse
import logging
import datetime
import pandas as pd
import numpy as np


from dataactbroker.helpers.uri_helper import RetrieveFileFromUri
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loader_utils import pad_function, insert_dataframe
from dataactvalidator.filestreaming.csv_selection import write_query_to_file

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
        if column in df.columns:
            df[column] = df[column].apply(pad_function, args=(pad_column_dict[column], True))
    return df


def clean_dataframe(df):
    """ Cleans the dataframe for consistency and linkages

        Args:
            df: pandas dataframe

        Returns:
            updated dataframe
    """
    # zero pad columns
    pad_column_dict = {
        'cgac': 3,
        'fpds_dep_id': 4,
        'frec': 4,
        'subtier_code': 4
    }
    df = pad_columns(df, pad_column_dict)

    # Move explanatory subtier codes to comments
    subsumed_subtier = 'Subsumed under DOD submissions. Not listed here so as to avoid dupliation.'
    if 'comment' in df.columns:
        df.loc[df['subtier_code'] == subsumed_subtier, 'comment'] = \
            'Subtier s{} duplication.'.format(subsumed_subtier[1:-12])
    df.loc[df['subtier_code'] == subsumed_subtier, 'subtier_code'] = np.nan
    df.loc[df['subtier_name'] == subsumed_subtier, 'subtier_name'] = np.nan

    # replace unknowns with nans
    unknowns = ['Unknown (may not exist)', 'Unknown (May Not Exist)']
    df = df.replace(unknowns, np.nan)

    # replace empty strings with nans
    df = df.replace(r'^\s+$', np.nan, regex=True)
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
    agency_list_df = clean_dataframe(agency_list_df)
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
    agency_list_codes_df = clean_dataframe(agency_list_codes_df)
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


def merge_broker_lists(sess):
    """ Pulls in agencies from agency_list.csv that are not in agency_codes_list.csv

        Args:
            sess: database connection
    """
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
            ON (tal.cgac IS NOT DISTINCT FROM talc.cgac
            AND tal.subtier_code IS NOT DISTINCT FROM talc.subtier_code
        )
        WHERE (talc.cgac IS NULL AND talc.subtier_code IS NULL);
    """
    inserted_rows = sess.execute(merge_sql)
    sess.commit()
    logger.info('Added {} rows from agency_list.csv to agency_codes_list.csv'.format(inserted_rows.rowcount))


def load_temp_authoritative_agency_list(sess, authoritative_agency_list_path, user_selectable_agency_list_path):
    """ Loads authoritative_agency_list.csv and user_selectable_agency_list.csv into a temporary table named
    temp_authoritative_agency_list

        Args:
            sess: database connection
            authoritative_agency_list_path: uri to authoritative agency list csv
            user_selectable_agency_list_path: uri to user_selectable_agency_list.csv
    """
    logger.info('Loading authoritative_agency_list.csv and user_selectable_agency_list.csv')
    authoritative_agency_list_names = [
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
        'admin_org_name',
        'admin_org',
        'frec',
        'frec_entity_desc',
        'subtier_source',
        'subtier_in_fpds_asp',
        'subtier_registered_asp',
        'original_name',
        'original_subtier_name',
        'is_frec',
        'mission',
        'website',
        'congressional_justification',
        'icon_filename'
    ]
    with RetrieveFileFromUri(authoritative_agency_list_path, 'r').get_file_object() as f:
        authoritative_agency_list_df = pd.read_csv(f, dtype=str, skiprows=1, names=authoritative_agency_list_names)
    with RetrieveFileFromUri(user_selectable_agency_list_path, 'r').get_file_object() as f:
        user_selectable_agency_list_df = pd.read_csv(f, dtype=str, skiprows=1, names=authoritative_agency_list_names)
    authoritative_agency_list_df = authoritative_agency_list_df.assign(comment=np.nan)
    authoritative_agency_list_df = clean_dataframe(authoritative_agency_list_df)
    user_selectable_agency_list_df = user_selectable_agency_list_df.assign(comment=np.nan)
    user_selectable_agency_list_df = clean_dataframe(user_selectable_agency_list_df)
    merged = authoritative_agency_list_df.merge(user_selectable_agency_list_df, on=['cgac', 'subtier_code'],
                                                how='left', suffixes=('', '_drop'), indicator=True)
    merged['user_selectable'] = np.where(merged._merge == 'both', True, False)
    to_drop = [x + '_drop' for x in authoritative_agency_list_names if x not in ['cgac', 'subtier_code']]
    to_drop.extend(['_merge', 'comment_drop'])
    merged.drop(to_drop, axis=1, inplace=True)
    create_temp_authoritative_agency_list_table(sess, merged)


def create_temp_authoritative_agency_list_table(sess, data):
    """ Creates a temporary authoritative agency list table with the given name and data.

        Args:
            sess: database connection
            data: pandas dataframe representing the authoritative agency list csv
    """
    logger.info('Making temp temp_authoritative_agency_list table')
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS temp_authoritative_agency_list (
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
            admin_org_name text,
            admin_org text,
            frec text,
            frec_entity_desc text,
            subtier_source text,
            subtier_in_fpds_asp text,
            subtier_registered_asp text,
            original_name text,
            original_subtier_name text,
            is_frec text,
            mission text,
            website text,
            congressional_justification text,
            icon_filename text,
            user_selectable boolean,
            comment text
        );
    """
    sess.execute(create_table_sql)
    # Truncating in case we didn't clear out this table after a failure in the script
    sess.execute('TRUNCATE TABLE temp_authoritative_agency_list;')
    insert_dataframe(data, 'temp_authoritative_agency_list', sess.connection())
    sess.commit()


def merge_broker_website_lists(sess):
    """ Pulls in agencies from the merged broker_agency_list that are not in authoritative_agency_list.csv

        Args:
            sess: database connection
    """
    merge_sql = """
        INSERT INTO temp_authoritative_agency_list (
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
            "admin_org_name",
            "admin_org",
            "frec",
            "frec_entity_desc",
            "subtier_source",
            "subtier_in_fpds_asp",
            "subtier_registered_asp",
            "original_name",
            "original_subtier_name",
            "is_frec",
            "mission",
            "website",
            "congressional_justification",
            "icon_filename",
            "user_selectable",
            "comment"
        )
        SELECT
            talc.cgac AS cgac,
            talc.fpds_dep_id AS fpds_dep_id,
            talc.name AS name,
            talc.abbreviation AS abbreviation,
            talc.registered_broker AS registered_broker,
            talc.registered_asp AS registered_asp,
            talc.original_source AS original_source,
            talc.subtier_code AS subtier_code,
            talc.subtier_name AS subtier_name,
            talc.subtier_abbr AS subtier_abbr,
            NULL AS admin_org_name,
            NULL AS admin_org,
            talc.frec AS frec,
            talc.frec_entity_desc AS frec_entity_desc,
            talc.subtier_source AS subtier_source,
            talc.subtier_in_fpds_asp AS subtier_in_fpds_asp,
            talc.subtier_registered_asp AS subtier_registered_asp,
            talc.original_name AS original_name,
            talc.original_subtier_name AS original_subtier_name,
            talc.is_frec AS is_frec,
            NULL AS mission,
            NULL AS website,
            NULL AS congressional_justification,
            NULL AS icon_filename,
            FALSE AS user_selectable,
            talc.comment AS comment
        FROM temp_agency_list_codes AS talc
        LEFT OUTER JOIN temp_authoritative_agency_list taal
            ON (talc.cgac IS NOT DISTINCT FROM taal.cgac
            AND talc.subtier_code IS NOT DISTINCT FROM taal.subtier_code
        )
        WHERE (taal.cgac IS NULL AND taal.subtier_code IS NULL);
    """
    inserted_rows = sess.execute(merge_sql)
    sess.commit()
    logger.info('Added {} rows from the merged broker agency list to authoritative_agency_list.csv'
                .format(inserted_rows.rowcount))


def export_master_agency_list(sess, export_filename):
    """ Export the master agency list

        Args:
            sess: database connection
            export_filename: name of generated master agency list
    """
    export_query = """
        SELECT
            cgac AS "CGAC AGENCY CODE",
            fpds_dep_id AS "FPDS DEPARTMENT ID",
            name AS "AGENCY NAME",
            abbreviation AS "AGENCY ABBREVIATION",
            registered_broker AS "REGISTERED IN BROKER",
            registered_asp AS "REGISTERED IN ASP",
            original_source AS "ORIGINAL SOURCE FOR CGAC or SubTier",
            subtier_code AS "SUBTIER CODE",
            subtier_name AS "SUBTIER NAME",
            subtier_abbr AS "SUBTIER ABBREVIATION",
            admin_org_name AS "Admin Org Name",
            admin_org AS "ADMIN_ORG",
            frec AS "FREC",
            frec_entity_desc AS "FREC Entity Description",
            subtier_source AS "SubTierAcronymSource",
            subtier_in_fpds_asp AS "SubTier appears in data in FPDS or ASP?",
            subtier_registered_asp AS "REGISTERED IN ASP",
            original_name AS "AGENCY NAME (ORIGINAL)",
            original_subtier_name AS "SUBTIER NAME (ORIGINAL)",
            is_frec AS "IS_FREC",
            mission AS "MISSION",
            website AS "WEBSITE",
            congressional_justification AS "CONGRESSIONAL JUSTIFICATION",
            icon_filename AS "ICON FILENAME",
            UPPER(user_selectable::text) AS "USER SELECTABLE ON USASPENDING.GOV",
            comment AS "COMMENT"
        FROM temp_authoritative_agency_list
        ORDER BY cgac, subtier_code
    """
    # Remove newlines for raw query writing
    export_query = export_query.replace('\n', ' ')
    logger.info('Generating master agency file: {}'.format(export_filename))
    write_query_to_file(sess, export_query, export_filename, generate_headers=True, generate_string=False)


def remove_temp_tables(sess):
    """ Cleanup and remove temporary tables made via the script

        Args:
            sess: database connection
    """
    drop_query = 'DROP TABLE temp_agency_list, temp_agency_list_codes, temp_authoritative_agency_list;'
    logger.info('Cleaning up temporary tables')
    sess.execute(drop_query)
    sess.commit()


def generate_master_agency_list(sess, agency_list, agency_list_codes, authoritative_agency_list,
                                user_selectable_agency_list, export_filename):
    """ Main script algorithm that generates the master agency list

        Args:
            sess: database connection
            agency_list: broker's agency_list.csv
            agency_list_codes: broker's agency_list_codes.csv
            authoritative_agency_list: website's authoritative_agency_list.csv
            user_selectable_agency_list: website's user_selectable_agency_list.csv
            export_filename: name of generated master agency list
    """
    load_temp_agency_list(sess, agency_list)
    load_temp_agency_list_codes(sess, agency_list_codes)
    merge_broker_lists(sess)
    load_temp_authoritative_agency_list(sess, authoritative_agency_list, user_selectable_agency_list)
    merge_broker_website_lists(sess)
    export_master_agency_list(sess, export_filename)
    remove_temp_tables(sess)


if __name__ == '__main__':
    now = datetime.datetime.now()
    configure_logging()
    parser = argparse.ArgumentParser(description='Merge broker agency lists')
    parser.add_argument('-acl', '--agency_list_codes', type=str, required=True, help="URI for agency_list_codes.csv")
    parser.add_argument('-al', '--agency_list', type=str, required=True, help="URI for agency_list.csv")
    parser.add_argument('-aal', '--authoritative_agency_list', type=str, required=True,
                        help="URI for authoritative_agency_list.csv")
    parser.add_argument('-usal', '--user_selectable_agency_list', type=str, required=True,
                        help="URI for user_selectable_agency_list.csv")
    parser.add_argument('-e', '--export_filename', type=str, default='agency_codes.csv',
                        help="Name of generated master agency list")

    with create_app().app_context():
        logger.info("Begin generating master agency list")
        sess = GlobalDB.db().session
        args = parser.parse_args()

        generate_master_agency_list(sess, args.agency_list, args.agency_list_codes, args.authoritative_agency_list,
                                    args.user_selectable_agency_list, args.export_filename)

        duration = str(datetime.datetime.now() - now)
        logger.info("Generating master agency list took {} seconds".format(duration))
