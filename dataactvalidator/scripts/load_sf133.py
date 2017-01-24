from collections import namedtuple
from datetime import date
import glob
import logging
import os
import re
import sys

import boto
import pandas as pd

from dataactcore.config import CONFIG_BROKER
from dataactcore.interfaces.db import GlobalDB
from dataactcore.logging import configure_logging
from dataactcore.models.domainModels import matching_cars_subquery, SF133
from dataactvalidator.health_check import create_app
from dataactvalidator.scripts.loaderUtils import clean_data, insert_dataframe

logger = logging.getLogger(__name__)


def load_all_sf133(sf133_path=None, force_sf133_load=False):
    """Load any SF-133 files that are not yet in the database."""
    # get a list of SF 133 files to load
    sf133_list = get_sf133_list(sf133_path)
    sf_re = re.compile(r'sf_133_(?P<year>\d{4})_(?P<period>\d{2})\.csv')
    for sf133 in sf133_list:
        # for each SF file, parse out fiscal year and period
        # and call the SF 133 loader
        file_match = sf_re.match(sf133.file)
        if not file_match:
            logger.info('Skipping SF 133 file with invalid name: %s',
                        sf133.full_file)
            continue
        logger.info('Starting %s...', sf133.full_file)
        load_sf133(
            sf133.full_file, file_match.group('year'), file_match.group('period'),
            force_sf133_load=force_sf133_load
        )


def fill_blank_sf133_lines(data):
    """Incoming .csv does not always include rows for zero-value SF-133 lines
    so we add those here because they're needed for the SF-133 validations.
    1. "pivot" the sf-133 dataset to explode it horizontally, creating one
        row for each tas/fiscal year/period, with columns for each SF-133 line.
    2. Fill any SF-133 line number cells with a missing value for a
       specific tas/fiscal year/period with a 0.0. We don't do this in the
       "pivot" step because that'll downcast floats to ints
    3. Once the zeroes are filled in, "melt" the pivoted data back to its normal
       format of one row per tas/fiscal year/period.
    NOTE: fields used for the pivot in step #1 (i.e., items in pivot_idx) cannot
    have NULL values, else they will be silently dropped by pandas :("""
    pivot_idx = (
        'created_at', 'updated_at', 'agency_identifier',
        'allocation_transfer_agency', 'availability_type_code',
        'beginning_period_of_availa', 'ending_period_of_availabil',
        'main_account_code', 'sub_account_code', 'tas', 'fiscal_year',
        'period')

    data = pd.pivot_table(
        data, values='amount', index=pivot_idx, columns=['line']).reset_index()
    data = data.fillna(value=0.0)
    data = pd.melt(data, id_vars=pivot_idx, value_name='amount')
    return data


def update_tas_id(fiscal_year, fiscal_period):
    """Set the tas_id on newly SF133 entries. We use raw SQL as sqlalchemy
    doesn't have operators like OVERLAPS and IS NOT DISTINCT FROM built in
    (resulting in a harder to understand query)."""
    sess = GlobalDB.db().session
    # number of months since 0AD for this fiscal period
    zero_based_period = fiscal_period - 1
    absolute_fiscal_month = 12 * fiscal_year + zero_based_period - 3
    absolute_following_month = absolute_fiscal_month + 1
    start_date = date(absolute_fiscal_month // 12,
                      (absolute_fiscal_month % 12) + 1,     # 1-based
                      1)
    end_date = date(absolute_following_month // 12,
                    (absolute_following_month % 12) + 1,    # 1-based
                    1)

    subquery = matching_cars_subquery(sess, SF133, start_date, end_date)
    logger.info("Updating tas_ids for Fiscal %s-%s", fiscal_year,
                fiscal_period)
    sess.query(SF133).\
        filter_by(fiscal_year=fiscal_year, period=fiscal_period).\
        update({SF133.tas_id: subquery}, synchronize_session=False)
    sess.commit()


def load_sf133(filename, fiscal_year, fiscal_period, force_sf133_load=False):
    """Load SF 133 (budget execution report) lookup table."""

    with create_app().app_context():
        sess = GlobalDB.db().session

        existing_records = sess.query(SF133).filter(
            SF133.fiscal_year == fiscal_year, SF133.period == fiscal_period)
        if force_sf133_load:
            # force a reload of this period's current data
            logger.info('Force SF 133 load: deleting existing records for {} {}'.format(
                fiscal_year, fiscal_period))
            delete_count = existing_records.delete()
            logger.info('{} records deleted'.format(delete_count))
        elif existing_records.count():
            # if there's existing data & we're not forcing a load, skip
            logger.info('SF133 {} {} already in database ({} records). Skipping file.'.format(
                fiscal_year, fiscal_period, existing_records.count()))
            return

        data = clean_sf133_data(filename, SF133)

        # Now that we've added zero lines for EVERY tas and SF 133 line number, get rid of the ones
        # we don't actually use in the validations. Arguably, it would be better just to include
        # everything, but that drastically increases the number of records we're inserting to the
        # sf_133 table. If we ever decide that we need *all* SF 133 lines that are zero value,
        # remove the next two lines.
        sf_133_validation_lines = [
            '1000', '1010', '1011', '1012', '1013', '1020', '1021', '1022',
            '1023', '1024', '1025', '1026', '1029', '1030', '1031', '1032',
            '1033', '1040', '1041', '1042', '1160', '1180', '1260', '1280',
            '1340', '1440', '1540', '1640', '1750', '1850', '1910', '2190',
            '2490', '2500', '3020', '4801', '4802', '4881', '4882', '4901',
            '4902', '4908', '4981', '4982'
        ]
        data = data[(data.line.isin(sf_133_validation_lines)) | (data.amount != 0)]

        # we didn't use the the 'keep_null' option when padding allocation transfer agency,
        # because nulls in that column break the pivot (see above comments).
        # so, replace the ata '000' with an empty value before inserting to db
        data['allocation_transfer_agency'] = data['allocation_transfer_agency'].str.replace('000', '')
        # make a pass through the dataframe, changing any empty values to None, to ensure
        # that those are represented as NULL in the db.
        data = data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

        # insert to db
        table_name = SF133.__table__.name
        num = insert_dataframe(data, table_name, sess.connection())
        update_tas_id(int(fiscal_year), int(fiscal_period))
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def clean_sf133_data(filename, sf133_data):
    data = pd.read_csv(filename, dtype=str)
    data = clean_data(
        data,
        sf133_data,
        {"ata": "allocation_transfer_agency",
         "aid": "agency_identifier",
         "availability_type_code": "availability_type_code",
         "bpoa": "beginning_period_of_availa",
         "epoa": "ending_period_of_availabil",
         "main_account": "main_account_code",
         "sub_account": "sub_account_code",
         "fiscal_year": "fiscal_year",
         "period": "period",
         "line_num": "line",
         "amount_summed": "amount"},
        {"allocation_transfer_agency": {"pad_to_length": 3},
         "agency_identifier": {"pad_to_length": 3},
         "main_account_code": {"pad_to_length": 4},
         "sub_account_code": {"pad_to_length": 3},
         # next 3 lines handle the TAS fields that shouldn't
         # be padded but should still be empty spaces rather
         # than NULLs. this ensures that the downstream pivot & melt
         # (which insert the missing 0-value SF-133 lines)
         # will work as expected (values used in the pivot
         # index cannot be NULL).
         # the "pad_to_length: 0" works around the fact
         # that sometimes the incoming data for these columns
         # is a single space and sometimes it is blank/NULL.
         "beginning_period_of_availa": {"pad_to_length": 0},
         "ending_period_of_availabil": {"pad_to_length": 0},
         "availability_type_code": {"pad_to_length": 0},
         "amount": {"strip_commas": True}}
    )

    # todo: find out how to handle dup rows (e.g., same tas/period/line number)
    # line numbers 2002 and 2012 are the only duped SF 133 report line numbers,
    # and they are not used by the validation rules, so for now
    # just remove them before loading our SF-133 table
    dupe_line_numbers = ['2002', '2102']
    data = data[~data.line.isin(dupe_line_numbers)]

    # add concatenated TAS field for internal use (i.e., joining to staging tables)
    data['tas'] = data.apply(lambda row: format_internal_tas(row), axis=1)
    data['amount'] = data['amount'].astype(float)

    data = fill_blank_sf133_lines(data)

    return data


def format_internal_tas(row):
    """Concatenate TAS components into a single field for internal use."""
    # This formatting should match formatting in dataactcore.models.stagingModels concat_tas
    tas = ''.join([
        row['allocation_transfer_agency'] if row['allocation_transfer_agency'] else '000',
        row['agency_identifier'] if row['agency_identifier'] else '000',
        row['beginning_period_of_availa'] if row['beginning_period_of_availa'].strip() else '0000',
        row['ending_period_of_availabil'] if row['ending_period_of_availabil'].strip() else '0000',
        row['availability_type_code'].strip() if row['availability_type_code'].strip() else ' ',
        row['main_account_code'] if row['main_account_code'] else '0000',
        row['sub_account_code'] if row['sub_account_code'] else '000'
    ])
    return tas


def get_sf133_list(sf133_path):
    """Return info about existing SF133 files as a list of named tuples."""
    SF133File = namedtuple('SF133', ['full_file', 'file'])
    if sf133_path is not None:
        logger.info('Loading local SF-133')
        # get list of SF 133 files in the specified local directory
        sf133_files = glob.glob(os.path.join(sf133_path, 'sf_133*.csv'))
        sf133_list = [SF133File(sf133, os.path.basename(sf133)) for sf133 in sf133_files]
    else:
        logger.info("Loading SF-133")
        if CONFIG_BROKER["use_aws"]:
            # get list of SF 133 files in the config bucket on S3
            s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
            s3bucket = s3connection.lookup(CONFIG_BROKER['sf_133_bucket'])
            # get bucketlistresultset with all sf_133 files
            sf133_files = s3bucket.list(prefix='sf_133')
            sf133_list = [SF133File(sf133, os.path.basename(sf133.name)) for sf133 in sf133_files]
        else:
            sf133_list = []

    return sf133_list

if __name__ == '__main__':
    configure_logging()
    force_load = '-f' in sys.argv or '--force' in sys.argv
    load_all_sf133(
        os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"),
        force_load
    )
