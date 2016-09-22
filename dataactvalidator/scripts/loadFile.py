import os
import pandas as pd
import boto
import glob
import logging
import re
from collections import namedtuple
from dataactvalidator.scripts.loaderUtils import LoaderUtils
from dataactcore.models.domainModels import CGAC, ObjectClass, ProgramActivity, SF133
from dataactcore.interfaces.db import databaseSession
from dataactcore.config import CONFIG_BROKER

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def loadCgac(filename):
    """Load CGAC (high-level agency names) lookup table."""
    model = CGAC

    with databaseSession() as sess:

        # for CGAC, delete and replace values
        sess.query(model).delete()

        # read CGAC values from csv
        data = pd.read_csv(filename, dtype=str)
        # clean data
        data = LoaderUtils.cleanData(
            data,
            model,
            {"cgac": "cgac_code", "agency": "agency_name"},
            {"cgac_code": {"pad_to_length": 3}}
        )
        # de-dupe
        data.drop_duplicates(subset=['cgac_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = LoaderUtils.insertDataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def loadObjectClass(filename):
    """Load object class lookup table."""
    model = ObjectClass

    with databaseSession() as sess:
        # for object class, delete and replace values
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = LoaderUtils.cleanData(
            data,
            model,
            {"max_oc_code": "object_class_code",
             "max_object_class_name": "object_class_name"},
            {}
        )
        # de-dupe
        data.drop_duplicates(subset=['object_class_code'], inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = LoaderUtils.insertDataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def loadProgramActivity(filename):
    """Load program activity lookup table."""
    model = ProgramActivity

    with databaseSession() as sess:

        # for program activity, delete and replace values??
        sess.query(model).delete()

        data = pd.read_csv(filename, dtype=str)
        data = LoaderUtils.cleanData(
            data,
            model,
            {"year": "budget_year",
             "agency_id": "agency_id",
             "alloc_id": "allocation_transfer_id",
             "account": "account_number",
             "pa_code": "program_activity_code",
             "pa_name": "program_activity_name"},
            {"program_activity_code": {"pad_to_length": 4},
             "agency_id": {"pad_to_length": 3},
             "allocation_transfer_id": {"pad_to_length": 3, "keep_null": True},
             "account_number": {"pad_to_length": 4}
             }
        )
        # because we're only loading a subset of program activity info,
        # there will be duplicate records in the dataframe. this is ok,
        # but need to de-duped before the db load.
        data.drop_duplicates(inplace=True)
        # insert to db
        table_name = model.__table__.name
        num = LoaderUtils.insertDataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def loadSF133(filename, fiscal_year, fiscal_period, force_load=False):
    """Load SF 133 (budget execution report) lookup table."""
    model = SF133

    with databaseSession() as sess:

        existing_records = sess.query(model).filter(
            model.fiscal_year == fiscal_year, model.period == fiscal_period)
        if force_load:
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

        data = pd.read_csv(filename, dtype=str)
        data = LoaderUtils.cleanData(
            data,
            model,
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
             "amount_summed":
            "amount"},
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
        data['tas'] = data.apply(lambda row: formatInternalTas(row), axis=1)

        # incoming .csv does not always include rows for zero-value SF-133 lines
        # so we add those here because they're needed for the SF-133 validations.
        # 1. "pivot" the sf-133 dataset to explode it horizontally, creating one
        # row for each tas/fiscal year/period, with columns for each SF-133 line.
        # the "fill_value=0" parameter puts a 0 into any Sf-133 line number cell
        # with a missing value for a specific tas/fiscal year/period.
        # 2. Once the zeroes are filled in, "melt" the pivoted data back to its normal
        # format of one row per tas/fiscal year/period.
        # NOTE: fields used for the pivot in step #1 (i.e., items in pivot_idx) cannot
        # have NULL values, else they will be silently dropped by pandas :(
        pivot_idx = ['created_at', 'updated_at', 'agency_identifier', 'allocation_transfer_agency',
                     'availability_type_code', 'beginning_period_of_availa', 'ending_period_of_availabil',
                     'main_account_code', 'sub_account_code', 'tas', 'fiscal_year', 'period']
        data.amount = data.amount.astype(float)
        data = pd.pivot_table(data, values='amount', index=pivot_idx, columns=['line'], fill_value=0).reset_index()
        data = pd.melt(data, id_vars=pivot_idx, value_name='amount')

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
        table_name = model.__table__.name
        num = LoaderUtils.insertDataframe(data, table_name, sess.connection())
        sess.commit()

    logger.info('{} records inserted to {}'.format(num, table_name))


def formatInternalTas(row):
    """Concatenate TAS components into a single field for internal use."""
    # This formatting should match formatting in dataactcore.models.stagingModels concatTas
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

def getSF133List(localSF133Dir):
    """Return info about existing SF133 files as a list of named tuples."""
    SF133File = namedtuple('SF133', ['full_file', 'file'])
    if localSF133Dir is not None:
        logger.info('Loading local SF-133')
        # get list of SF 133 files in the specified local directory
        sf133_files = glob.glob(os.path.join(localSF133Dir, 'sf_133*.csv'))
        sf133_list = [SF133File(sf133, os.path.basename(sf133)) for sf133 in sf133_files]
    else:
        logger.info("Loading SF-133")
        if CONFIG_BROKER["use_aws"]:
            # get list of SF 133 files in the config bucket on S3
            s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
            s3bucket = s3connection.lookup(CONFIG_BROKER['aws_bucket'])
            # get bucketlistresultset with all sf_133 files
            sf133_files = s3bucket.list(
                prefix='{}/sf_133'.format(CONFIG_BROKER['sf_133_folder']))
            sf133_list = [SF133File(sf133, os.path.basename(sf133.name)) for sf133 in sf133_files]
        else:
            sf133_list = []

    return sf133_list


def loadDomainValues(basePath, localSF133Dir = None, localProgramActivity = None):
    """Load all domain value files.

    Parameters
    ----------
        basePath : directory that contains the domain values files.
        localSF133Dir : location of the SF 133 files (None = get from S3).
        localProgramActivity : optional location of the program activity file (None = use basePath)
    """

    logger.info('Loading CGAC')
    loadCgac(os.path.join(basePath,"cgac.csv"))
    logger.info('Loading object class')
    loadObjectClass(os.path.join(basePath,"object_class.csv"))
    logger.info('Loading program activity')

    if localProgramActivity is not None:
        loadProgramActivity(localProgramActivity)
    else:
        loadProgramActivity(os.path.join(basePath, "program_activity.csv"))

    # get a list of SF 133 files to load
    sf133_list = getSF133List(localSF133Dir)
    SF_RE = re.compile(r'sf_133_(?P<year>\d{4})_(?P<period>\d{2})\.csv')
    for sf133 in sf133_list:
        # for each SF file, parse out fiscal year and period
        # and call the SF 133 loader
        file_match = SF_RE.match(sf133.file)
        if not file_match:
            logger.info('{}Skipping SF 133 file with invalid name: {}'.format(
                os.linesep, sf133.full_file))
            continue
        logger.info('{}Starting {}...'.format(os.linesep, sf133.full_file))
        loadSF133(
            sf133.full_file, file_match.group('year'), file_match.group('period'))


if __name__ == '__main__':
    loadDomainValues(
        os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config")
    )
