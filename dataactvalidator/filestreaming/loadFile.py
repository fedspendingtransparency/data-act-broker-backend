import os
import pandas as pd
import boto
import glob
import logging
import re
from collections import namedtuple
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactcore.models.domainModels import CGAC,ObjectClass,ProgramActivity,SF133
from dataactcore.config import CONFIG_BROKER

logger = logging.getLogger(__name__)

def loadCgac(filename):
    interface = ValidatorValidationInterface()
    model = CGAC

    # for CGAC, delete and replace values
    interface.session.query(model).delete()
    interface.session.commit()

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
    # Fix up cells that have spaces instead of being empty.
    # Set the truly empty cells to None so they get inserted to db as NULL
    # TODO: very ugly function below...is there a better way?
    data = data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)
    # insert to db
    table_name = model.__table__.name
    num = LoaderUtils.insertDataframe(data, table_name, interface.engine)
    logger.info('{} records inserted to {}'.format(num, table_name))


def loadObjectClass(filename):
    interface = ValidatorValidationInterface()
    model = ObjectClass

    # for object class, delete and replace values
    interface.session.query(model).delete()
    interface.session.commit()

    data = pd.read_csv(filename, dtype=str)
    data = LoaderUtils.cleanData(
        data,
        model,
        {"max_oc_code":"object_class_code",
         "max_object_class_name": "object_class_name"},
        {}
    )
    # de-dupe
    data.drop_duplicates(subset=['object_class_code'], inplace=True)
    # TODO: very ugly function below...is there a better way?
    data = data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)
    # insert to db
    table_name = model.__table__.name
    num = LoaderUtils.insertDataframe(data, table_name, interface.engine)
    logger.info('{} records inserted to {}'.format(num, table_name))


def loadProgramActivity(filename):
    interface = ValidatorValidationInterface()
    model = ProgramActivity

    # for program activity, delete and replace values??
    interface.session.query(model).delete()
    interface.session.commit()

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
    # TODO: very ugly function below...is there a better way?
    data = data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)
    # insert to db
    table_name = model.__table__.name
    num = LoaderUtils.insertDataframe(data, table_name, interface.engine)
    logger.info('{} records inserted to {}'.format(num, table_name))


def loadSF133(filename, fiscal_year, fiscal_period, force_load=False):
    interface = ValidatorValidationInterface()
    model = SF133

    existing_records = interface.session.query(model).filter(
        model.fiscal_year == fiscal_year, model.period == fiscal_period)
    if force_load:
        # force a reload of this period's current data
        logger.info('Force SF 133 load: deleting existing records for {} {}'.format(
            fiscal_year, fiscal_period))
        delete_count = existing_records.delete()
        interface.session.commit()
        logger.info('{} records deleted'.format(delete_count))
    elif existing_records.count():
        # if there's existing data & we're not forcing a load, skip
        logger.info('SF133 {} {} already in database ({} records). Skipping file.'.format(
            fiscal_year, fiscal_period, existing_records.count()))
        return

    data = pd.read_csv(filename, dtype=str, keep_default_na=False)
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
        {"allocation_transfer_agency": {"pad_to_length": 3, "keep_null": True},
         "agency_identifier": {"pad_to_length": 3},
         "main_account_code": {"pad_to_length": 4},
         "sub_account_code": {"pad_to_length": 3},
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

    # zero out line numbers not supplied in the file
    pivot_idx = ['created_at', 'updated_at', 'agency_identifier', 'allocation_transfer_agency',
                 'availability_type_code', 'beginning_period_of_availa', 'ending_period_of_availabil',
                 'main_account_code', 'sub_account_code', 'tas', 'fiscal_year', 'period']
    data.amount = data.amount.astype(float)  # this line triggers the settingwithcopy warning
    data = pd.pivot_table(data, values='amount', index=pivot_idx, columns=['line'], fill_value=0).reset_index()
    data = pd.melt(data, id_vars=pivot_idx, value_name='amount')

    # Now that we've added zero lines for EVERY tas and SF 133 line number, get rid of the ones
    # we don't actually use in the validations. Arguably, it would be better just to include
    # everything, but that drastically increases the number of records we're inserting to the
    # sf_133 table. If we ever decide that we need *all* SF 133 lines that are zero value,
    # uncomment the next line.
    sf_133_validation_lines = [
        '1000', '1010', '1011', '1012', '1013', '1020', '1021', '1022',
        '1023', '1024', '1025', '1026', '1029', '1030', '1031', '1032',
        '1033', '1040', '1041', '1042', '1160', '1180', '1260', '1280',
        '1340', '1440', '1540', '1640', '1750', '1850', '1910', '2190',
        '2490', '2500', '3020', '4801', '4802', '4881', '4882', '4901',
        '4902', '4908', '4981', '4982'
    ]
    data = data[(data.line.isin(sf_133_validation_lines)) | (data.amount != 0)]

    # TODO: very ugly function below...is there a better way?
    data = data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)
    # insert to db
    table_name = model.__table__.name
    num = LoaderUtils.insertDataframe(data, table_name, interface.engine)
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
