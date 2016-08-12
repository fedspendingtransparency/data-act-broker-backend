import os
import pandas as pd
import boto
import glob
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactcore.models.domainModels import CGAC,ObjectClass,ProgramActivity,SF133
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.csvS3Reader import CsvS3Reader
from sqlalchemy import and_


def loadCgac(filename):
    interface = ValidatorValidationInterface()
    model = CGAC

    # for CGAC, delete and replace values
    interface.session.query(model).delete()
    interface.session.commit()

    # read CGAC values from csv
    data = pd.read_csv(filename, dtype=str)
    # toss out rows with missing CGAC codes
    data = data[data['CGAC'].notnull()]
    # clean data
    data = LoaderUtils.cleanData(
        data,
        model,
        {"cgac": "cgac_code", "agency": "agency_name"},
        {"cgac_code": {"pad_to_length": 3, "skip_duplicates": True}}
    )
    # insert to db
    LoaderUtils.insertDataframe(data, model.__table__.name, interface.engine)


def loadObjectClass(filename):
    interface = ValidatorValidationInterface()
    model = ObjectClass

    # for object class, delete and replace values
    interface.session.query(model).delete()
    interface.session.commit()

    data = pd.read_csv(filename, dtype=str)
    # toss out blank rows
    data.dropna(inplace=True)
    data = LoaderUtils.cleanData(
        data,
        model,
        {"max_oc_code":"object_class_code",
         "max_object_class_name": "object_class_name"},
        {"object_class_code": {"skip_duplicates": True}}
    )
    # insert to db
    LoaderUtils.insertDataframe(data, model.__table__.name, interface.engine)


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
         "account_number": {"pad_to_length": 4},
         "allocation_transfer_id": {"pad_to_length": 3}}
    )
    # because we're only loading a subset of program activity info,
    # there will be duplicate records in the dataframe. this is ok,
    # but need to de-duped before the db load.
    data.drop_duplicates(inplace=True)
    # insert to db
    LoaderUtils.insertDataframe(data, model.__table__.name, interface.engine)


def loadSF133(filename, fiscal_year, fiscal_period, force_load=False):
    interface = ValidatorValidationInterface()
    model = SF133

    existing_records = interface.session.query(model).filter(
        and_(model.fiscal_year == fiscal_year, model.period == fiscal_period))
    if force_load:
        # force a reload of this period's current data
        existing_records.delete()
    elif existing_records.count():
        # if there's existing data & we're not forcing a load, skip
        print('{} SF133 {} {} records already loaded. No records inserted to database'.format(
            existing_records, fiscal_year, fiscal_period))
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
         "amount": {"strip_commas": True}}
    )
    # todo: find out how to handle dup rows (e.g., same tas/period/line number)
    # line numbers 2002 and 2012 are the only duped line numbers,
    # and they are not used by the validation rules, so for now
    # just remove them before loading our SF-133 table
    dupe_line_numbers = ['2002', '2102']
    data = data[~data.line.isin(dupe_line_numbers)]

    # get rid of commas in dollar amounts
    data.amount = data.amount.str.replace(",", "")

    # add concatenated TAS field for internal use (i.e., joining to staging tables)
    data['tas'] = data.apply(lambda row: formatInternalTas(row), axis=1)
    # insert to db
    LoaderUtils.insertDataframe(data, model.__table__.name, interface.engine)

    # todo: insert 0 line numbers if necessary for validation rules


def formatInternalTas(row):
    """Concatenate TAS components into a single field for internal use."""
    # This formatting should match formatting in dataactcore.models.stagingModels concatTas
    tas = '{}{}{}{}{}{}{}'.format(
        row['allocation_transfer_agency'] if row['allocation_transfer_agency'] else '000',
        row['agency_identifier'] if row['agency_identifier'] else '000',
        row['beginning_period_of_availa'] if row['beginning_period_of_availa'] else '0000',
        row['ending_period_of_availabil'] if row['ending_period_of_availabil'] else '0000',
        ' ' if pd.isnull(row['availability_type_code']) else row['availability_type_code'],
        row['main_account_code'] if row['main_account_code'] else '0000',
        row['sub_account_code'] if row['sub_account_code'] else '000')
    return tas


def loadDomainValues(basePath, localSFPath = None, localProgramActivity = None):
    """Load all domain value files, localSFPath is used to point to a SF-133 file, if not provided it will be downloaded from S3."""
    print("Loading CGAC")
    loadCgac(os.path.join(basePath,"cgac.csv"))
    print("Loading object class")
    loadObjectClass(os.path.join(basePath,"object_class.csv"))
    print("Loading program activity")
    if localProgramActivity is not None:
        loadProgramActivity(localProgramActivity)
    else:
        loadProgramActivity(os.path.join(basePath, "program_activity.csv"))

    # SF 133 is kept on S3, so need to download that
    reader = CsvS3Reader()

    if localSFPath is not None:
        print("Loading local SF-133")
        # get list of SF 133 files in the specified local directory
        sf133Files = glob.glob(os.path.join(localSFPath, 'sf_133*.csv'))
        for sf133 in sf133Files:
            file = os.path.basename(sf133).replace('.csv', '')
            fileParts = file.split('_')
            if len(fileParts) < 4:
                print('Skipping SF 133 file with invalid name: {}'.format(sf133))
                continue
            year = file.split('_')[-2]
            period = file.split('_')[-1]
            print('Starting {}...'.format(sf133))
            loadSF133(sf133, year, period)
    else:
        print("Loading SF-133")
        if(CONFIG_BROKER["use_aws"]):
            # get list of SF 133 files in the config bucket on S3
            s3connection = boto.s3.connect_to_region(CONFIG_BROKER['aws_region'])
            s3bucket = s3connection.lookup(CONFIG_BROKER['aws_bucket'])
            # get bucketlistresultset with all sf_133 files
            sf133Files = s3bucket.list(
                prefix='{}/sf_133'.format(CONFIG_BROKER['sf_133_folder']))
            for sf133 in sf133Files:
                file = sf133.name.split(CONFIG_BROKER['sf_133_folder'])[-1].replace('.csv', '')
                fileParts = file.split('_')
                if len(fileParts) < 4:
                    print('Skipping SF 133 file with invalid name: {}'.format(sf133))
                    continue
                year = file.split('_')[-2]
                period = file.split('_')[-1]
                print('Starting {}...'.format(sf133.name))
                loadSF133(sf133, year, period)

if __name__ == '__main__':
    loadDomainValues(
        os.path.join(CONFIG_BROKER["path"], "dataactvalidator", "config"))
