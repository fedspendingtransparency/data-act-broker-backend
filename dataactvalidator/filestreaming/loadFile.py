import os
import pandas as pd
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactcore.models.domainModels import CGAC,ObjectClass,ProgramActivity,SF133
from dataactcore.config import CONFIG_BROKER
from dataactvalidator.filestreaming.csvS3Reader import CsvS3Reader


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
    insertDataframe(data, model.__table__.name, interface.engine)


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
    insertDataframe(data, model.__table__.name, interface.engine)


def loadProgramActivity(filename):
    interface = ValidatorValidationInterface()
    model = ProgramActivity

    # for program activity, delete and replace values??
    interface.session.query(model).delete()
    interface.session.commit()

    data = pd.read_csv(filename, dtype=str)
    # toss out blank rows
    data.dropna(inplace=True)
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
    # insert to db
    insertDataframe(data, model.__table__.name, interface.engine)


def loadSF133(filename):
    interface = ValidatorValidationInterface()
    model = SF133

    # TODO: skip all this if period is already loaded. force load a monthly update if necessary

    data = pd.read_csv(filename, dtype=str)
    # Fix up cells that have spaces instead of being empty.
    # Set the truly empty cells to None so they get inserted to db as NULL
    # TODO: very ugly function below...is there a better way?
    data = data.applymap(lambda x: str(x).strip() if len(str(x).strip()) else None)

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
    insertDataframe(data, model.__table__.name, interface.engine)

def insertDataframe(df, table, engine):
    df.to_sql(
        table,
        engine,
        index=False,
        if_exists='append'
    )
    del df

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
        # Load SF 133 from same path
        print("Loading local SF-133")
        loadSF133(localSFPath)
    else:
        # Download files if using aws, if not they will need to already be in config folder
        print("Loading SF-133")
        if(CONFIG_BROKER["use_aws"]):
            reader.downloadFile(CONFIG_BROKER["aws_region"],CONFIG_BROKER["aws_bucket"],"/".join([CONFIG_BROKER["sf_133_folder"],CONFIG_BROKER["sf_133_file"]]),os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config",CONFIG_BROKER["sf_133_file"]))

        loadSF133(os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config",CONFIG_BROKER["sf_133_file"]))


if __name__ == '__main__':
    loadDomainValues(os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config"))
