import os
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactvalidator.filestreaming.csvS3Reader import CsvS3Reader
from dataactcore.models.domainModels import CGAC,ObjectClass,ProgramActivity,SF133
from dataactcore.config import CONFIG_BROKER

def loadCgac(filename):
    LoaderUtils.loadCsv(filename,CGAC,ValidatorValidationInterface(),{"cgac":"cgac_code","agency":"agency_name"},{"cgac_code":{"pad_to_length":3,"skip_duplicates":True}})

def loadObjectClass(filename):
    LoaderUtils.loadCsv(filename,ObjectClass,ValidatorValidationInterface(),{"max_oc_code":"object_class_code","max_object_class_name":"object_class_name"},{"object_class_code":{"skip_duplicates":True}})

def loadSF133(filename):
    """ Load SF133 files, set skipClear to True for second file """
    LoaderUtils.loadCsv(filename,SF133,ValidatorValidationInterface(),{"ata":"allocation_transfer_agency","aid":"agency_identifier","availability_type_code":"availability_type_code","bpoa":"beginning_period_of_availa","epoa":"ending_period_of_availabil","main_account":"main_account_code","sub_account":"sub_account_code","fiscal_year":"fiscal_year","period":"period","line_num":"line","amount_summed":"amount"},{"allocation_transfer_agency":{"pad_to_length":3},"agency_identifier":{"pad_to_length":3},"main_account_code":{"pad_to_length":4},"sub_account_code":{"pad_to_length":3},"amount":{"strip_commas":True}})

def loadProgramActivity(filename):
    LoaderUtils.loadCsv(filename, ProgramActivity, ValidatorValidationInterface(), {"year":"budget_year","agency_id":"agency_id",
        "alloc_id":"allocation_transfer_id","account":"account_number","pa_code":"program_activity_code","pa_name":"program_activity_name"},
        {"program_activity_code":{"pad_to_length":4},"agency_id":{"pad_to_length":3},"account_number":{"pad_to_length":4},"allocation_transfer_id":{"pad_to_length":3}})


def loadDomainValues(basePath, localSFPath = None, localProgramActivity = None):
    """ Load all domain value files, localSFPath is used to point to a SF-133 file, if not provided it will be downloaded from S3  """
    print("Loading CGAC")
    loadCgac(os.path.join(basePath,"cgac.csv"))
    print("Loading object class")
    loadObjectClass(os.path.join(basePath,"object_class.csv"))

    # SF 133 is kept on S3, so need to download that
    reader = CsvS3Reader()

    if localSFPath is not None:
        # Load SF 133 from same path
        print("Loading local SF-133")
        loadSF133(localSFPath)
    else:
        # Download files if using aws, if not they will need to already be in config folder
        print("Loading default SF-133")
        if(CONFIG_BROKER["use_aws"]):
            reader.downloadFile(CONFIG_BROKER["aws_region"],CONFIG_BROKER["aws_bucket"],"/".join([CONFIG_BROKER["sf_133_folder"],CONFIG_BROKER["sf_133_file"]]),os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config",CONFIG_BROKER["sf_133_file"]))

        loadSF133(os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config",CONFIG_BROKER["sf_133_file"]))
    print("Loading program activity")
    if localProgramActivity is not None:
        loadProgramActivity(localProgramActivity)
    else:
        loadProgramActivity(os.path.join(basePath,"program_activity.csv"))


if __name__ == '__main__':
    loadDomainValues(os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config"))