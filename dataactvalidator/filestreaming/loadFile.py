import os
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactvalidator.filestreaming.csvS3Reader import CsvS3Reader
from dataactcore.models.domainModels import CGAC,ObjectClass,SF133
from dataactcore.config import CONFIG_BROKER

def loadCgac(filename):
    LoaderUtils.loadCsv(filename,CGAC,ValidatorValidationInterface(),{"cgac":"cgac_code","agency":"agency_name"},{"cgac_code":{"pad_to_length":3,"skip_duplicates":True}})

def loadObjectClass(filename):
    LoaderUtils.loadCsv(filename,ObjectClass,ValidatorValidationInterface(),{"max_oc_code":"object_class_code","max_object_class_name":"object_class_name"},{"object_class_code":{"skip_duplicates":True}})

def loadSF133(filename,skipClear = False):
    """ Load SF133 files, set skipClear to True for second file """
    LoaderUtils.loadCsv(filename,SF133,ValidatorValidationInterface(),{"ata":"allocationtransferagencyidentifier","aid":"agencyidentifier","availability_type_code":"availabilitytypecode","bpoa":"beginningperiodofavailability","epoa":"endingperiodofavailability","main_account":"mainaccountcode","sub_account":"subaccountcode","fiscal_year":"fiscal_year","period":"period","line_num":"line","amount_summed":"amount"},{"allocationtransferagencyidentifier":{"pad_to_length":3},"agencyidentifier":{"pad_to_length":3},"mainaccountcode":{"pad_to_length":4},"subaccountcode":{"pad_to_length":3}},skipClear=skipClear)

def loadDomainValues(basePath):
    loadCgac(os.path.join(basePath,"cgac.csv"))
    loadObjectClass(os.path.join(basePath,"object_class.csv"))
    # SF 133 is kept on S3, so need to download that
    reader = CsvS3Reader()
    # Download files if using aws, if not they will need to already be in broker_files location
    if(CONFIG_BROKER["use_aws"]):
        print("Pulling values from config: " + str(CONFIG_BROKER["aws_region"]) + " " + str(CONFIG_BROKER["aws_bucket"]) + " " + str(CONFIG_BROKER["sf_133_folder"]) + " " + str(CONFIG_BROKER["sf_133_file_one"]))
        reader.downloadFile(CONFIG_BROKER["aws_region"],CONFIG_BROKER["aws_bucket"],"/".join([CONFIG_BROKER["sf_133_folder"],CONFIG_BROKER["sf_133_file_one"]]),os.path.join(CONFIG_BROKER["broker_files"],CONFIG_BROKER["sf_133_file_one"]))
        reader.downloadFile(CONFIG_BROKER["aws_region"],CONFIG_BROKER["aws_bucket"],"/".join([CONFIG_BROKER["sf_133_folder"],CONFIG_BROKER["sf_133_file_two"]]),os.path.join(CONFIG_BROKER["broker_files"],CONFIG_BROKER["sf_133_file_two"]))

    loadSF133(os.path.join(CONFIG_BROKER["broker_files"],CONFIG_BROKER["sf_133_file_one"]))
    loadSF133(os.path.join(CONFIG_BROKER["broker_files"],CONFIG_BROKER["sf_133_file_two"]),True)

if __name__ == '__main__':
    loadDomainValues(os.path.join(CONFIG_BROKER["path"],"dataactvalidator","config"))