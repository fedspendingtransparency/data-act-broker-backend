import os
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactcore.models.domainModels import CGAC,ObjectClass,ProgramActivity

def loadCgac(filename):
    LoaderUtils.loadCsv(filename,CGAC,ValidatorValidationInterface(),{"cgac":"cgac_code","agency":"agency_name"},{"cgac_code":{"pad_to_length":3,"skip_duplicates":True}})

def loadObjectClass(filename):
    LoaderUtils.loadCsv(filename,ObjectClass,ValidatorValidationInterface(),{"max_oc_code":"object_class_code","max_object_class_name":"object_class_name"},{"object_class_code":{"skip_duplicates":True}})

def loadProgramActivity(filename):
    LoaderUtils.loadCsv(filename, ProgramActivity, ValidatorValidationInterface(), {"year":"budget_year","agency_id":"agency_id",
        "alloc_id":"allocation_transfer_id","account":"account_number","pa_code":"program_activity_code","pa_name":"program_activity_name"},
        {"program_activity_code":{"pad_to_length":4},"agency_id":{"pad_to_length":3},"account_number":{"pad_to_length":4},"allocation_transfer_id":{"pad_to_length":3}})

def loadDomainValues(basePath):
    loadCgac(os.path.join(basePath,"cgac.csv"))
    loadObjectClass(os.path.join(basePath,"object_class.csv"))
    loadProgramActivity(os.path.join(basePath,"program_activity.csv"))

if __name__ == '__main__':
    loadDomainValues("../config/")