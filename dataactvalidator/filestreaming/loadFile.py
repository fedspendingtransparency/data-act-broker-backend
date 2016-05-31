import os
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactcore.models.domainModels import CGAC,ObjectClass

def loadCgac(filename):
    LoaderUtils.loadCsv(filename,CGAC,ValidatorValidationInterface(),{"cgac":"cgac_code","agency":"agency_name"},{"cgac_code":{"pad_to_length":3,"skip_duplicates":True}})

def loadObjectClass(filename):
    LoaderUtils.loadCsv(filename,ObjectClass,ValidatorValidationInterface(),{"max_oc_code":"object_class_code","max_object_class_name":"object_class_name"},{"object_class_code":{"skip_duplicates":True}})

def loadDomainValues(basePath):
    loadCgac(os.path.join(basePath,"cgac.csv"))
    loadObjectClass(os.path.join(basePath,"object_class.csv"))

if __name__ == '__main__':
    loadDomainValues("../config/")