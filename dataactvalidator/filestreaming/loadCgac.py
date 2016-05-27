from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactcore.models.domainModels import CGAC

def loadCgac(filename):
    LoaderUtils.loadCsv(filename,CGAC,ValidatorValidationInterface(),{"cgac":"cgac_code","agency":"agency_name"},{"cgac_code":{"pad_to_length":3,"skip_duplicates":True}})

if __name__ == '__main__':
    loadCgac("../config/cgac.csv")