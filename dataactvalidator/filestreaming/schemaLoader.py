import csv
import os
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from fieldCleaner import FieldCleaner

class SchemaLoader(object):

    """
    This class will load a schema file and writes the set of validation rules to validation database.

    """
    fieldFiles = {"appropriations":"appropFields.csv","award":"awardFields.csv","award_financial":"awardFinancialFields.csv","program_activity":"programActivityFields.csv"}

    @staticmethod
    def loadFields(fileTypeName,schemaFileName):
        """
        Load schema file to create validation rules and removes existing
        schemas

        Arguments:
        schemaFileName -- filename of csv file that holds schema definition
        fileTypeName --  the type of file that the schema represents
        """

        #Step 1 Clean out the database
        database = ValidatorValidationInterface()
        database.removeColumnsByFileType(fileTypeName)

        #Step 2 add the new fields
        with open(schemaFileName, 'rU') as csvfile:
            reader = csv.DictReader(csvfile)
            for record in reader:
                record = FieldCleaner.cleanRecord(record)

                if(LoaderUtils.checkRecord(record, ["fieldname","required","data_type"])) :
                    database.addColumnByFileType(
                        fileTypeName,
                        FieldCleaner.cleanString(record["fieldname"]),
                        FieldCleaner.cleanString(record["fieldname_short"]),
                        record["required"],
                        record["data_type"],
                        record["padded_flag"],
                        record["field_length"])
                else :
                   raise ValueError('CSV File does not follow schema')
        # Commit fields
        database.session.commit()

    @classmethod
    def loadAllFromPath(cls,path):
        # Load field definitions into validation DB
        for key in cls.fieldFiles:
            filepath = os.path.join(path,cls.fieldFiles[key])
            cls.loadFields(key,filepath)

if __name__ == '__main__':
    SchemaLoader.loadAllFromPath("../config/")
