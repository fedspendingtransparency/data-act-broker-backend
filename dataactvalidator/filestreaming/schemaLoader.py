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
    ruleFiles = {"appropriations":"appropRules.csv","award_financial":"awardFinancialRules.csv","program_activity":"programActivityRules.csv"}

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
        database.removeRulesByFileType(fileTypeName)
        database.removeColumnsByFileType(fileTypeName)

        #Step 2 add the new fields
        with open(schemaFileName, 'rU') as csvfile:
            reader = csv.DictReader(csvfile)
            for record in reader:
                record = FieldCleaner.cleanRecord(record)
                if(LoaderUtils.checkRecord(record, ["fieldname","required","data_type"])) :
                    columnId = database.addColumnByFileType(fileTypeName,FieldCleaner.cleanString(record["fieldname"]),record["required"],record["data_type"])
                    if "field_length" in record:
                        # When a field length is specified, create a rule for it
                        length = record["field_length"].strip()
                        if(len(length) > 0):
                            # If there are non-whitespace characters here, create a length rule
                            database.addRule(columnId,"LENGTH",length,"Field must be no longer than specified limit")
                else :
                   raise ValueError('CSV File does not follow schema')

    @staticmethod
    def loadRules(fileTypeName, filename):
        """ Populate rule and multi_field_rule tables from rule rile

        Args:
            filename: File with rule specifications
            fileTypeName: Which type of file to load rules for
        """
        validationDb = ValidatorValidationInterface()
        fileId = validationDb.getFileId(fileTypeName)

        with open(filename, 'rU') as ruleFile:
            reader = csv.DictReader(ruleFile)
            for record in reader:
                if(FieldCleaner.cleanString(record["is_single_field"]) == "true"):
                    # Find column ID based on field name
                    try:
                        columnId = validationDb.getColumnId(FieldCleaner.cleanName(record["field_name"]),fileTypeName)
                    except Exception as e:
                        raise Exception("".join([str(e),"Failed on field ",FieldCleaner.cleanName(record["field_name"])," and file ",fileTypeName]))
                    # Write to rule table
                    if "rule_timing" in record and "rule_label" in record:
                        validationDb.addRule(columnId,str(record["rule_type"]),str(record["rule_text_one"]),str(record["description"]),str(record["rule_timing"]),str(record["rule_label"]))
                    else:
                        validationDb.addRule(columnId,str(record["rule_type"]),str(record["rule_text_one"]),str(record["description"]))
                else:
                    # Write to multi_field_rule table
                    validationDb.addMultiFieldRule(fileId,str(record["rule_type"]),str(record["rule_text_one"]),str(record["rule_text_two"]),str(record["description"]),ruleTiming = str(record["rule_timing"]),ruleLabel=str(record["rule_label"]))

    @staticmethod
    def loadCrossRules(filename):
        """ Populate multifield rule table with cross file validation rules """
        validationDb = ValidatorValidationInterface()
        with open(filename, 'rU') as ruleFile:
            reader = csv.DictReader(ruleFile)
            for record in reader:
                fileId = validationDb.getFileId(record["file"])
                if record["target_file"]:
                    targetFileId = validationDb.getFileId(record["target_file"])
                else:
                    targetFileId = None
                validationDb.addMultiFieldRule(
                    fileId, record["rule_type"], record["rule_text_one"],
                    record["rule_text_two"], record["description"],
                    record["rule_label"], record["rule_timing"], targetFileId)

    @classmethod
    def loadAllFromPath(cls,path):
        # Load field definitions into validation DB
        for key in cls.fieldFiles:
            filepath = os.path.join(path,cls.fieldFiles[key])
            cls.loadFields(key,filepath)
        # Load rules files
        for key in cls.ruleFiles:
            filepath = os.path.join(path,cls.ruleFiles[key])
            cls.loadRules(key,filepath)
        # Load cross file validation rules
        filePath = os.path.join(path,"crossFileRules.csv")
        cls.loadCrossRules(filePath)

if __name__ == '__main__':
    SchemaLoader.loadAllFromPath("../config/")
