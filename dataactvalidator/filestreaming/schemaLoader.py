import csv
from dataactvalidator.interfaces.validatorValidationInterface import ValidatorValidationInterface
from dataactvalidator.filestreaming.loaderUtils import LoaderUtils
from fieldCleaner import FieldCleaner

class SchemaLoader(object):

    """
    This class will load a schema file and writes the set of validation rules to validation database.

    """

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
                        print("Failed on field " + FieldCleaner.cleanName(record["field_name"]) + " and file " + fileTypeName)
                        raise e
                    # Write to rule table
                    if "rule_timing" in record and "rule_label" in record:
                        validationDb.addRule(columnId,record["rule_type"],record["rule_text_one"],record["description"],record["rule_timing"],record["rule_label"])
                    else:
                        validationDb.addRule(columnId,record["rule_type"],record["rule_text_one"],record["description"])
                else:
                    # Write to multi_field_rule table
                    validationDb.addMultiFieldRule(fileId,record["rule_type"],record["rule_text_one"],record["rule_text_two"],record["description"])

    @staticmethod
    def loadCrossRules(filename):
        """ Populate multifield rule table with cross file validation rules """
        validationDb = ValidatorValidationInterface()
        with open(filename, 'rU') as ruleFile:
            reader = csv.DictReader(ruleFile)
            for record in reader:
                fileId = validationDb.getFileId(record["file"])
                validationDb.addMultiFieldRule(fileId,record["rule_type"],record["rule_text_one"],record["rule_text_two"],record["description"],record["rule_label"],record["rule_timing"])

if __name__ == '__main__':
    # Load field definitions and rules into validation DB
    SchemaLoader.loadFields("appropriations","../config/appropFields.csv")
    SchemaLoader.loadFields("award","../config/awardFields.csv")
    SchemaLoader.loadFields("award_financial","../config/awardFinancialFields.csv")
    SchemaLoader.loadFields("program_activity","../config/programActivityFields.csv")
    # Load rules files
    SchemaLoader.loadRules("appropriations","../config/appropRules.csv")
    #SchemaLoader.loadRules("award","../config/awardRules.csv")
    SchemaLoader.loadRules("award_financial","../config/awardFinancialRules.csv")
    SchemaLoader.loadRules("program_activity","../config/programActivityRules.csv")
    # Load cross file validation rules
    SchemaLoader.loadCrossRules("../config/crossFileRules.csv")