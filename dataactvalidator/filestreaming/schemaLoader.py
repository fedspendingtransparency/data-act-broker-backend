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
                            database.addRule(columnId,"LENGTH",length,"","Field must be no longer than specified limit",originalLabel="")
                else :
                   raise ValueError('CSV File does not follow schema')

    @staticmethod
    def loadSqlRules(filename):
        """ Populate SQL rule table from file."""
        validationDb = ValidatorValidationInterface()

        # delete existing sql rules
        validationDb.deleteSqlRules()

        # add sql rules in the validator's config folder
        with open(filename, 'rU') as sqlRuleFile:
            reader = csv.DictReader(sqlRuleFile)
            for record in reader:
                # look up file type id
                try:
                    fileId = validationDb.getFileId(FieldCleaner.cleanString(record["file_type"]))
                except Exception as e:
                    raise Exception("{}: file type={}, rule label={}. Rule not loaded.".format(
                        e, record["file_type"], record["rule_label"]))
                # set cross file flag
                if (FieldCleaner.cleanString(record["rule_cross_file_flag"])
                    in ['true', 't', 'y', 'yes']):
                    cross_file_flag = True
                else:
                    cross_file_flag = False
                # look up severity
                try:
                    severity = validationDb.getRuleSeverityByName(record["severity_name"])
                except Exception as e:
                    raise Exception("{}: severity={}, rule label={}. Rule not loaded.".format(
                        e, record["severity_name"], record["rule_label"]))
                # insert sql rule record
                try:
                    validationDb.addSqlRule(record["rule_sql"],
                        record["rule_label"], record["rule_description"],
                        record["rule_error_message"], fileId, severity,
                        cross_file_flag)
                except Exception as e:
                    raise Exception("{}: sql rule insert failed (file={}, label={}, rule={})".format(
                            e, fileId, record["rule_label"], record["rule_description"]))

    @staticmethod
    def loadRules(fileTypeName, filename):
        """ Populate rule table from rule rile

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
                else:
                    # Multi field rules don't get a file_column
                    columnId = None
                # Look up rule timing id
                try:
                    ruleTimingId = validationDb.getRuleTimingIdByName(
                        FieldCleaner.cleanName(record["rule_timing"]))
                except Exception as e:
                    raise Exception("".join(
                        [str(e), " Rule load failed on timing value ", FieldCleaner.cleanName(record["rule_timing"]), " and file ",
                         fileTypeName]))
                # Target file info is applicable to cross-file rules only
                targetFileId = None
                # Write to rule table
                try:
                    validationDb.addRule(columnId,
                        str(record["rule_type"]), str(record["rule_text_one"]),
                        str(record["rule_text_two"]), str(record["description"]),
                        ruleTimingId, str(record["rule_label"]),
                        targetFileId=targetFileId, fileId=fileId, originalLabel=record["original_label"])
                except Exception as e:
                    raise Exception('{}: rule insert failed (file={}, rule={}'.format(
                        e, fileTypeName, record["description"]))

    @staticmethod
    def loadCrossRules(filename):
        """ Populate rule table with cross file validation rules """
        validationDb = ValidatorValidationInterface()
        with open(filename, 'rU') as ruleFile:
            reader = csv.DictReader(ruleFile)
            for record in reader:
                fileId = validationDb.getFileId(record["file"])
                if record["target_file"]:
                    targetFileId = validationDb.getFileId(record["target_file"])
                else:
                    targetFileId = None
                # Look up rule timing id
                try:
                    ruleTimingId = validationDb.getRuleTimingIdByName(
                        FieldCleaner.cleanName(record["rule_timing"]))
                except Exception as e:
                    raise Exception("".join(
                        [str(e), "Cross-file rule load failed on timing value ", FieldCleaner.cleanName(record["rule_timing"]),
                         " and file ",
                         fileTypeName]))
                try:
                    validationDb.addRule(
                        None, record["rule_type"], record["rule_text_one"],
                        record["rule_text_two"], record["description"], ruleTimingId,
                        record["rule_label"], targetFileId, fileId = fileId, originalLabel=record["original_label"])
                except Exception as e:
                    raise Exception('{}: cross-file rule insert failed (rule={}'.format(
                        e, record["description"]))

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
        # Load SQL validation rules
        filePath = os.path.join(path, "sqlRules.csv")
        cls.loadSqlRules(filePath)

if __name__ == '__main__':
    SchemaLoader.loadAllFromPath("../config/")
