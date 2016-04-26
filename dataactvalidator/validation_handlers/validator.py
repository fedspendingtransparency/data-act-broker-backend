import re
from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import reflection
from decimal import *
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactvalidator.models.validationModels import TASLookup
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder

class Validator(object):
    """
    Checks individual records against specified validation tests
    """
    BOOLEAN_VALUES = ["TRUE","FALSE","YES","NO","1","0"]

    @classmethod
    def crossValidate(cls,rules, submissionId):
        """ Evaluate all rules for cross file validation

        Args:
            rules -- List of MultiFieldRule objects
        """
        failures = []
        # Put each rule through evaluate, appending all failures into list
        for rule in rules:
            (passed, ruleFailures) = cls.evaluateCrossFileRule(rule, submissionId)
            if not passed:
                failures.extend(ruleFailures)
        # Return list of cross file validation failures
        return failures

    @staticmethod
    def getTable(submissionId, fileType, stagingDb):
        """ Get ORM table based on submission ID and file type """
        meta = MetaData(bind=stagingDb.engine)
        # Get name of staging tables
        tableName = stagingDb.getTableNameBySubmissionId(submissionId,fileType)
        # Create ORM table from database
        table = Table(tableName, meta, autoload=True, autoload_with=stagingDb.engine)
        return table

    @staticmethod
    def getRecordsIfNone(sourceTable, stagingDb, record = None):
        """ If record is None, load all records from sourceTable, otherwise return record in list """
        if record:
            sourceRecords = [record]
        else:
            # If no record provided, get list of all entries in first table
            sourceRecords = stagingDb.session.query(sourceTable).all()
        return sourceRecords
    @classmethod
    def evaluateCrossFileRule(cls, rule, submissionId, record = None):
        """ Evaluate specified rule against all records to which it applies """
        failures = [] # Can get multiple failures for these rule types
        rulePassed = True # Set to false on first failures
        # Get rule type
        ruleType = rule.multi_field_rule_type.name.lower()
        fileType = rule.file_type.name
        interfaces = InterfaceHolder()
        stagingDb = interfaces.stagingDb
        if ruleType == "field_match":
            targetType = rule.rule_text_2
            # Get ORM objects for source and target staging tables
            sourceTable = cls.getTable(submissionId, fileType, stagingDb)
            targetTable = cls.getTable(submissionId, targetType, stagingDb)
            # TODO Could try to do a join and see what doesn't match, or otherwise improve performance by avoiding a
            # TODO new query against second table for every record in first table, possibly index second table at start
            # Can apply rule to a specified record or all records in first table
            sourceRecords = cls.getRecordsIfNone(sourceTable,stagingDb,record)
            fieldsToCheck = cls.cleanSplit(rule.rule_text_1,True)
            # For each entry, check for the presence of matching values in second table
            for record in sourceRecords:
                # Build query to filter for each field to match
                matchDict = {}
                query = stagingDb.session.query(targetTable)
                for field in fieldsToCheck:
                    # Have to get file column IDs for source and target tables
                    sourceColId = interfaces.validationDb.getColumnId(field,fileType)
                    targetColId = interfaces.validationDb.getColumnId(field,targetType)
                    matchDict[field] = getattr(record,str(sourceColId))
                    query = query.filter(getattr(targetTable.c,str(targetColId)) == matchDict[field])
                # Make sure at least one in target table record matches
                if not query.first():
                    # Fields don't match target file, add to failures
                    rulePassed = False
                    failures.append([fieldsToCheck,rule.description,str(matchDict)])
        elif ruleType == "rule_if":
            # Get all records from source table
            sourceTable = cls.getTable(submissionId, fileType, stagingDb)

            columns = list(sourceTable.columns)
            colNames = []
            print("columns is: " + str(type(columns)))
            print("column is: " + str(type(columns[0])))
            print("name is: " + str(columns[0].name))
            for i in range(0,len(columns)):
                colNames.append(interfaces.validationDb.getFieldNameByColId(columns[i].name))
            print("column names: " + str(colNames))


            # Can apply rule to a specified record or all records in first table
            sourceRecords = cls.getRecordsIfNone(sourceTable,stagingDb,record)
            # Get both rules, condition to check and rule to apply based on condition
            condition = interfaces.validationDb.getMultiFieldRuleByLabel(rule.rule_text_2)
            conditionalRule = interfaces.validationDb.getMultiFieldRuleByLabel(rule.rule_text_1)
            # Apply first rule for all records that pass second rule
            for record in sourceRecords:
                # Record is a tuple, we need it to be a dict with field names as keys
                print("record from source:" + str(record))
                recordDict = dict(zip(colNames,list(record)))
                print("recordDict: " + str(recordDict))
                if cls.evaluateCrossFileRule(condition,submissionId,recordDict)[0]:
                    result = cls.evaluateCrossFileRule(conditionalRule,submissionId,recordDict)
                    if not result[0]:
                        # Record if we have seen a failure
                        rulePassed = False
                        failures = failures.extend(result[1])
        elif ruleType == "greater":
            if not record:
                # Must provide a record for this rule
                raise ValueError("Cannot apply greater rule without a record")
            rulePassed = getattr(record,rule.rule_text_2) > rule.rule_text_1
        return rulePassed,failures

    @staticmethod
    def validate(record,rules,csvSchema,fileType,interfaces):
        """
        Args:
        record -- dict represenation of a single record of data
        rules -- list of rule Objects
        csvSchema -- dict of schema for the current file.
        fileType -- name of file type to check against

        Returns:
        True if validation passed, False if failed, and list of failed rules, each with field, description of failure, and value that failed
        """
        recordFailed = False
        failedRules = []
        for fieldName in csvSchema :
            if(csvSchema[fieldName].required and  not fieldName in record ):
                return False, [[fieldName, ValidationError.requiredError, ""]]

        for fieldName in record :
            checkRequiredOnly = False
            currentSchema =  csvSchema[fieldName]
            ruleSubset = Validator.getRules(fieldName, fileType, rules,interfaces.validationDb)
            currentData = record[fieldName]
            if(currentData != None):
                currentData = currentData.strip()

            if(currentData == None or len(currentData) == 0):
                if(currentSchema.required ):
                    # If empty and required return field name and error
                    recordFailed = True
                    failedRules.append([fieldName, ValidationError.requiredError, ""])
                    continue
                else:
                    #if field is empty and not required its valid
                    checkRequiredOnly = True

            # Always check the type in the schema
            if(not checkRequiredOnly and not Validator.checkType(currentData,currentSchema.field_type.name) ) :
                recordFailed = True
                failedRules.append([fieldName, ValidationError.typeError, currentData])
                # Don't check value rules if type failed
                continue

            # Check for a type rule in the rule table, don't want to do value checks if type is not correct
            typeFailed = False
            for currentRule in ruleSubset:
                if(checkRequiredOnly):
                    # Only checking conditional requirements
                    continue
                if(currentRule.rule_type.name == "TYPE"):
                    if(not Validator.checkType(currentData,currentRule.rule_text_1) ) :
                        recordFailed = True
                        typeFailed = True
                        failedRules.append([fieldName, ValidationError.typeError, currentData])
            if(typeFailed):
                # If type failed, don't do value checks
                continue
            #Field must pass all rules
            for currentRule in ruleSubset :
                if(checkRequiredOnly and currentRule.rule_type_id != interfaces.validationDb.getRuleType("REQUIRED_CONDITIONAL")):
                    # If data is empty, only check conditional required rules
                    continue
                if(not Validator.evaluateRule(currentData,currentRule,currentSchema.field_type.name,interfaces,record)):
                    recordFailed = True
                    failedRules.append([fieldName,"".join(["Failed rule: ",str(currentRule.description)]), currentData])
        # Check all multi field rules for this file type
        multiFieldRules = interfaces.validationDb.getMultiFieldRulesByFile(fileType)
        for rule in multiFieldRules:
            if not Validator.evaluateMultiFieldRule(rule,record,interfaces,fileType):
                recordFailed = True
                failedRules.append(["MultiField", "".join(["Failed rule: ",str(rule.description)]), Validator.getMultiValues(rule,record)])


        return (not recordFailed), failedRules

    @staticmethod
    def getRules(fieldName, fileType,rules,validationInterface) :
        """ From a given set of rules, create a list of only the rules that apply to specified field during file validation

        Args:
            fieldName: Field to find rules for
            fileType: Name of file to check against
            rules: Original set of rules
            validationInterface: interface for validation DB

        Returns:
            List of rules that apply to specified field
        """
        fileId = validationInterface.getFileId(fileType)
        returnList =[]
        for rule in rules :
            if(rule.file_column.name == fieldName and rule.file_column.file_id == fileId and rule.rule_timing_id == validationInterface.getRuleTimingIdByName("file_validation")) :
                returnList.append(rule)
        return returnList

    @staticmethod
    def checkType(data,datatype) :
        """ Determine whether data is of the correct type

        Args:
            data: Data to be checked
            datatype: Type to check against

        Returns:
            True if data is of specified type, False otherwise
        """
        if(data.strip() == ""):
            # An empty string matches all types
            return True
        if(datatype == "STRING") :
            return(len(data) > 0)
        if(datatype == "BOOLEAN") :
            if(data.upper() in Validator.BOOLEAN_VALUES) :
                return True
            return False
        if(datatype == "INT") :
            try:
                int(data)
                return True
            except:
                return False
        if(datatype == "DECIMAL") :
            try:
                Decimal(data)
                return True
            except:
                return False
        if(datatype == "LONG"):
            try:
                long(data)
                return True
            except:
                return False
        raise ValueError("".join(["Data Type Error, Type: ",datatype,", Value: ",data]))

    @staticmethod
    def getIntFromString(data) :
        """ Convert string to int, converts to float first to avoid exceptions when data is represented as float """
        return int(float(data))

    @staticmethod
    def getType(data,datatype) :
        """ Convert data into specified type

        Args:
            data: Data to be converted
            datatype: Type to conert into

        Returns:
            Data in specified type
        """
        if(datatype =="INT") :
            return int(float(data))
        if(datatype =="DECIMAL") :
            return Decimal(data)
        if(datatype == "STRING" or datatype =="BOOLEAN") :
            return data
        if(datatype == "LONG"):
            return long(data)
        raise ValueError("Data Type Invalid")

    @staticmethod
    def evaluateRule(data,rule,datatype,interfaces,record):
        """ Checks data against specified rule

        Args:
            data: Data to be checked
            rule: Rule object to test against
            datatype: Type to convert data into

        Returns:
            True if rule passed, False otherwise
        """
        if data is None:
            # Treat blank as an empty string
            data = ""
        value1 = rule.rule_text_1
        currentRuleType = rule.rule_type.name
        if(currentRuleType =="LENGTH") :
            return len(data.strip()) <= Validator.getIntFromString(value1)
        elif(currentRuleType =="LESS") :
            return Validator.getType(data,datatype) < Validator.getType(value1,datatype)
        elif(currentRuleType =="GREATER") :
            return Validator.getType(data,datatype) > Validator.getType(value1,datatype)
        elif(currentRuleType =="EQUAL") :
            return Validator.getType(data,datatype) == Validator.getType(value1,datatype)
        elif(currentRuleType =="NOT EQUAL") :
            return not (Validator.getType(data,datatype) == Validator.getType(value1,datatype))
        elif(currentRuleType == "TYPE"):
            # Type checks happen earlier, but type rule is still included in rule set, so skip it
            return True
        elif(currentRuleType == "IN_SET"):
            setList = Validator.cleanSplit(value1,toLower = False)
            return (data in setList)
        elif(currentRuleType == "MIN LENGTH"):
            return len(data.strip()) >= Validator.getIntFromString(value1)
        elif(currentRuleType == "REQUIRED_CONDITIONAL"):
            return Validator.conditionalRequired(data,rule,datatype,interfaces,record)
        raise ValueError("Rule Type Invalid")

    @staticmethod
    def conditionalRequired(data,rule,datatype,interfaces,record):
        """ If conditional rule passes, data must not be empty """
        # Get rule object for conditional rule
        conditionalRule = interfaces.validationDb.getRuleByLabel(rule.rule_text_1)
        conditionalTypeId = conditionalRule.file_column.field_types_id
        conditionalDataType = interfaces.validationDb.getFieldTypeById(conditionalTypeId)
        # If conditional rule passes, check that data is not empty
        if Validator.evaluateRule(record[conditionalRule.file_column.name],conditionalRule,conditionalDataType,interfaces,record):
            result = not (data is None or data == "")
            return result
        else:
            # If conditional rule fails, this field is not required, so the condtional requirement passes
            return True

    @staticmethod
    def evaluateMultiFieldRule(rule, record, interfaces, fileType):
        """ Check a rule involving more than one field of a record

        Args:
            rule: MultiFieldRule object to check against
            record: Record to be checked
            fileType: File type being checked

        Returns:
            True if rule passes, False otherwise
        """
        ruleType = rule.multi_field_rule_type.name.upper()
        if(ruleType == "CAR_MATCH"):
            # Look for an entry in car table that matches all fields
            fieldsToCheck = Validator.cleanSplit(rule.rule_text_1)
            tasFields = Validator.cleanSplit(rule.rule_text_2)
            if(len(fieldsToCheck) != len(tasFields)):
                raise ResponseException("Number of fields to check does not match number of fields checked against",StatusCode.CLIENT_ERROR,ValueError)
            return Validator.validateTAS(fieldsToCheck, tasFields, record, interfaces, fileType)
        else:
            raise ResponseException("Bad rule type for multi-field rule",StatusCode.INTERNAL_ERROR)

    @staticmethod
    def cleanSplit(string, toLower = True):
        """ Split string on commas and remove whitespace around each element"""
        stringList = string.split(",")
        for i in range(0,len(stringList)):
            stringList[i] = stringList[i].strip()
            if(toLower):
                stringList[i] = stringList[i].lower()
        return stringList

    @staticmethod
    def getMultiValues(rule,record):
        """ Create string out of values for all fields involved in this rule

        Args:
            rule: Rule to return fields for
            record: Record to pull values from

        Returns:
            One string including all values involved in this rule check
        """
        fields = Validator.cleanSplit(rule.rule_text_1)
        output = ""
        for field in fields:
            value = record[field]
            if(value == None):
                # For concatenating fields, represent None with an empty string
                value = ""
            output = "".join([output,field,": ",value,", "])
        return output[:-2]

    @staticmethod
    def validateTAS(fieldsToCheck, tasFields, record, interfaces, fileType):
        """ Check for presence of TAS for specified record in TASLookup table

        Args:
            fieldsToCheck: Set of fields involved in TAS check
            tasFields: Corresponding field names in TASLookup table
            record: Record to check TAS for
            fileType: File type being checked

        Returns:
            True if TAS is in CARS, False otherwise
        """
        query = interfaces.validationDb.session.query(TASLookup)

        for i in range(0,len(fieldsToCheck)):
            data = record[str(fieldsToCheck[i])]
            if(data == None):
                # Set data to empty string so it can be padded with leading zeros
                data = ""
            field = fieldsToCheck[i].lower()
            # Pad field with leading zeros
            length = interfaces.validationDb.getColumnLength(field, fileType)
            data = data.zfill(length)
            query = query.filter(TASLookup.__dict__[tasFields[i]] == data)

        queryResult = query.all()
        if(len(queryResult) == 0):
            # TAS not found, record invalid
            return False
        elif(len(queryResult) == 1):
            # Found a TAS match
            return True
        else:
            # Multiple instances of same TAS, something is going wrong
            raise ResponseException("TAS check is malfunctioning",StatusCode.INTERNAL_ERROR)