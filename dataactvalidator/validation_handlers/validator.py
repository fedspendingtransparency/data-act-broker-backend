import json
from sqlalchemy.orm.exc import NoResultFound
from decimal import *
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactcore.models import domainModels
from dataactcore.models.validationModels import RuleSql
from dataactvalidator.validation_handlers.validationError import ValidationError
from dataactcore.models.domainModels import TASLookup
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner
from dataactcore.utils.cloudLogger import CloudLogger

class Validator(object):
    """
    Checks individual records against specified validation tests
    """
    BOOLEAN_VALUES = ["TRUE","FALSE","YES","NO","1","0"]
    tableAbbreviations = {"appropriations":"approp","award_financial_assistance":"afa","award_financial":"af","object_class_program_activity":"op","appropriation":"approp"}
    # Set of metadata fields that should not be directly validated
    META_FIELDS = ["row_number", "is_first_quarter", "agencyidentifier_padded", "allocationtransferagencyidentifier_padded", "mainaccountcode_padded", "subaccountcode_padded", "programactivitycode_padded"]

    @classmethod
    def crossValidateSql(cls, rules, submissionId):
        """ Evaluate all sql-based rules for cross file validation

        Args:
            rules -- List of Rule objects
            submissionId -- ID of submission to run cross-file validation
        """
        failures = []
        # Put each rule through evaluate, appending all failures into list
        interfaces = InterfaceHolder()
        # Get short to long colname dictionary
        shortColnames = interfaces.validationDb.getShortToLongColname()

        for rule in rules:
            failedRows = interfaces.validationDb.connection.execute(
                rule.rule_sql.format(submissionId))
            if failedRows.rowcount:
                # get list of fields involved in this validation
                # note: row_number is metadata, not a field being
                # validated, so exclude it
                cols = failedRows.keys()
                cols.remove('row_number')
                columnString = ", ".join(shortColnames[c] if c in shortColnames else c for c in cols)
                for row in failedRows:
                    # get list of values for each column
                    values = ["{}: {}".format(shortColnames[c], str(row[c])) if c in shortColnames else "{}: {}".format(c, str(row[c])) for c in cols]
                    values = ", ".join(values)
                    targetFileType = interfaces.validationDb.getFileTypeById(rule.target_file_id)
                    failures.append([rule.file.name, targetFileType, columnString,
                        str(rule.rule_description), values, row['row_number'],str(rule.rule_label),rule.file_id,rule.target_file_id])

        # Return list of cross file validation failures
        return failures

    @staticmethod
    def getRecordsIfNone(submissionId, fileType, stagingDb, record=None):
        """Return source record or records needed for cross-file validation.

        Args:
            submissionId - ID of submission being validated (int)
            fileType - fileType of source record(s) being validated (string)
            stagingDb - staging db interface (staging tables are in the validation db)
            record - set to none if we want all table records for submission

        Returns:
            sourceRecords - a list of dictionaries, each representing
            a row in the staging table that corresponds to the fileType
        """
        if record:
            sourceRecords = [record]
        else:
            # If no record provided, get list of all entries in first table
            sourceRecords = stagingDb.getSubmissionsByFileType(submissionId, fileType).all()
            sourceRecords = [r.__dict__ for r in sourceRecords]
        return sourceRecords

    @classmethod
    def validate(cls,record,rules,csvSchema,fileType,interfaces):
        """
        Args:
        record -- dict representation of a single record of data
        rules -- list of rule Objects
        csvSchema -- dict of schema for the current file.
        fileType -- name of file type to check against

        Returns:
        Tuple of three values:
        True if validation passed, False if failed
        List of failed rules, each with field, description of failure, and value that failed
        True if type check passed, False if type failed
        """
        recordFailed = False
        recordTypeFailure = False
        failedRules = []

        # Get short to long colname dictionary
        shortColnames = interfaces.validationDb.getShortToLongColname()

        for fieldName in csvSchema :
            #todo: check short colnames here
            if(csvSchema[fieldName].required and  not fieldName in record ):
                return False, [[fieldName, ValidationError.requiredError, "", ""]], False

        for fieldName in record :
            if fieldName in cls.META_FIELDS:
                # Skip row number, nothing to validate on that
                continue
            elif fieldName in shortColnames:
                # Change shrot colname to longname for validation
                fieldName = shortColnames[fieldName]
            checkRequiredOnly = False
            currentSchema = csvSchema[fieldName]
            ruleSubset = Validator.getRules(fieldName, fileType, rules,interfaces.validationDb)
            currentData = record[fieldName]
            if(currentData != None):
                currentData = currentData.strip()

            if(currentData == None or len(currentData) == 0):
                if(currentSchema.required ):
                    # If empty and required return field name and error
                    recordFailed = True
                    failedRules.append([fieldName, ValidationError.requiredError, "", ""])
                    continue
                else:
                    # If field is empty and not required its valid
                    checkRequiredOnly = True

            # Always check the type in the schema
            if(not checkRequiredOnly and not Validator.checkType(currentData,currentSchema.field_type.name) ) :
                recordTypeFailure = True
                recordFailed = True
                failedRules.append([fieldName, ValidationError.typeError, currentData,""])
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
                        recordTypeFailure = True
                        typeFailed = True
                        failedRules.append([fieldName, ValidationError.typeError, currentData,""])
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
                    failedRules.append([fieldName,"".join(["Failed rule: ",str(currentRule.description)]), currentData, str(currentRule.original_label)])
        # Check all multi field rules for this file type
        multiFieldRules = interfaces.validationDb.getMultiFieldRulesByFile(fileType)
        for rule in multiFieldRules:
            if not Validator.evaluateRule(record,rule,None,interfaces,record):
                recordFailed = True
                failedRules.append(["MultiField", "".join(["Failed rule: ",str(rule.description)]), Validator.getMultiValues(rule, record, interfaces), str(rule.original_label)])
        return (not recordFailed), failedRules, (not recordTypeFailure)

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
            # Look for single field rules that apply to this field and file, and are timed to run during record-level validation
            if(rule.file_column is not None and rule.file_column.name == fieldName and rule.file_column.file_id == fileId and rule.rule_timing_id == validationInterface.getRuleTimingIdByName("file_validation")) :
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
        if datatype is None:
            # If no type specified, don't need to check anything
            return True
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
            datatype: Type to convert into

        Returns:
            Data in specified type
        """
        if datatype is None:
            # If no type specified, don't try to process data
            return data
        if(datatype =="INT") :
            return int(float(data))
        if(datatype =="DECIMAL") :
            return Decimal(data)
        if(datatype == "STRING" or datatype =="BOOLEAN") :
            return data
        if(datatype == "LONG"):
            return long(data)
        raise ValueError("Data Type Invalid")

    @classmethod
    def evaluateRule(cls,data,rule,datatype,interfaces,record):
        """ Checks data against specified rule

        Args:
            data: Data to be checked
            rule: Rule object to test against
            datatype: Type to convert data into
            interfaces: InterfaceHolder object to the databases
            record: Some rule types require the entire record as a dict

        Returns:
            True if rule passed, False otherwise
        """
        if data is None:
            # Treat blank as an empty string
            data = ""
        value = rule.rule_text_1
        currentRuleType = rule.rule_type.name
        # Call specific rule function
        ruleFunction = "_".join(["rule",str(currentRuleType).lower()])
        ruleFunction = FieldCleaner.cleanString(ruleFunction)
        try:
            ruleMethod = getattr(cls, str(ruleFunction))
            return ruleMethod(data, value, rule, datatype, interfaces, record)
        except AttributeError as e:
            # Unrecognized rule type
            raise ResponseException(str(e), StatusCode.INTERNAL_ERROR, ValueError)

    @classmethod
    def rule_length(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is shorter than specified length"""
        return len(data.strip()) <= Validator.getIntFromString(value)

    @classmethod
    def rule_less(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is less than specified value"""
        return Validator.getType(data,datatype) < Validator.getType(value,datatype)

    @classmethod
    def rule_greater(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is greater than specified value"""
        return Validator.getType(data,datatype) > Validator.getType(value,datatype)

    @classmethod
    def rule_equal(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is equal to specified value"""
        data = data.lower() if isinstance(data, str) else data
        value = value.lower() if isinstance(value, str) else value
        return Validator.getType(data,datatype) == Validator.getType(value,datatype)

    @classmethod
    def rule_not_equal(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is not equal to specified value"""
        data = data.lower() if isinstance(data, str) else data
        value = value.lower() if isinstance(value, str) else value
        return not (Validator.getType(data,datatype) == Validator.getType(value,datatype))

    @classmethod
    def rule_sum(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data sums to value in specified field"""
        return Validator.validateSum(record[rule.rule_text_1], rule.rule_text_2, record)

    @classmethod
    def rule_sum_to_value(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data sums to value in specified field"""
        return Validator.validateSum(rule.rule_text_1, rule.rule_text_2, record)

    @classmethod
    def rule_type(cls, data, value, rule, datatype, interfaces, record):
        """Type checks happen earlier, but type rule is still included in rule set, so skip it"""
        return True

    @classmethod
    def rule_in_set(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is one of a set of valid values"""
        setList = Validator.cleanSplit(value)
        return data.lower() in setList

    @classmethod
    def rule_min_length(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is at least the minimum length"""
        result = len(data.strip()) >= Validator.getIntFromString(value)
        return result

    @classmethod
    def rule_required_conditional(cls, data, value, rule, datatype, interfaces, record):
        """Checks that data is present if specified rule passes"""
        return Validator.conditionalRequired(data,rule,datatype,interfaces,record)

    @classmethod
    def rule_exists_in_table(cls, data, value, rule, datatype, interfaces, record):
        """ Check that field value exists in specified table, rule_text_1 has table and column to check against, rule_text_2 is length to pad to """
        ruleTextOne = str(rule.rule_text_1).split(",")
        if len(ruleTextOne) != 2:
            # Bad rule definition
            raise ResponseException("exists_in_table rule incorrectly defined, must have both table and field in rule_text_one",StatusCode.INTERNAL_ERROR,ValueError)
        # Not putting model name through FieldCleaner because model names will have uppercase
        model = getattr(domainModels,str(ruleTextOne[0]).strip())
        field = FieldCleaner.cleanString(ruleTextOne[1])
        ruleTextTwo = FieldCleaner.cleanString(rule.rule_text_2)
        if len(ruleTextTwo) == 0:
            # Skip padding
            paddedData = FieldCleaner.cleanString(data)
        else:
            # Pad data to correct length
            try:
                padLength = int(ruleTextTwo)
            except ValueError as e:
                # Need an integer in rule_text_two
                raise ResponseException("Need an integer width in rule_text_two for exists_in_table rules",StatusCode.INTERNAL_ERROR,ValueError)
            paddedData = FieldCleaner.cleanString(data).zfill(padLength)

        # Build query for model and field specified
        query = interfaces.validationDb.session.query(model).filter(getattr(model,field) == paddedData)
        try:
            # Check that value exists in table, should be unique
            interfaces.validationDb.runUniqueQuery(query,"Data not found in table", "Conflicting entries found for this data")
            # If unique result found, rule passed
            return True
        except ResponseException as e:
            # If exception is no result found, rule failed
            if type(e.wrappedException) == type(NoResultFound()):
                return False
            else:
                # This is an unexpected exception, so re-raise it
                raise

    @classmethod
    def rule_set_exists_in_table(cls, data, value, rule, datatype, interfaces, record):
        """ Check that set of values exists in specified table, rule_text_1 is table, rule_text_2 is dict mapping
        columns in record to columns in domain values table """
        # Load mapping from record fields to domain value fields
        fieldMap = json.loads(rule.rule_text_2)
        # Get values for fields in record into new dict between table columns and values to check for
        valueDict = {}
        blankSkip = True
        noBlankSkipFields = True
        for field in fieldMap:
            if "skip_if_blank" in fieldMap[field]:
                noBlankSkipFields = False
                # If all these are blank, rule passes
                if record[field] is not None and record[field].strip() != "":
                    blankSkip = False
            if "skip_if_below" in fieldMap[field]:
                try:
                    if record[field] is None or str(record[field]).strip() == "":
                        # No year provided, so this should be skipped as not being checkable against post 2016 budgets
                        return True
                    if int(record[field]) < fieldMap[field]["skip_if_below"]:
                        # Don't apply rule to records in this case (e.g. program activity before 2016)
                        return True
                except (TypeError, ValueError):
                    # Could not cast as an int, this is a failure for this record
                    return False
            if "pad_to_length" in fieldMap[field]:
                # Pad with leading zeros if needed
                try:
                    fieldValue = cls.padToLength(record[field],fieldMap[field]["pad_to_length"])
                except ValueError as e:
                    # If we cannot pad this value, it is not matchable (usually too long), so the rule has failed
                    return False
            else:
                fieldValue = record[field]
            valueDict[fieldMap[field]["target_field"]] = fieldValue

        if not noBlankSkipFields and blankSkip:
            # This set of fields was all blank and at least one field is marked as "skip_if_blank", so skip rule
            return True


        # Parse out model object
        model = getattr(domainModels,str(rule.rule_text_1))

        # Filter query by each field
        query = interfaces.validationDb.session.query(model)
        for field in valueDict:
            query = query.filter(getattr(model,field) == valueDict[field])

        # NoResultFound is return False, other exceptions should be reraised
        try:
            # Check that value exists in table, should be unique
            interfaces.validationDb.runUniqueQuery(query,"Data not found in table", "Conflicting entries found for this data")
            # If unique result found, rule passed
            return True
        except ResponseException as e:
            # If exception is no result found, rule failed
            if type(e.wrappedException) == type(NoResultFound()):
                return False
            else:
                # This is an unexpected exception, so re-raise it
                raise

    @staticmethod
    def padToLength(data,padLength):
        """ Pad data with leading zeros

        Args:
            data: string to be padded
            padLength: length of string after padding

        Returns:
            padded string of length padLength
        """
        if data is None:
            # Convert None to empty string so it can be padded with zeros
            return data
        data = data.strip()
        if data == "":
            # Empty values treated as null
            return None
        if len(data) <= padLength:
            return data.zfill(padLength)
        else:
            raise ValueError("".join(["Value is too long: ",str(data)]))

    @classmethod
    def rule_required_set_conditional(cls, data, value, rule, datatype, interfaces, record):
        """ If conditional rule passes, require all fields in rule_text_one """
        return Validator.conditionalRequired(data,rule,datatype,interfaces,record)

    @classmethod
    def rule_check_prefix(cls, data, value, rule, datatype, interfaces, record):
        """ Check that 1-digit prefix is consistent with reimbursable flag """
        dataString = FieldCleaner.cleanString(data)

        # Load target field and dict to compare with
        targetField = FieldCleaner.cleanName(rule.rule_text_1)
        prefixMap = json.loads(str(rule.rule_text_2))

        # Check that character and value are consistent with dict in rule_text_2
        if dataString[0] not in prefixMap:
            # Unknown prefix, this is a failure
            return False
        source = prefixMap[dataString[0]]
        target = record[targetField]
        source = source.lower() if source is not None else source
        target = target.lower() if target is not None else target

        if source == target:
            # Matches the value in target field, rule passes
            return True
        else:
            return False

    @classmethod
    def rule_rule_if(cls, data, value, rule, datatype, interfaces, record):
        """ Apply rule in rule_text_1 if rule in rule_text_2 passes """
        # Get rule object for conditional rule
        conditionalRule = interfaces.validationDb.getRuleByLabel(rule.rule_text_2)
        if conditionalRule.file_column is not None:
            # This is a single field rule
            conditionalTypeId = conditionalRule.file_column.field_types_id
            conditionalDataType = interfaces.validationDb.getFieldTypeById(conditionalTypeId)
            conditionalData = record[conditionalRule.file_column.name]
        else:
            conditionalDataType = None
            conditionalData = record
        # If conditional rule passes, check primary rule passes
        if Validator.evaluateRule(conditionalData,conditionalRule,conditionalDataType,interfaces,record):
            # Get rule object for primary rule
            primaryRule = interfaces.validationDb.getRuleByLabel(rule.rule_text_1)
            if primaryRule.file_column is not None:
                # This is a single field rule
                primaryTypeId = primaryRule.file_column.field_types_id
                primaryDataType = interfaces.validationDb.getFieldTypeById(primaryTypeId)
                primaryData = record[primaryRule.file_column.name]
            else:
                primaryDataType = None
                primaryData = record
            # Return result of primary rule
            return Validator.evaluateRule(primaryData,primaryRule,primaryDataType,interfaces,record)
        else:
            # If conditional rule fails, overall rule passes without checking primary
            return True

    @staticmethod
    def requireOne(record, fields, interfaces):
        """ Require at least one of the specified fields to be present

        Args:
            record: Dict for current record
            fields: List of fields to check
            interfaces: interface holder for DBs

        Returns:
            True if at least one of the fields is present
        """
        for field in fields:
            fieldName = FieldCleaner.cleanName(field)
            if fieldName in record and record[fieldName] is not None and str(record[fieldName]).strip() != "":
                # If data is present in this field, rule is satisfied
                return True

        # If all were empty, return false
        return False

    @staticmethod
    def isFieldPopulated(data):
        """ Field is considered to be populated if not None and not entirely whitespace """
        if data is not None and str(data).strip() != "":
            return True
        else:
            return False

    @classmethod
    def conditionalRequired(cls, data,rule,datatype,interfaces,record, isList = False):
        """ If conditional rule passes, data must not be empty

        Args:
            data: Data to be checked
            rule: Rule object to test against
            datatype: Type to convert data into
            interfaces: InterfaceHolder object to the databases
            record: Some rule types require the entire record as a dict
        """
        # Get rule object for conditional rule
        conditionalRule = interfaces.validationDb.getRuleByLabel(rule.rule_text_1)
        if conditionalRule.file_column is not None:
            # This is a single field rule
            conditionalTypeId = conditionalRule.file_column.field_types_id
            conditionalDataType = interfaces.validationDb.getFieldTypeById(conditionalTypeId)
            conditionalData = record[conditionalRule.file_column.name]
        else:
            conditionalDataType = None
            conditionalData = record
        # If conditional rule passes, check that data is not empty
        if Validator.evaluateRule(conditionalData,conditionalRule,conditionalDataType,interfaces,record):
            if isList:
                # rule_text_2 is a list of fields
                fieldList = rule.rule_text_2.split(",")
                for field in fieldList:
                    if not cls.isFieldPopulated(record[FieldCleaner.cleanName(field)]):
                        # If any are empty, rule fails
                        return False
            else:
                # data is value from a single field
                return cls.isFieldPopulated(data)
        else:
            # If conditional rule fails, this field is not required, so the condtional requirement passes
            return True


    @classmethod
    def rule_car_match(cls, data, value, rule, datatype, interfaces, record):
        """Checks that record has a valid TAS"""
        # Look for an entry in car table that matches all fields
        fieldsToCheck = cls.cleanSplit(rule.rule_text_1)
        tasFields = cls.cleanSplit(rule.rule_text_2)
        if(len(fieldsToCheck) != len(tasFields)):
            raise ResponseException("Number of fields to check does not match number of fields checked against",StatusCode.CLIENT_ERROR,ValueError)
        return cls.validateTAS(fieldsToCheck, tasFields, record, interfaces, rule.file_type.name)

    @classmethod
    def rule_sum_fields(cls, data, value, rule, datatype, interfaces, record):
        """Checks that set of fields sums to value in other field"""
        valueToMatch = record[FieldCleaner.cleanName(rule.rule_text_1)]
        if valueToMatch is None or valueToMatch == "":
            valueToMatch = 0
        return cls.validateSum(valueToMatch, rule.rule_text_2, record)

    @classmethod
    def rule_require_one_of_set(cls, data, value, rule, datatype, interfaces, record):
        """Checks that record at least one of this set of fields populated"""
        return cls.requireOne(record,rule.rule_text_1.split(','),interfaces)

    @classmethod
    def rule_not(cls, data, value, rule, datatype, interfaces, record):
        """Passes if the specified rule fails"""
        # Negate the rule specified
        conditionalRule = interfaces.validationDb.getRuleByLabel(rule.rule_text_1)
        return not cls.evaluateRule(data, conditionalRule, datatype, interfaces, record)

    @staticmethod
    def cleanSplit(string, toLower=True):
        """ Split string on commas and remove whitespace around each element

        Args:
            string - String to be split
            toLower - If True, also changes string to lowercase
        """
        if string is None:
            # Convert None to empty list
            return []
        stringList = string.split(",")
        for i in range(0,len(stringList)):
            stringList[i] = stringList[i].strip()
            if(toLower):
                stringList[i] = stringList[i].lower()
        return stringList

    @classmethod
    def getValueList(cls,rule,record,interfaces):
        """ Create list out of values for all fields involved in this rule

        Args:
            rule: Rule to return fields for
            record: Record to pull values from
            interfaces: InterfaceHolder for DBs
        Returns:
            One string including all values involved in this rule check
        """
        ruletext1 = Validator.cleanSplit(rule.rule_text_1, True)
        ruletext2 = Validator.cleanSplit(rule.rule_text_2, True)
        ruleType = interfaces.validationDb.getRuleTypeById(rule.rule_type_id)
        if ruleType == "CAR_MATCH" or ruleType == "REQUIRE_ONE_OF_SET" or ruleType == "CHECK_PREFIX":
            fields = ruletext1
        elif ruleType == "NOT" or ruleType == "MIN_LENGTH" or ruleType == "REQUIRED_CONDITIONAL":
            fields = []
        elif ruleType == "SUM_TO_VALUE":
            fields = ruletext2
        elif ruleType == "SET_EXISTS_IN_TABLE":
            fields = json.loads(rule.rule_text_2).keys()
        elif ruleType == "RULE_IF":
            # Combine fields for both rules
            ruleOne = interfaces.validationDb.getRuleByLabel(rule.rule_text_1)
            ruleTwo = interfaces.validationDb.getRuleByLabel(rule.rule_text_2)
            fields = cls.getValueList(ruleOne,record,interfaces) + cls.getValueList(ruleTwo,record,interfaces)
        else:
            fields = ruletext1 + ruletext2

        # Add file column if present
        fileColId = rule.file_column_id
        if fileColId is not None:
            fileColumn = interfaces.validationDb.getColumnById(fileColId)
            fields.append(fileColumn.name)

        return fields

    @classmethod
    def getMultiValues(cls,rule,record,interfaces):
        """ Create string out of values for all fields involved in this rule

        Args:
            rule: Rule to return fields for
            record: Record to pull values from
            interfaces: InterfaceHolder for DBs
        Returns:
            One string including all values involved in this rule check
        """
        fields = cls.getValueList(rule,record,interfaces)

        output = ""
        outputDict = {}
        for field in fields:
            try:
                value = record[str(field)]
            except KeyError as e:
                raise KeyError("Field " + str(field) + " not found while checking rule type " + str(ruleType))
            if(value == None):
                # For concatenating fields, represent None with an empty string
                value = ""
            if field not in outputDict:
                outputDict[field] = True
                output = "".join([output,field,": ",value,", "])

        return output[:-2]

    @staticmethod
    def validateSum(value, fields_to_sum, record):
        """ Check that the value of one field is the sum of others

        :param value: The field which holds the sum we will validate against
        :param fields_to_sum: A comma separated list of fields which we should sum. These should be valid Decimals
        :param record: Record containing the data for the current record
        :return: True if the sum of fields is equal to the designated sum field
        """

        decimalValues = []

        # Validate that our sum is a decimal
        if Validator.checkType(str(value), 'DECIMAL'):
            decimalSum = Validator.getType(value, 'DECIMAL')
        else:
            return False

        # Validate each field we are summing is a decimal and store their values in an array
        for field in Validator.cleanSplit(fields_to_sum, True):
            entry = record[FieldCleaner.cleanName(field)]
            if entry is None or entry == "":
                decimalValues.append(0)
            elif Validator.checkType(entry, 'DECIMAL'):
                decimalValues.append(Validator.getType(entry, 'DECIMAL'))
            else:
                return False

        return decimalSum == sum(decimalValues)

    @staticmethod
    def validateTAS(fieldsToCheck, tasFields, record, interfaces, fileType):
        """ Check for presence of TAS for specified record in TASLookup table

        Args:
            fieldsToCheck: Set of fields involved in TAS check
            tasFields: Corresponding field names in TASLookup table
            record: Record to check TAS for
            interfaces: InterfaceHolder object to the databases
            fileType: File type being checked

        Returns:
            True if TAS is in CARS, False otherwise
        """
        query = interfaces.validationDb.session.query(TASLookup)

        for i in range(0,len(fieldsToCheck)):
            data = record[str(fieldsToCheck[i])]
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

    @classmethod
    def validateFileBySql(cls, submissionId, fileType, interfaces):
        """ Check all SQL rules

        Args:
            submissionId: submission to be checked
            fileType: file type being checked
            interfaces: database interface objects

        Returns:
            List of errors found, each element has:
             field names
             error message
             values in fields involved
             row number
        """

        CloudLogger.logError("VALIDATOR_INFO: ", "Beginning SQL validation rules on submissionID: " + str(submissionId) + " fileType: "+ fileType, "")

        # Pull all SQL rules for this file type
        fileId = interfaces.validationDb.getFileId(fileType)
        rules = interfaces.validationDb.session.query(RuleSql).filter(RuleSql.file_id == fileId).filter(
            RuleSql.rule_cross_file_flag == False).all()
        errors = []

        # Get short to long colname dictionary
        shortColnames = interfaces.validationDb.getShortToLongColname()

        # For each rule, execute sql for rule
        for rule in rules:
            CloudLogger.logError("VALIDATOR_INFO: ", "Running query: "+str(RuleSql.query_name)+" on submissionID: " + str(submissionId) + " fileType: "+ fileType, "")
            failures = interfaces.stagingDb.connection.execute(rule.rule_sql.format(submissionId))
            if failures.rowcount:
                # Create column list (exclude row_number)
                cols = failures.keys()
                cols.remove("row_number")
                # Build error list
                for failure in failures:
                    errorMsg = rule.rule_error_message
                    row = failure["row_number"]
                    # Create strings for fields and values
                    valueList = ["{}: {}".format(shortColnames[field], str(failure[field])) if field in shortColnames else "{}: {}".format(field, str(failure[field])) for field in cols]
                    valueString = ", ".join(valueList)
                    fieldList = [shortColnames[field] if field in shortColnames else field for field in cols]
                    fieldString = ", ".join(fieldList)
                    errors.append([fieldString, errorMsg, valueString, row, rule.rule_label, fileId, rule.target_file_id])

            # Pull where clause out of rule
            wherePosition = rule.rule_sql.lower().find("where")
            whereClause = rule.rule_sql[wherePosition:].format(submissionId)
            # Find table to apply this to
            model = interfaces.stagingDb.getModel(fileType)
            tableName = model.__tablename__
            tableAbbrev = cls.tableAbbreviations[tableName]
            # Update valid_record to false for all that fail this rule
            updateQuery = "UPDATE {} as {} SET valid_record = false {}".format(tableName,tableAbbrev,whereClause)
            interfaces.stagingDb.connection.execute(updateQuery)

            CloudLogger.logError("VALIDATOR_INFO: ", "Completed SQL validation rules on submissionID: " + str(submissionId) + " fileType: "+ fileType, "")

        return errors
