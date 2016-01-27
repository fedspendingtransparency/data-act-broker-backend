import re
from validation_handlers.validationError import ValidationError
from interfaces.interfaceHolder import InterfaceHolder
from dataactcore.models.validationModels import TASLookup
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode

class Validator(object):
    """
    Checks individual records against specified validation tests
    """
    IS_INTERGER  = re.compile(r"^[-]?\d+$")
    IS_DECIMAL  = re.compile(r"^[-]?((\d+(\.\d*)?)|(\.\d+))$")
    FIELD_LENGTH = {"allocationtransferrecipientagencyid":3, "appropriationaccountresponsibleagencyid":3, "obligationavailabilityperiodstartfiscalyear":4, "obligationavailabilityperiodendfiscalyear":4,"appropriationmainaccountcode":4, "appropriationsubaccountcode":3, "obligationunlimitedavailabilityperiodindicator":1}

    @staticmethod
    def validate(record,rules,csvSchema,fileType):
        """
        Args:
        record -- dict represenation of a single record of data
        rules -- list of rule Objects
        csvSchema -- dict of schema for the current file.
        Returns:
        True if validation passed, False if failed, and list of failed rules, each with field, description of failure, and value that failed
        """

        recordFailed = False
        failedRules = []
        for fieldName in csvSchema :
            if(csvSchema[fieldName].required and  not fieldName in record ):
                return False, [[fieldName, ValidationError.requiredError, ""]]

        for fieldName in record :

            currentSchema =  csvSchema[fieldName]
            ruleSubset = Validator.getRules(fieldName,rules)
            currentData = record[fieldName].strip()


            if(len(currentData) == 0):
                if(currentSchema.required ):
                    # If empty and required return field name and error
                    return False, [[fieldName, ValidationError.requiredError, ""]]
                else:
                    #if field is empty and not required its valid
                    continue
            # Always check the type in the schema
            if(not Validator.checkType(currentData,currentSchema.field_type.name) ) :
                recordFailed = True
                failedRules.append([fieldName, ValidationError.typeError, currentData])
                # Don't check value rules if type failed
                continue

            # Check for a type rule in the rule table, don't want to do value checks if type is not correct
            typeFailed = False
            for currentRule in ruleSubset:
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
                if(not Validator.evaluateRule(currentData,currentRule,currentSchema.field_type.name)):
                    recordFailed = True
                    failedRules.append([fieldName, "Failed rule: " + str(currentRule.description), currentData])
        # Check all multi field rules for this file type
        validationDb = InterfaceHolder.VALIDATION
        multiFieldRules = validationDb.getMultiFieldRulesByFile(fileType)
        for rule in multiFieldRules:
            if not Validator.evaluateMultiFieldRule(rule,record):
                recordFailed = True
                failedRules.append(["MultiField", "Failed rule: " + str(rule.description), Validator.getMultiValues(rule,record)])


        return (not recordFailed), failedRules

    @staticmethod
    def getRules(fieldName,rules) :
        returnList =[]
        for rule in rules :
            if( rule.file_column.name == fieldName) :
                returnList.append(rule)
        return returnList

    @staticmethod
    def checkType(data,datatype) :
        if(datatype == "STRING") :
            return(len(data) > 0)
        if(datatype == "BOOLEAN") :
            if(data.upper() in ["TRUE","FALSE","YES","NO","1","0"]) :
                return True
            return False 
        if(datatype == "INT") :
            return Validator.IS_INTERGER.match(data) is not None
        if(datatype == "DECIMAL") :
            if (Validator.IS_DECIMAL.match(data) is None ) :
                return Validator.IS_INTERGER.match(data) is not None
            return True
        raise ValueError("Data Type Error, Type: " + datatype + ", Value: " + data)

    @staticmethod
    def getIntFromString(data) :
        return int(float(data))

    @staticmethod
    def getType(data,datatype) :
        if(datatype =="INT") :
            return int(float(data))
        if(datatype =="DECIMAL") :
            return float(data)
        if(datatype == "STRING" or datatype =="BOOLEAN") :
            return data
        raise ValueError("Data Type Invalid")

    @staticmethod
    def evaluateRule(data,rule,datatype):
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
            setList = value1.split(",")
            for i in range(0,len(setList)):
                setList[i] = setList[i].strip()
            return (data in setList)
        raise ValueError("Rule Type Invalid")

    @staticmethod
    def evaluateMultiFieldRule(rule, record):
        ruleType = rule.multi_field_rule_type.name.upper()
        if(ruleType == "CAR_MATCH"):
            # Look for an entry in car table that matches all fields
            fieldsToCheck = Validator.cleanSplit(rule.rule_text_1)
            tasFields = Validator.cleanSplit(rule.rule_text_2)
            if(len(fieldsToCheck) != len(tasFields)):
                raise ResponseException("Number of fields to check does not match number of fields checked against",StatusCode.CLIENT_ERROR,ValueError)
            return Validator.validateTAS(fieldsToCheck, tasFields, record)
        else:
            raise ResponseException("Bad rule type for multi-field rule",StatusCode.INTERNAL_ERROR)

    @staticmethod
    def cleanSplit(string):
        string = string.split(",")
        for i in range(0,len(string)):
            string[i] = string[i].lower().strip()
        return string

    @staticmethod
    def getMultiValues(rule,record):
        fields = Validator.cleanSplit(rule.rule_text_1)
        output = ""
        for field in fields:
            output += field + ": " + record[field] + ", "
        return output[0:len(output)-2]

    @staticmethod
    def validateTAS(fieldsToCheck, tasFields, record):
        validationDB = InterfaceHolder.VALIDATION
        query = validationDB.session.query(TASLookup)
        queryResult = query.all()

        for i in range(0,len(fieldsToCheck)):
            data = record[str(fieldsToCheck[i])]
            field = fieldsToCheck[i].lower()
            # Pad field with leading zeros
            if field in Validator.FIELD_LENGTH:
                length = Validator.FIELD_LENGTH[field]
                if len(data) < length:
                    numZeros = length - len(data)
                    zeroString = ""
                    for j in range(0,numZeros):
                        zeroString += "0"
                    data = zeroString + data
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
