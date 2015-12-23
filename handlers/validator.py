import re
class Validator(object):
    """
    Checks individual records against specified validation tests
    """

    @staticmethod
    def validate(record,rules,csvSchema):
        """
        Args:
        record -- dict represenation of a single record of data
        rules -- list of rule Objects
        csvSchema -- dict of schema for the current file.
        Returns:
        True if validation passed, False if failed
        """
        for fieldName in csvSchema :
            if(csvSchema[fieldName].required and  not fieldName in record ):
                return False, fieldName, "Required field not populated"

        for fieldName in record :

            currentSchema =  csvSchema[fieldName]
            ruleSubset = Validator.getRules(fieldName,rules)
            currentData = record[fieldName].strip()


            if(len(currentData) == 0):
                if(currentSchema.required ):
                    # If empty and required return field name and error
                    return False, fieldName, "Required field not populated"
                else:
                    #if field is empty and not required its valid
                    continue
            # Always check the type
            if(not Validator.checkType(currentData,currentSchema.field_type.name) ) :
                return False, fieldName, "Wrong type for this field"
            #Field must pass all rules
            for currentRule in ruleSubset :
                if(not Validator.evaluateRule(currentData,currentRule,currentSchema.field_type.name)):
                    return False, fieldName, "Failed rule: " + str(currentRule.description)
        return True, " ", " "

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
            if(data in ["YES","NO","1","0"]) :
                return True
            return False
        if(datatype == "INT") :
            return re.match(r"^[-]?[1-9]\d*$", data) is not None
        if(datatype == "DECIMAL") :
            if (re.match(r"^[-]?((\d+(\.\d*)?)|(\.\d+))$", data) is None ) :
                return re.match(r"^[-]?[1-9]\d*$", data) is not None
            return True
        raise ValueError("Data Type Invalid " + data)

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
            return len(data) < Validator.getIntFromString(value1)
        if(currentRuleType =="LESS") :
            return Validator.getType(data,datatype) < Validator.getType(value1,datatype)
        if(currentRuleType =="GREATER") :
            return Validator.getType(data,datatype) > Validator.getType(value1,datatype)
        if(currentRuleType =="EQUAL") :
            return Validator.getType(data,datatype) == Validator.getType(value1,datatype)
        if(currentRuleType =="NOT EQUAL") :
            return not (Validator.getType(data,datatype) == Validator.getType(value1,datatype))
        raise ValueError("Rule Type Invalid")
