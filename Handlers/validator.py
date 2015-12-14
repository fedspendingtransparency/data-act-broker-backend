import interfaces
class Validator(ValidationInterface):
    """
    Checks individual records against specified validation tests
    """

    @staticmethod
    def validate(record,rules):
        """
        Args:
        record -- dict represenation of a single record
        testDef -- String representation of validation to be performed

        Returns:
        True if validation passed, False if failed
        """
        for fieldName in record :
            ruleSubset = Validator.getRules(fieldName)
            for currentRule in ruleSubset :
                if(not validateor.evaluateRule(record[fieldName],currentRule)):
                    return False
        return True

    @staticmethod
    def getRules(fieldName,rules) :
        returnList =[]
        for rule in rules :
            if( rule.file_column.name == fieldName) :
                returnList.append(rule)
    return returnList

    @staticmethod
    def evaluateRule(data,rule):
        #TODO The rules
        return False
