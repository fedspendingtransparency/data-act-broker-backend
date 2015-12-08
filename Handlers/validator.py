class Validator:
    """ Checks individual records against specified validation tests
    """
    
    @staticmethod
    def validate(record,testDef):
        """
        Args:
        record -- dict represenation of a single record
        testDef -- String representation of validation to be performed

        Returns:
        True if validation passed, False if failed
        """