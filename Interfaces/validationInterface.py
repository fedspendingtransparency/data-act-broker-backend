class ValidationgInterface:
    """ Manages all interaction with the validation database
    """

    def getValidations(self,type):
        """ Get array of dicts for all validations of specified type
        Args:
        type -- type of validation to check for (e.g. single_record, cross_record, external)

        Returns:
        array of dicts, each representing a single validation
        """