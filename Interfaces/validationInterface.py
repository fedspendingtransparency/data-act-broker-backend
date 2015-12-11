class ValidationgInterface:
    """ Manages all interaction with the validation database
    """

    TYPE_DICT = {"0":"Integer", "1":"Text"}
    CONSTRAINT_DICT = {"0":"", "1":"Primary Key", "2":"NOT NULL"}

    def getValidations(self,type):
        """ Get array of dicts for all validations of specified type
        Args:
        type -- type of validation to check for (e.g. single_record, cross_record, external)

        Returns:
        array of dicts, each representing a single validation
        """

    def getFieldsByFile(self,filetype):
        """ Returns a dict of valid field names that can appear in this type of file

        Args:
        filetype -- One of the set of valid types of files (e.g. Award, AwardFinancial)

        Returns:
        dict with field names as keys and values "type" and "constraint" pulled from the TYPE_DICT and CONSTRAINT_DICT
        """