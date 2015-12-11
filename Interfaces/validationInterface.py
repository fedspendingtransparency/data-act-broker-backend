from models.field import FieldType, FieldConstraint

class ValidationInterface:
    """ Manages all interaction with the validation database
    """

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
        dict with field names as keys and values "type" and "constraint" pulled from FieldType and FieldConstraint
        """