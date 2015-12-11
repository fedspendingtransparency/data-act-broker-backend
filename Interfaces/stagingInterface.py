from interfaces.validationInterface import ValidationInterface

class StagingInterface:
    """ Manages all interaction with the staging database
    """

    def createTable(self,filetype):
        """ Create staging table for new file
        Args:
        filetype -- type of file to create a table for (e.g. Award, AwardFinancial)

        Returns:
        tableName if created, exception otherwise
        """
        validationDB = ValidationInterface()
        fields = validationDB.getFieldsByFile(filetype)


    def writeData(self,tableName, data):
        """ Writes some number of validated records to staging database
        Args:
        data -- records to be written
        tableName -- which table to write this data to

        Returns:

        """