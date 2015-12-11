class StagingInterface:
    """ Manages all interaction with the staging database
    """

    def createTable(self,columnDict):
        """ Create staging table for new file
        Args:
        columnDict -- Keys are column names, values are "type" (e.g. text NOT NULL)

        Returns:
        tableName if created, exception otherwise
        """

    def writeData(self,tableName, data):
        """ Writes some number of validated records to staging database
        Args:
        data -- records to be written
        tableName -- which table to write this data to

        Returns:

        """