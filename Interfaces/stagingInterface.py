from interfaces.validationInterface import ValidationInterface
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.field import FieldType, FieldConstraint
from interfaces.jobTrackerInterface import JobTrackerInterface

class StagingInterface(BaseInterface):
    """ Manages all interaction with the staging database
    """

    def createTable(self,filetype,jobId):
        """ Create staging table for new file
        Args:
        filetype -- type of file to create a table for (e.g. Award, AwardFinancial)

        Returns:
        tableName if created, exception otherwise
        """
        tableName = str(filetype)+str(jobId)

        # Write tablename to related job in job tracker
        jobTracker = JobTrackerInterface()
        jobTracker.addStagingTable(jobId,tableName)

        validationDB = ValidationInterface()
        fields = validationDB.getFieldsByFile(filetype)
        # Create sequence to be used for primary key
        sequenceStatement = "CREATE SEQUENCE " + tableName + "Serial START 1"
        self.runStatement(sequenceStatement)
        # Construct the base table creation command
        tableStatement = "CREATE TABLE " + tableName + "("
        # Add each column
        for field in fields.iterKeys():
            tableStatement += field + " " + field["type"] + " "
            if(field["constraint"] == FieldConstraint.PRIMARY_KEY):
                tableStatement += field["constraint"] + " DEFAULT nextval('" + sequenceStatement + "'), "
            else:
                tableStatement += field["constraint"] + ", "

        # Execute table creation
        self.runStatement(tableStatement)
        return tableName

    def writeData(self,tableName, data):
        """ Writes some number of validated records to staging database
        Args:
        data -- records to be written (array of dicts, each dict is a row)
        tableName -- which table to write this data to

        Returns:
        True if all rows were successful
        """

        success = True
        for row in data:
            if(not self.writeRecord(tableName, row)):
                success = False
        return success

    def writeRecord(self, tableName, record):
        """ Write single record to specified table
        Args:
        tableName -- table to write to
        record -- dict with column names as keys

        Returns:
        True if successful
        """
        fieldNames = "("
        fieldValues = "("
        # For each field, add to fieldNames and fieldValues strings
        for key in record.iterKeys():
            fieldNames += key + ", "
            fieldValues += record[key] + ", "

        # Remove last comma and space and close lists
        fieldNames = fieldNames[:-2] + ")"
        fieldValues = fieldValues[:-2] + ")"

        # Create insert statement
        statement = "INSERT INTO " + tableName + " " + fieldNames + " VALUES " + fieldValues
        self.runStatement(statement)