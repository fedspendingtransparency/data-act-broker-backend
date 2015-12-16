from interfaces.validationInterface import ValidationInterface
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.field import FieldType, FieldConstraint
from interfaces.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.stagingInterface import StagingInterface as BaseStagingInterface
import dataactcore
from sqlalchemy.exc import ProgrammingError

class StagingInterface(BaseStagingInterface):
    """ Manages all interaction with the staging database
    """

    def createTable(self,filetype,filename,jobId,tableName=None):
        """ Create staging table for new file
        Args:
        filetype -- type of file to create a table for (e.g. Award, AwardFinancial)

        Returns:
        tableName if created, exception otherwise
        """
        if(tableName==None):
            tableName = "job"+str(jobId)

        # Alternate way of naming tables
        #tableName = "data" + tableName.replace("/","").replace("\\","").replace(".","")
        # Write tablename to related job in job tracker
        jobTracker = JobTrackerInterface()
        jobTracker.addStagingTable(jobId,tableName)
        print(filetype)
        validationDB = ValidationInterface()
        fields = validationDB.getFieldsByFile(filetype)
        print("Fields by file:")
        print(str(fields))
        # Create sequence to be used for primary key
        sequenceName = tableName + "Serial"
        sequenceStatement = "CREATE SEQUENCE " + sequenceName + " START 1"
        try:
            self.runStatement(sequenceStatement)
        except ProgrammingError:
            # Sequence already exists
            pass
        primaryAssigned = False
        # Construct the base table creation command
        tableStatement = "CREATE TABLE " + tableName + " ("
        # Add each column
        for field in fields:
            print("Adding field " + str(field))
            tableStatement += field.name + " " + field.field_type.name
            if(field.field_type.description == "PRIMARY_KEY"):
                tableStatement += " PRIMARY KEY DEFAULT nextval('" + sequenceName + "'), "
                primaryAssigned = True
            elif(field.required):
                tableStatement += " NOT NULL, "
            else:
                tableStatement += ", "

        if(not primaryAssigned):
            # If no primary key assigned, add one based on table name
            tableStatement += tableName + "id" + " INTEGER PRIMARY KEY DEFAULT nextval('" + sequenceName + "'), "

        # Add closing paranthesis
        tableStatement = tableStatement[0:-2] + ")"

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
        for key in record.iterkeys():
            fieldNames += key.replace(" ","_") + ", "
            fieldValues += record[key] + ", "

        # Remove last comma and space and close lists
        fieldNames = fieldNames[:-2] + ")"
        fieldValues = fieldValues[:-2] + ")"

        # Create insert statement
        statement = "INSERT INTO " + tableName + " " + fieldNames + " VALUES " + fieldValues

        self.runStatement(statement)

    #@staticmethod
    def dropTable(self,table):
        try:
            print("Dropping table "+table)
            self.runStatement("DROP TABLE "+table)
        except:
            # Table was not found
            pass
        return True
