from interfaces.validationInterface import ValidationInterface
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.field import FieldType, FieldConstraint
from interfaces.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.stagingInterface import StagingInterface as BaseStagingInterface
import dataactcore
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData



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
        print("Table name:")
        print(tableName)
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
        print("Fields by file number of keys:")
        print(str(len(fields)))
        """ Might not need sequence for ORM
        # Create sequence to be used for primary key
        sequenceName = tableName + "Serial"
        sequenceStatement = "CREATE SEQUENCE " + sequenceName + " START 1"
        try:
            self.runStatement(sequenceStatement)
        except ProgrammingError:
            # Sequence already exists
            pass
        """
        primaryAssigned = False
        # TODO change all handling in this class to go through ORM with dynamic class creation
        # Create empty dict for field names and values
        classFieldDict = {"__tablename__":tableName}
        # Add each column
        for key in fields.iterkeys():
            # Build column statement for this key
            # Get correct type name
            fieldTypeName = fields[key].field_type.name
            if(fieldTypeName.lower() == "string"):
                fieldTypeName = "Text"
            elif(fieldTypeName.lower() == "int"):
                fieldTypeName = "Integer"
            # Get extra parameters (primary key or not null)
            extraParam = ""
            if(fields[key].field_type.description == "PRIMARY_KEY"):
                extraParam =  ", primary_key = True"
                primaryAssigned = True
            elif(fields[key].required):
                extraParam = ", nullable = False"

            columnDeclaration = "Column("+fieldTypeName+extraParam+")"
            # Add column to dict
            classFieldDict[key.replace(" ","_")] = columnDeclaration

        if(not primaryAssigned):
            # If no primary key assigned, add one based on table name
            classFieldDict[tableName + "id"] = "Column(Integer, primary_key = True)"


        # Create ORM class based on dict
        print(str(classFieldDict))
        self.orm = type(tableName,(declarative_base(),),classFieldDict)

        # Create table
        self.orm.__table__.create(self.engine)

        # Create table from metadata
        #meta = MetaData()
        #meta.create_all(bind=self.engine,)

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

        # TODO rewrite to use ORM model stored in self.orm

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
