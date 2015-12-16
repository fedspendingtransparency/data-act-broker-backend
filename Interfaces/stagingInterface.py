from interfaces.validationInterface import ValidationInterface
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.field import FieldType, FieldConstraint
from interfaces.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.stagingInterface import StagingInterface as BaseStagingInterface
import dataactcore
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData, Column, Integer, Text, Numeric, Boolean



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
        validationDB = ValidationInterface()
        fields = validationDB.getFieldsByFile(filetype)

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
                fieldTypeName = Text
            elif(fieldTypeName.lower() == "int"):
                fieldTypeName = Integer
            elif(fieldTypeName.lower() == "decimal"):
                fieldTypeName = Numeric
            elif(fieldTypeName.lower() == "boolean"):
                fieldTypeName = Boolean
            else:
                raise ValueError("Bad field type")
            # Get extra parameters (primary key or not null)
            extraParam = ""
            if(fields[key].field_type.description == "PRIMARY_KEY"):
                classFieldDict[key.replace(" ","_")] = Column(fieldTypeName, primary_key=True)
                primaryAssigned = True
            elif(fields[key].required):
                classFieldDict[key.replace(" ","_")] = Column(fieldTypeName, nullable=False)
            else:
                classFieldDict[key.replace(" ","_")] = Column(fieldTypeName)


        if(not primaryAssigned):
            # If no primary key assigned, add one based on table name
            classFieldDict[tableName + "id"] = Column(Integer, primary_key = True)


        # Create ORM class based on dict
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
        print("Record to be written:")
        print(str(record))

        # Create ORM object from class defined by createTable
        try:
            record = self.orm()
        except:
            # createTable was not called
            raise Exception("Must call createTable before writing")

        # For each field, add value to ORM object
        for key in record.iterkeys():
            print("Before adding")
            print(record.__dict__)
            record[key.replace(" ","_")] = record[key]
            print("After adding")
            print(record.__dict__)

        self.session.commit()

    #@staticmethod
    def dropTable(self,table):
        try:
            print("Dropping table "+table)
            self.runStatement("DROP TABLE "+table)
        except:
            # Table was not found
            print("Table "+table+" not found")
            pass
        return True
