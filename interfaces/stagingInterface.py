from interfaces.validationInterface import ValidationInterface
from interfaces.stagingTable import StagingTable
from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.field import FieldType, FieldConstraint
from interfaces.jobTrackerInterface import JobTrackerInterface
from dataactcore.models.stagingInterface import StagingInterface as BaseStagingInterface
import dataactcore
from sqlalchemy.exc import ProgrammingError, ResourceClosedError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData, Column, Integer, Text, Numeric, Boolean
import inspect


class StagingInterface(BaseStagingInterface):
    """ Manages all interaction with the staging database
    """

    def createTable(self, fileType, filename, jobId, tableName=None):
        """ Create staging table for new file
        Args:
        fileType -- type of file to create a table for (e.g. Award, AwardFinancial)

        Returns:
        tableName if created, exception otherwise
        """
        if(tableName==None):
            tableName = "job"+str(jobId)

        while(self.tableExists(tableName)):
            # Now an exception, could change the name here if desired
            raise ValueError("Table already exists")

        # Alternate way of naming tables
        #tableName = "data" + tableName.replace("/","").replace("\\","").replace(".","")
        # Write tableName to related job in job tracker
        from interfaces.interfaceHolder import InterfaceHolder  # This is done here to avoid circular import issues
        jobTracker = InterfaceHolder.JOB_TRACKER
        jobTracker.addStagingTable(jobId,tableName)
        validationDB = InterfaceHolder.VALIDATION
        fields = validationDB.getFieldsByFile(fileType)

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
        customTable =  StagingTable(type(tableName,(declarative_base(),),classFieldDict))

        # Create table
        customTable.create(self.engine)

        # Create table from metadata
        #meta = MetaData()
        #meta.create_all(bind=self.engine,)

        return customTable

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


    def writeRecord(self, table, record):
        """ Write single record to specified table
        Args:
        table -- table orm object to write to
        record -- dict with column names as keys

        Returns:
        True if successful
        """

        # Create ORM object from class defined by createTable
        try:
            insert = table.insert(record)
        except:
            print "!!!!"
            # createTable was not called
            raise Exception("Must call createTable before writing")

        self.session.add(insert)
        self.session.commit()

    #@staticmethod
    def dropTable(self,table):
        try:
            self.runStatement("DROP TABLE "+table)
        except Exception as e:
            # Table was not found
            pass

        try:
            self.session.close()
        except ResourceClosedError:
            # Connection already closed
            pass
        return True


    def tableExists(self,table):
        """ True if table exists, false otherwise """
        return self.engine.dialect.has_table(self.engine.connect(),table)

    def countRows(self,table):
        """ Returns number of rows in the specified table """
        if(self.tableExists(table)):
            response =  (self.runStatement("SELECT COUNT(*) FROM "+table)).fetchone()[0]
            # Try to prevent blocking
            self.session.close()
            return response
        else:
            return 0
