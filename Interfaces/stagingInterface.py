from interfaces.validationInterface import ValidationInterface
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

    def createTable(self,filetype,filename,jobId,tableName=None):
        """ Create staging table for new file
        Args:
        filetype -- type of file to create a table for (e.g. Award, AwardFinancial)

        Returns:
        tableName if created, exception otherwise
        """
        if(tableName==None):
            tableName = "job"+str(jobId)

        while(self.tableExists(tableName)):
            tableName += "_"

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
        print(type(record))

        # Create ORM object from class defined by createTable
        try:
            recordOrm = self.orm()
        except:
            # createTable was not called
            raise Exception("Must call createTable before writing")

        print("Members in ORM:")
        attributes = self.getPublicMembers(recordOrm)
        print(str(attributes))
        # For each field, add value to ORM object
        for key in record.iterkeys():
            attr = key.replace(" ","_")
            print("Writing attribute " + attr)
            if not attr in attributes:
                print(attr + " is not in " + str(attributes))
            print("Before writing")
            print(getattr(recordOrm,attr))
            setattr(recordOrm,attr,record[key])
            print("After writing")
            print(getattr(recordOrm,attr))

        self.session.add(recordOrm)
        self.session.commit()

    #@staticmethod
    def dropTable(self,table):
        try:
            self.session.close()
            try:
                #self.connection.close()
                pass
            except ResourceClosedError:
                # Connection already closed
                pass
            self.runStatement("DROP TABLE "+table)
        except Exception as e:
            # Table was not found
            #print("Table "+table+" not found")
            pass
        return True

    @staticmethod
    def getPublicMembers(obj):
        response = []
        for member in dir(obj):
            if(member[0] != "_"):
                response.append(member)
        return response

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