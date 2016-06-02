from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Text, Numeric, Boolean, BigInteger
from dataactvalidator.interfaces.interfaceHolder import InterfaceHolder
from dataactvalidator.filestreaming.fieldCleaner import FieldCleaner

class StagingTable(object):
    """ Represents a staging table used for a single job """

    BATCH_INSERT = True
    INSERT_BY_ORM = False
    BATCH_SIZE = 100

    def __init__(self,interfaces):
        # Start first batch
        self.batch = []
        self.interface = interfaces.stagingDb
        self.interfaces = interfaces

    def createTable(self, fileType, filename, jobId, tableName=None):
        """ Create staging table for new file
        Args:
        fileType -- type of file to create a table for (e.g. Award, AwardFinancial)

        Returns:
        tableName if created, exception otherwise
        """
        if(tableName == None):
            tableName = self.interface.getTableName(jobId)
        self.name = tableName

        if(self.interface.tableExists(tableName)):
            # Old table still present, drop table and replace
            self.interface.dropTable(tableName)

        # Alternate way of naming tables
        #tableName = "data" + tableName.replace("/","").replace("\\","").replace(".","")
        # Write tableName to related job in job tracker

        self.interfaces.jobDb.addStagingTable(jobId,tableName)
        fields = self.interfaces.validationDb.getFieldsByFile(fileType)

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
        # Create empty dict for field names and values
        classFieldDict = {"__tablename__":tableName}
        # Create dict to hold record for field names
        fieldNameMap = {}
        # Add each column
        for key in fields:
            # Build column statement for this key
            # Create cleaned version of key
            newKey = str(fields[key].file_column_id)
            # Get correct type name
            fieldTypeName = FieldCleaner.cleanString(fields[key].field_type.name)
            if(fieldTypeName == "string"):
                fieldTypeName = Text
            elif(fieldTypeName == "int"):
                fieldTypeName = Integer
            elif(fieldTypeName == "decimal"):
                fieldTypeName = Numeric
            elif(fieldTypeName == "boolean"):
                fieldTypeName = Boolean
            elif(fieldTypeName == "long"):
                fieldTypeName = BigInteger
            else:
                raise ValueError("Bad field type")
            # Get extra parameters (primary key or not null)
            extraParam = ""
            if(FieldCleaner.cleanString(fields[key].field_type.description) == "primary_key"):
                classFieldDict[newKey] = Column(fieldTypeName, primary_key=True)
                primaryAssigned = True
            elif(fields[key].required):
                classFieldDict[newKey] = Column(fieldTypeName, nullable=False)
            else:
                classFieldDict[newKey] = Column(fieldTypeName)
            # First record will hold field names
            fieldNameMap[str(newKey)] = str(key)
        # Add column for row number
        classFieldDict["row"] = Column(Integer, nullable=False)

        if(not primaryAssigned):
            # If no primary key assigned, add one based on table name
            classFieldDict["".join([tableName,"id"])] = Column(Integer, primary_key = True)


        # Create ORM class based on dict
        self.orm = type(tableName,(declarative_base(),),classFieldDict)
        self.jobId = jobId

        # Create table
        self.orm.__table__.create(self.interface.engine)

        # Add field name map to table
        self.interface.addFieldNameMap(tableName,fieldNameMap)

    def endBatch(self):
        """ Called at end of process to send the last batch """
        if not self.BATCH_INSERT:
            # Not batching, just return
            return False
        if(len(self.batch)>0):
            self.interface.connection.execute(self.orm.__table__.insert(),self.batch)
            self.batch = []
            return True
        else:
            return False

    def insertList(self, data):
        """ Writes some number of validated records to staging database
        Args:
        data -- records to be written (array of dicts, each dict is a row)

        Returns:
        True if all rows were successful
        """

        success = True
        for row in data:
            if(not self.insert(row)):
                success = False
        return success

    def insert(self, record, fileType):
        """ Write single record to this table
        Args:
        record: dict with column names as keys
        fileType: Type of file record is in

        Returns:
        True if successful
        """

        # Need to translate the provided record to use column IDs instead of field names for keys
        idRecord = {}
        # Mark if header
        for key in record:
            if key == "row":
                idRecord[key] = record[key]
            else:
                idRecord[str(self.interfaces.validationDb.getColumnId(key,fileType))] = record[key]

        if(self.BATCH_INSERT):
            if(self.INSERT_BY_ORM):
                raise NotImplementedError("Have not implemented ORM method for batch insert")
            else:
                self.batch.append(idRecord)
                if(len(self.batch)>self.BATCH_SIZE):
                    # Time to write the batch
                    self.interface.connection.execute(self.orm.__table__.insert(),self.batch)
                    # Reset batch
                    self.batch = []
                return True
        else:
            if(self.INSERT_BY_ORM):
                try:
                    recordOrm = self.orm()
                except:
                    # createTable was not called
                    raise Exception("Must call createTable before writing")

                attributes = self.getPublicMembers(recordOrm)

                # For each field, add value to ORM object
                for key in idRecord:
                    attr = FieldCleaner.cleanString(key) #key.replace(" ","_")
                    setattr(recordOrm,attr,idRecord[key])

                self.interface.session.add(recordOrm)
                self.interface.session.commit()
                return True
            else:
                raise ValueError("Must do either batch or use ORM, cannot set both to False")

    @staticmethod
    def getPublicMembers(obj):
        """ Get list of available members that do not begin with underscore """
        response = []
        for member in dir(obj):
            if(member[0] != "_"):
                response.append(member)
        return response
