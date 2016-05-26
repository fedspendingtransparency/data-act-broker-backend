from dataactcore.models.stagingInterface import StagingInterface
from dataactcore.models.stagingModels import FieldNameMap
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.interfaces.validatorJobTrackerInterface import ValidatorJobTrackerInterface
from sqlalchemy import MetaData, Table
from sqlalchemy.exc import NoSuchTableError


class ValidatorStagingInterface(StagingInterface):
    """ Manages all interaction with the staging database """

    def dropTable(self,table):
        """

        Args:
            table: Table to be dropped

        Returns:
            True if successful
        """

        metadata = MetaData()
        stagingTable = Table(table, metadata, autoload_with=self.engine)
        stagingTable.drop(bind=self.engine)

    def tableExists(self,table):
        """ True if table exists, false otherwise """
        return self.engine.dialect.has_table(self.engine.connect(),table)

    def countRows(self,table):
        """ Returns number of rows in the specified table """
        metadata = MetaData()
        try:
            stagingTable = Table(table, metadata, autoload_with=self.engine)
        except NoSuchTableError:
            return 0
        rows = self.session.query(stagingTable).count()
        self.session.close()
        return rows

    @classmethod
    def getTableName(cls, jobId):
        """ Get the staging table name based on the job ID """
        # Get submission ID and file type
        jobDb = ValidatorJobTrackerInterface()
        submissionId = jobDb.getSubmissionId(jobId)
        jobType = jobDb.getJobType(jobId)
        if jobType == "csv_record_validation":
            fileType = jobDb.getFileType(jobId)
        elif jobType == "validation":
            fileType = "_cross_file"
        else:
            raise ResponseException("Unknown Job Type",StatusCode.CLIENT_ERROR,ValueError)
        # Get table name based on submissionId and fileType
        return cls.getTableNameBySubmissionId(submissionId, fileType)

    @staticmethod
    def getTableNameBySubmissionId(submissionId, fileType):
        """ Get staging table name based on submission ID and file type """
        return "".join(["submission",str(submissionId),str(fileType)])

    def getFieldNameMap(self, tableName):
        """ Return the dict mapping column IDs to field names """
        query = self.session.query(FieldNameMap).filter(FieldNameMap.table_name == tableName)
        return self.runUniqueQuery(query,"No map for that table", "Conflicting maps for that table").column_to_field_map

    def addFieldNameMap(self, tableName, fieldNameMap):
        """ Add dict for field names to staging DB

        Args:
            tableName: Table map is being added for
            fieldNameMap: Dict with column IDs as keys and field names as values
        """
        newMap = FieldNameMap(table_name = tableName, column_to_field_map = str(fieldNameMap))
        self.session.add(newMap)
        self.session.commit()