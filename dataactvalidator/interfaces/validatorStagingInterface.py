from dataactcore.models.baseInterface import BaseInterface
from dataactcore.config import CONFIG_DB
from dataactcore.utils.responseException import ResponseException
from dataactcore.utils.statusCode import StatusCode
from dataactvalidator.interfaces.validatorJobTrackerInterface import ValidatorJobTrackerInterface

class ValidatorStagingInterface(BaseInterface):
    """ Manages all interaction with the staging database """

    dbConfig = CONFIG_DB
    dbName = dbConfig['staging_db_name']
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbName = self.dbConfig['staging_db_name']
        super(ValidatorStagingInterface, self).__init__()

    @staticmethod
    def getDbName():
        """ Return database name"""
        return ValidatorStagingInterface.dbName

    def dropTable(self,table):
        """

        Args:
            table: Table to be dropped

        Returns:
            True if successful
        """
        self.runStatement("".join(["DROP TABLE ",table]))
        self.session.commit()

    def tableExists(self,table):
        """ True if table exists, false otherwise """
        return self.engine.dialect.has_table(self.engine.connect(),table)

    def countRows(self,table):
        """ Returns number of rows in the specified table """
        if(self.tableExists(table)):
            response =  (self.runStatement("".join(["SELECT COUNT(*) FROM ",table]))).fetchone()[0]
            # Try to prevent blocking
            self.session.close()
            return response
        else:
            return 0

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