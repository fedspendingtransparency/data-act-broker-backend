from dataactcore.models.baseInterface import BaseInterface
from dataactcore.models.errorModels import Status, ErrorType, FileStatus, ErrorData

class ErrorInterface(BaseInterface):
    """ Manages communication with error database """
    dbName = "error_data"
    credFileName = "dbCred.json"
    Session = None
    engine = None
    session = None

    def __init__(self):
        self.dbConfigFile = self.getCredFilePath()
        super(ErrorInterface,self).__init__()
        #Base.metadata.bind = self.engine
        #Base.metadata.create_all(self.engine)


    @staticmethod
    def getDbName():
        """ Return database name"""
        return ErrorInterface.dbName

    def getSession(self):
        return self.session

    def getStatusId(self,statusName):
        """ Get status ID for given name """
        return self.getIdFromDict(Status,"STATUS_DICT","name",statusName,"status_id")

    def getTypeId(self,typeName):
        return self.getIdFromDict(ErrorType,"TYPE_DICT","name",typeName,"error_type_id")

    def getFileStatusByJobId(self, jobId):
        """ Get the File Status object with the specified job ID

        Args:
            jobId: job to get file status for

        Returns:
            A File Status model object
        """
        query = self.session.query(FileStatus).filter(FileStatus.job_id == jobId)
        return self.runUniqueQuery(query,"No file for that job ID", "Multiple files have been associated with that job ID")

    def checkStatusByJobId(self, jobId):
        """ Query status for specified job

        Args:
            jobId: job to check status for

        Returns:
            Status ID of specified job
        """
        return self.getFileStatusByJobId(jobId).status_id

    def checkNumberOfErrorsByJobId(self, jobId):
        """ Get the total number of errors for a specified job

        Args:
            jobId: job to get errors for

        Returns:
            Number of errors for specified job
        """
        queryResult = self.session.query(ErrorData).filter(ErrorData.job_id == jobId).all()
        numErrors = 0
        for result in queryResult:
            # For each row that matches jobId, add the number of that type of error
	        numErrors += result.occurrences
        return numErrors

    def getMissingHeadersByJobId(self, jobId):
        return self.getFileStatusByJobId(jobId).headers_missing