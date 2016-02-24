from sqlalchemy.orm import joinedload
from dataactcore.models.errorModels import FileStatus, ErrorData, Status
from dataactcore.models.errorInterface import ErrorInterface

class ErrorHandler(ErrorInterface) :
    """ Manages communication with the error database """

    def getErrorMetericsByJobId (self,jobId) :
        """ Get error metrics for specified job, including number of errors for each field name and error type """
        resultList = []

        query = self.session.query(FileStatus).options(joinedload("status")).filter(FileStatus.job_id == jobId)
        queryResult = self.runUniqueQuery(query,"No file status for this job", "Conflicting file statuses for this job")

        if(not queryResult.status.status_id == Status.getStatus("complete")) :
            return [["File Level Error",str(queryResult.status.description),1]]

        queryResult = self.session.query(ErrorData).options(joinedload("error_type")).filter(ErrorData.job_id == jobId).all()
        for result in queryResult:
            if(result.error_type is None) :
                errorType  = result.rule_failed
            else :
                errorType  = result.error_type.description
            recordList = [result.field_name,str(result.occurrences),errorType]
            resultList.append(recordList)
        return resultList
